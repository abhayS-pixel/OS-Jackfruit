/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    int log_read_fd;
    int producer_started;
    pthread_t producer_thread;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    pthread_cond_t metadata_cv;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    bounded_buffer_t *buffer;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} producer_arg_t;

static volatile sig_atomic_t global_sigchld;
static volatile sig_atomic_t global_stop;

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item);
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item);
void *logging_thread(void *arg);
int child_fn(void *arg);
int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes);
int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid);

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static ssize_t write_full(int fd, const void *buf, size_t count)
{
    const char *cursor = buf;
    size_t total = 0;

    while (total < count) {
        ssize_t written = write(fd, cursor + total, count - total);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (written == 0)
            break;
        total += (size_t)written;
    }

    return (ssize_t)total;
}

static ssize_t read_full(int fd, void *buf, size_t count)
{
    char *cursor = buf;
    size_t total = 0;

    while (total < count) {
        ssize_t nread = read(fd, cursor + total, count - total);
        if (nread < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (nread == 0)
            break;
        total += (size_t)nread;
    }

    return (ssize_t)total;
}

static void copy_string(char *dst, size_t dst_size, const char *src)
{
    size_t len;

    if (dst_size == 0)
        return;

    if (src == NULL) {
        dst[0] = '\0';
        return;
    }

    len = strlen(src);
    if (len >= dst_size)
        len = dst_size - 1;

    memcpy(dst, src, len);
    dst[len] = '\0';
}

static void signal_handler(int signo)
{
    if (signo == SIGCHLD) {
        global_sigchld = 1;
        return;
    }
    global_stop = 1;
}

static container_record_t *find_container_by_id(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur != NULL; cur = cur->next) {
        if (strncmp(cur->id, id, sizeof(cur->id)) == 0)
            return cur;
    }

    return NULL;
}

static container_record_t *find_container_by_pid(supervisor_ctx_t *ctx, pid_t pid)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur != NULL; cur = cur->next) {
        if (cur->host_pid == pid)
            return cur;
    }

    return NULL;
}

static int is_live_state(container_state_t state)
{
    return state == CONTAINER_STARTING || state == CONTAINER_RUNNING;
}

static int rootfs_in_use(supervisor_ctx_t *ctx, const char *rootfs)
{
    container_record_t *cur;

    for (cur = ctx->containers; cur != NULL; cur = cur->next) {
        if (is_live_state(cur->state) &&
            strncmp(cur->rootfs, rootfs, sizeof(cur->rootfs)) == 0)
            return 1;
    }

    return 0;
}

static void *producer_thread_main(void *arg)
{
    producer_arg_t *producer = arg;
    log_item_t item;

    memset(&item, 0, sizeof(item));
    copy_string(item.container_id, sizeof(item.container_id), producer->container_id);

    for (;;) {
        ssize_t nread = read(producer->read_fd, item.data, sizeof(item.data));
        if (nread < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        if (nread == 0)
            break;
        item.length = (size_t)nread;
        if (bounded_buffer_push(producer->buffer, &item) != 0)
            break;
    }

    close(producer->read_fd);
    free(producer);
    return NULL;
}

static int write_response(int fd, int status, const char *message)
{
    control_response_t resp;

    memset(&resp, 0, sizeof(resp));
    resp.status = status;
    if (message != NULL)
        copy_string(resp.message, sizeof(resp.message), message);

    return write_full(fd, &resp, sizeof(resp)) == (ssize_t)sizeof(resp) ? 0 : -1;
}

static void process_exit_status(supervisor_ctx_t *ctx, pid_t pid, int status)
{
    container_record_t *record;
    pthread_t producer_thread;
    int join_producer = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_pid(ctx, pid);
    if (record == NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }

    if (WIFEXITED(status)) {
        record->exit_code = WEXITSTATUS(status);
        record->exit_signal = 0;
        record->state = record->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
    } else if (WIFSIGNALED(status)) {
        record->exit_code = 128 + WTERMSIG(status);
        record->exit_signal = WTERMSIG(status);
        record->state = record->stop_requested ? CONTAINER_STOPPED : CONTAINER_KILLED;
    }

    if (record->producer_started) {
        producer_thread = record->producer_thread;
        record->producer_started = 0;
        join_producer = 1;
    }

    pthread_cond_broadcast(&ctx->metadata_cv);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0)
        unregister_from_monitor(ctx->monitor_fd, record->id, record->host_pid);

    if (join_producer)
        pthread_join(producer_thread, NULL);
}

static void reap_children(supervisor_ctx_t *ctx, int blocking)
{
    int status;
    pid_t pid;

    for (;;) {
        pid = waitpid(-1, &status, blocking ? 0 : WNOHANG);
        if (pid > 0) {
            process_exit_status(ctx, pid, status);
            if (blocking)
                continue;
        }

        if (pid == 0)
            break;

        if (pid < 0) {
            if (errno == EINTR)
                continue;
            if (errno == ECHILD)
                break;
        }

        if (!blocking)
            break;
    }

    global_sigchld = 0;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           char *message,
                           size_t message_len)
{
    int pipefd[2];
    int log_fd;
    child_config_t child_cfg;
    container_record_t *record;
    producer_arg_t *producer;
    void *stack;
    pid_t pid;

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id(ctx, req->container_id) != NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message, message_len, "container id already exists");
        return -1;
    }
    if (rootfs_in_use(ctx, req->rootfs)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(message, message_len, "container rootfs is already in use");
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pipe(pipefd) != 0) {
        snprintf(message, message_len, "pipe failed: %s", strerror(errno));
        return -1;
    }

    memset(&child_cfg, 0, sizeof(child_cfg));
    copy_string(child_cfg.id, sizeof(child_cfg.id), req->container_id);
    copy_string(child_cfg.rootfs, sizeof(child_cfg.rootfs), req->rootfs);
    copy_string(child_cfg.command, sizeof(child_cfg.command), req->command);
    child_cfg.nice_value = req->nice_value;
    child_cfg.log_write_fd = pipefd[1];

    stack = malloc(STACK_SIZE);
    if (stack == NULL) {
        close(pipefd[0]);
        close(pipefd[1]);
        snprintf(message, message_len, "stack allocation failed");
        return -1;
    }

    pid = clone(child_fn,
                (char *)stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                &child_cfg);
    if (pid < 0) {
        snprintf(message, message_len, "clone failed: %s", strerror(errno));
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    free(stack);
    close(pipefd[1]);

    record = calloc(1, sizeof(*record));
    producer = calloc(1, sizeof(*producer));
    if (record == NULL || producer == NULL) {
        kill(pid, SIGKILL);
        close(pipefd[0]);
        free(record);
        free(producer);
        snprintf(message, message_len, "allocation failed");
        return -1;
    }

    copy_string(record->id, sizeof(record->id), req->container_id);
    copy_string(record->rootfs, sizeof(record->rootfs), req->rootfs);
    record->host_pid = pid;
    record->started_at = time(NULL);
    record->state = CONTAINER_RUNNING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->log_read_fd = pipefd[0];
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    log_fd = open(record->log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (log_fd < 0) {
        kill(pid, SIGKILL);
        close(pipefd[0]);
        free(record);
        free(producer);
        snprintf(message, message_len, "failed to create log file: %s", strerror(errno));
        return -1;
    }
    close(log_fd);

    producer->buffer = &ctx->log_buffer;
    producer->read_fd = pipefd[0];
    copy_string(producer->container_id, sizeof(producer->container_id), req->container_id);

    if (pthread_create(&record->producer_thread, NULL, producer_thread_main, producer) != 0) {
        kill(pid, SIGKILL);
        close(pipefd[0]);
        free(record);
        free(producer);
        snprintf(message, message_len, "producer thread creation failed");
        return -1;
    }
    record->producer_started = 1;

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd,
                              req->container_id,
                              pid,
                              req->soft_limit_bytes,
                              req->hard_limit_bytes);

    snprintf(message, message_len, "started %s pid=%d", req->container_id, pid);
    return 0;
}

static int wait_for_container(supervisor_ctx_t *ctx, const char *container_id)
{
    container_record_t *record;
    int done = 0;
    int status = 1;

    while (!done) {
        if (global_sigchld)
            reap_children(ctx, 0);

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_id(ctx, container_id);
        if (record != NULL && !is_live_state(record->state)) {
            status = record->exit_signal ? 128 + record->exit_signal : record->exit_code;
            done = 1;
        } else {
            pthread_mutex_unlock(&ctx->metadata_lock);
            pause();
            continue;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    return status;
}

static int stream_file_to_fd(const char *path, int fd)
{
    char buffer[1024];
    int input_fd;
    ssize_t nread;

    input_fd = open(path, O_RDONLY);
    if (input_fd < 0)
        return -1;

    while ((nread = read(input_fd, buffer, sizeof(buffer))) > 0) {
        if (write_full(fd, buffer, (size_t)nread) != nread) {
            close(input_fd);
            return -1;
        }
    }

    close(input_fd);
    return 0;
}

static void render_ps(supervisor_ctx_t *ctx, int fd)
{
    container_record_t *cur;
    char line[1024];
    struct tm tm_buf;
    size_t used;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (cur = ctx->containers; cur != NULL; cur = cur->next) {
        localtime_r(&cur->started_at, &tm_buf);
        used = (size_t)snprintf(line,
                                sizeof(line),
                                "%s pid=%d state=%s started=%04d-%02d-%02d %02d:%02d:%02d soft=%lu hard=%lu exit=%d signal=%d log=%s\n",
                                cur->id,
                                cur->host_pid,
                                state_to_string(cur->state),
                                tm_buf.tm_year + 1900,
                                tm_buf.tm_mon + 1,
                                tm_buf.tm_mday,
                                tm_buf.tm_hour,
                                tm_buf.tm_min,
                                tm_buf.tm_sec,
                                cur->soft_limit_bytes,
                                cur->hard_limit_bytes,
                                cur->exit_code,
                                cur->exit_signal,
                                cur->log_path);
        write_full(fd, line, used);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void shutdown_all_containers(supervisor_ctx_t *ctx)
{
    container_record_t *cur;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (cur = ctx->containers; cur != NULL; cur = cur->next) {
        if (is_live_state(cur->state)) {
            cur->stop_requested = 1;
            kill(cur->host_pid, SIGTERM);
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static int wait_for_state_change(supervisor_ctx_t *ctx,
                                 const char *container_id,
                                 useconds_t timeout_us)
{
    useconds_t waited = 0;

    while (waited < timeout_us) {
        container_record_t *record;
        int exited = 0;

        reap_children(ctx, 0);

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_id(ctx, container_id);
        if (record != NULL && !is_live_state(record->state))
            exited = 1;
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (exited)
            return 0;

        usleep(100000);
        waited += 100000;
    }

    return -1;
}

static void free_all_containers(supervisor_ctx_t *ctx)
{
    container_record_t *cur;
    container_record_t *next;

    pthread_mutex_lock(&ctx->metadata_lock);
    cur = ctx->containers;
    ctx->containers = NULL;
    pthread_mutex_unlock(&ctx->metadata_lock);

    while (cur != NULL) {
        next = cur->next;
        if (cur->producer_started)
            pthread_join(cur->producer_thread, NULL);
        free(cur);
        cur = next;
    }
}

static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    container_record_t *record;
    char message[CONTROL_MESSAGE_LEN];

    memset(&req, 0, sizeof(req));
    if (read_full(client_fd, &req, sizeof(req)) != (ssize_t)sizeof(req)) {
        write_response(client_fd, -1, "failed to read request");
        return;
    }

    memset(message, 0, sizeof(message));

    switch (req.kind) {
    case CMD_START:
        if (start_container(ctx, &req, message, sizeof(message)) != 0) {
            write_response(client_fd, -1, message);
            return;
        }
        write_response(client_fd, 0, message);
        return;
    case CMD_RUN:
        if (start_container(ctx, &req, message, sizeof(message)) != 0) {
            write_response(client_fd, -1, message);
            return;
        }
        write_response(client_fd, wait_for_container(ctx, req.container_id), message);
        return;
    case CMD_PS:
        write_response(client_fd, 0, "");
        render_ps(ctx, client_fd);
        return;
    case CMD_LOGS:
    {
        int log_fd;

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_id(ctx, req.container_id);
        if (record != NULL)
            copy_string(message, sizeof(message), record->log_path);
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (message[0] == '\0') {
            write_response(client_fd, -1, "container not found");
            return;
        }
        log_fd = open(message, O_RDONLY);
        if (log_fd < 0) {
            write_response(client_fd, -1, "failed to read log file");
            return;
        }
        close(log_fd);
        if (write_response(client_fd, 0, "") != 0)
            return;
        stream_file_to_fd(message, client_fd);
        return;
    }
    case CMD_STOP:
        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_by_id(ctx, req.container_id);
        if (record == NULL) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            write_response(client_fd, -1, "container not found");
            return;
        }
        record->stop_requested = 1;
        pthread_mutex_unlock(&ctx->metadata_lock);
        if (kill(record->host_pid, SIGTERM) != 0) {
            write_response(client_fd, -1, strerror(errno));
            return;
        }
        if (wait_for_state_change(ctx, req.container_id, 2000000) != 0) {
            kill(record->host_pid, SIGKILL);
            wait_for_state_change(ctx, req.container_id, 2000000);
        }
        write_response(client_fd, 0, "stop signal sent");
        return;
    default:
        write_response(client_fd, -1, "unknown command");
        return;
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;
        ssize_t written;
        size_t offset;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0)
            continue;

        offset = 0;
        while (offset < item.length) {
            written = write(fd, item.data + offset, item.length - offset);
            if (written < 0) {
                if (errno == EINTR)
                    continue;
                break;
            }
            offset += (size_t)written;
        }
        close(fd);
    }

    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = arg;
    int null_fd;

    if (sethostname(cfg->id, strlen(cfg->id)) != 0)
        return 1;

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0)
        return 1;

    if (chdir(cfg->rootfs) != 0)
        return 1;

    if (chroot(".") != 0)
        return 1;

    if (chdir("/") != 0)
        return 1;

    if (mkdir("/proc", 0555) != 0 && errno != EEXIST)
        return 1;

    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        return 1;

    if (cfg->nice_value != 0 && setpriority(PRIO_PROCESS, 0, cfg->nice_value) != 0)
        return 1;

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0)
        return 1;

    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0)
        return 1;

    null_fd = open("/dev/null", O_RDONLY);
    if (null_fd >= 0) {
        dup2(null_fd, STDIN_FILENO);
        close(null_fd);
    }

    close(cfg->log_write_fd);
    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    copy_string(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    copy_string(req.container_id, sizeof(req.container_id), container_id);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = pthread_cond_init(&ctx.metadata_cv, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_cond_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_cond_destroy(&ctx.metadata_cv);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /*
     * TODO:
     *   1) open /dev/container_monitor
     *   2) create the control socket / FIFO / shared-memory channel
     *   3) install SIGCHLD / SIGINT / SIGTERM handling
     *   4) spawn the logger thread
     *   5) enter the supervisor event loop
     */
    (void)rootfs;

    if (mkdir(LOG_DIR, 0755) != 0 && errno != EEXIST) {
        perror("mkdir logs");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.metadata_cv);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.metadata_cv);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind");
        close(ctx.server_fd);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.metadata_cv);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (listen(ctx.server_fd, 16) != 0) {
        perror("listen");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.metadata_cv);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGCHLD, &sa, NULL) != 0 ||
        sigaction(SIGINT, &sa, NULL) != 0 ||
        sigaction(SIGTERM, &sa, NULL) != 0) {
        perror("sigaction");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.metadata_cv);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        if (ctx.monitor_fd >= 0)
            close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_cond_destroy(&ctx.metadata_cv);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    while (!ctx.should_stop) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);

        if (client_fd < 0) {
            if (errno == EINTR) {
                reap_children(&ctx, 0);
                if (global_stop)
                    ctx.should_stop = 1;
                continue;
            }
            perror("accept");
            break;
        }

        reap_children(&ctx, 0);
        handle_client(&ctx, client_fd);
        close(client_fd);
        reap_children(&ctx, 0);
        if (global_stop)
            ctx.should_stop = 1;
    }

    shutdown_all_containers(&ctx);
    reap_children(&ctx, 1);
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    free_all_containers(&ctx);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_cond_destroy(&ctx.metadata_cv);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    struct sockaddr_un addr;
    control_response_t resp;
    char buffer[1024];
    int fd;
    ssize_t nread;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (write_full(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(fd);
        return 1;
    }

    if (read_full(fd, &resp, sizeof(resp)) != (ssize_t)sizeof(resp)) {
        perror("read");
        close(fd);
        return 1;
    }

    if (resp.message[0] != '\0')
        fprintf(resp.status == 0 || (req->kind == CMD_RUN && resp.status >= 0) ? stdout : stderr,
                "%s\n",
                resp.message);

    if (req->kind == CMD_PS || req->kind == CMD_LOGS) {
        while ((nread = read(fd, buffer, sizeof(buffer))) > 0)
            if (write_full(STDOUT_FILENO, buffer, (size_t)nread) != nread)
                break;
    }

    close(fd);
    if (req->kind == CMD_RUN)
        return resp.status >= 0 ? resp.status : 1;
    return resp.status == 0 ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);
    copy_string(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_string(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);
    copy_string(req.rootfs, sizeof(req.rootfs), argv[3]);
    copy_string(req.command, sizeof(req.command), argv[4]);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    copy_string(req.container_id, sizeof(req.container_id), argv[2]);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
