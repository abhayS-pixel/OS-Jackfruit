/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Complete implementation with:
 *   - UNIX domain socket control-plane IPC
 *   - Container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - Producer/consumer log buffering
 *   - Signal handling and graceful shutdown
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
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
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
    container_record_t *containers;
} supervisor_ctx_t;

static supervisor_ctx_t *g_supervisor_ctx = NULL;

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
 * Producer-side insertion into the bounded buffer.
 * Blocks when full, fails on shutdown.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while buffer is full and not shutting down */
    while (buffer->count >= LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    /* Fail if shutdown began */
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Insert item */
    memcpy(&buffer->items[buffer->tail], item, sizeof(log_item_t));
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    /* Wake up consumers */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

/*
 * Consumer-side removal from the bounded buffer.
 * Waits when empty, returns -1 on shutdown with empty buffer.
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while buffer is empty and not shutting down */
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    /* Exit if shutting down and buffer is empty */
    if (buffer->shutting_down && buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Remove item */
    memcpy(item, &buffer->items[buffer->head], sizeof(log_item_t));
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    /* Wake up producers */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

/*
 * Logging consumer thread.
 * Continuously pops log chunks and writes them to container log files.
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    FILE *log_file;
    char full_path[PATH_MAX];

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0) {
            /* Shutdown and buffer drained */
            break;
        }

        /* Open log file in append mode */
        snprintf(full_path, sizeof(full_path), "%s/%s.log", LOG_DIR, item.container_id);
        log_file = fopen(full_path, "a");
        if (log_file) {
            fwrite(item.data, 1, item.length, log_file);
            fclose(log_file);
        } else {
            fprintf(stderr, "Failed to open log file: %s\n", full_path);
        }
    }

    return NULL;
}

/*
 * Clone child entrypoint.
 * Sets up namespaces, filesystem, and executes the container command.
 */
int child_fn(void *arg)
{
    child_config_t *config = (child_config_t *)arg;
    char *args[4];
    char proc_mount[PATH_MAX];

    /* Set nice value if specified */
    if (config->nice_value != 0) {
        if (nice(config->nice_value) < 0 && errno != 0) {
            perror("nice");
        }
    }

    /* Set hostname to container ID */
    if (sethostname(config->id, strlen(config->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    /* Mount proc filesystem */
    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        /* Might fail if already mounted, continue */
    }

    /* Change root to container rootfs */
    if (chdir(config->rootfs) < 0) {
        perror("chdir to rootfs");
        return 1;
    }

    if (chroot(config->rootfs) < 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") < 0) {
        perror("chdir to /");
        return 1;
    }

    /* Mount /proc inside the new root */
    snprintf(proc_mount, sizeof(proc_mount), "/proc");
    mkdir(proc_mount, 0755); /* Create if not exists */
    if (mount("proc", proc_mount, "proc", 0, NULL) < 0) {
        /* May already be mounted */
    }

    /* Redirect stdout and stderr to logging pipe */
    if (config->log_write_fd >= 0) {
        dup2(config->log_write_fd, STDOUT_FILENO);
        dup2(config->log_write_fd, STDERR_FILENO);
        close(config->log_write_fd);
    }

    /* Execute the command using sh -c */
    args[0] = "/bin/sh";
    args[1] = "-c";
    args[2] = config->command;
    args[3] = NULL;

    execvp(args[0], args);
    perror("execvp");
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
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* Helper: Find container by ID */
static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *curr = ctx->containers;
    while (curr) {
        if (strcmp(curr->id, id) == 0)
            return curr;
        curr = curr->next;
    }
    return NULL;
}

/* Helper: Add container to list */
static void add_container(supervisor_ctx_t *ctx, container_record_t *record)
{
    record->next = ctx->containers;
    ctx->containers = record;
}

/* Helper: Create logging pipe reader thread */
static void *log_reader_thread(void *arg)
{
    struct {
        int fd;
        char container_id[CONTAINER_ID_LEN];
        bounded_buffer_t *buffer;
    } *params = arg;

    char read_buf[LOG_CHUNK_SIZE];
    ssize_t nread;
    log_item_t item;

    while ((nread = read(params->fd, read_buf, sizeof(read_buf))) > 0) {
        strncpy(item.container_id, params->container_id, sizeof(item.container_id) - 1);
        item.container_id[sizeof(item.container_id) - 1] = '\0';
        item.length = nread;
        memcpy(item.data, read_buf, nread);

        if (bounded_buffer_push(params->buffer, &item) != 0) {
            break;
        }
    }

    close(params->fd);
    free(params);
    return NULL;
}

/* Handle start/run command */
static int handle_start_container(supervisor_ctx_t *ctx,
                                   const control_request_t *req,
                                   control_response_t *resp,
                                   int is_blocking)
{
    container_record_t *record;
    child_config_t *config;
    void *stack;
    pid_t child_pid;
    int pipefd[2];
    pthread_t reader_tid;
    struct {
        int fd;
        char container_id[CONTAINER_ID_LEN];
        bounded_buffer_t *buffer;
    } *reader_params;

    pthread_mutex_lock(&ctx->metadata_lock);

    /* Check if container already exists */
    if (find_container(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' already exists", req->container_id);
        return -1;
    }

    /* Create container record */
    record = calloc(1, sizeof(container_record_t));
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Out of memory");
        return -1;
    }

    strncpy(record->id, req->container_id, sizeof(record->id) - 1);
    record->started_at = time(NULL);
    record->state = CONTAINER_STARTING;
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    /* Create pipe for logging */
    if (pipe(pipefd) < 0) {
        free(record);
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Failed to create pipe");
        return -1;
    }

    /* Allocate child config and stack */
    config = malloc(sizeof(child_config_t));
    stack = malloc(STACK_SIZE);
    if (!config || !stack) {
        close(pipefd[0]);
        close(pipefd[1]);
        free(config);
        free(stack);
        free(record);
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Out of memory");
        return -1;
    }

    strncpy(config->id, req->container_id, sizeof(config->id) - 1);
    strncpy(config->rootfs, req->rootfs, sizeof(config->rootfs) - 1);
    strncpy(config->command, req->command, sizeof(config->command) - 1);
    config->nice_value = req->nice_value;
    config->log_write_fd = pipefd[1];

    /* Clone container with namespaces */
    child_pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWUTS | SIGCHLD,
                      config);

    if (child_pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        free(config);
        free(stack);
        free(record);
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Clone failed: %s", strerror(errno));
        return -1;
    }

    /* Close write end in parent */
    close(pipefd[1]);

    /* Update record */
    record->host_pid = child_pid;
    record->state = CONTAINER_RUNNING;

    /* Register with monitor */
    if (ctx->monitor_fd >= 0) {
        register_with_monitor(ctx->monitor_fd, req->container_id, child_pid,
                              req->soft_limit_bytes, req->hard_limit_bytes);
    }

    /* Start log reader thread */
    reader_params = malloc(sizeof(*reader_params));
    if (reader_params) {
        reader_params->fd = pipefd[0];
        strncpy(reader_params->container_id, req->container_id, sizeof(reader_params->container_id) - 1);
        reader_params->buffer = &ctx->log_buffer;
        pthread_create(&reader_tid, NULL, log_reader_thread, reader_params);
        pthread_detach(reader_tid);
    } else {
        close(pipefd[0]);
    }

    add_container(ctx, record);
    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message), "Container '%s' started (PID %d)", 
             req->container_id, child_pid);

    /* If blocking (run), wait for container to exit */
    if (is_blocking) {
        int status;
        waitpid(child_pid, &status, 0);
        
        pthread_mutex_lock(&ctx->metadata_lock);
        if (WIFEXITED(status)) {
            record->exit_code = WEXITSTATUS(status);
            record->state = CONTAINER_EXITED;
        } else if (WIFSIGNALED(status)) {
            record->exit_signal = WTERMSIG(status);
            record->state = CONTAINER_KILLED;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    return 0;
}

/* Handle ps command */
static int handle_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    container_record_t *curr;
    char buffer[CONTROL_MESSAGE_LEN];
    int offset = 0;

    pthread_mutex_lock(&ctx->metadata_lock);

    offset += snprintf(buffer + offset, sizeof(buffer) - offset,
                       "ID                PID     STATE      STARTED\n");

    curr = ctx->containers;
    while (curr && offset < (int)sizeof(buffer) - 100) {
        char time_str[64];
        struct tm *tm_info = localtime(&curr->started_at);
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);

        offset += snprintf(buffer + offset, sizeof(buffer) - offset,
                           "%-16s  %-6d  %-9s  %s\n",
                           curr->id, curr->host_pid,
                           state_to_string(curr->state),
                           time_str);
        curr = curr->next;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    strncpy(resp->message, buffer, sizeof(resp->message) - 1);
    return 0;
}

/* Handle logs command */
static int handle_logs(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp)
{
    container_record_t *record;
    char log_path[PATH_MAX];
    FILE *log_file;
    char buffer[CONTROL_MESSAGE_LEN];
    size_t nread;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container(ctx, req->container_id);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Container '%s' not found", req->container_id);
        return -1;
    }
    strncpy(log_path, record->log_path, sizeof(log_path) - 1);
    pthread_mutex_unlock(&ctx->metadata_lock);

    log_file = fopen(log_path, "r");
    if (!log_file) {
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Log file not found");
        return -1;
    }

    nread = fread(buffer, 1, sizeof(buffer) - 1, log_file);
    buffer[nread] = '\0';
    fclose(log_file);

    resp->status = 0;
    strncpy(resp->message, buffer, sizeof(resp->message) - 1);
    return 0;
}

/* Handle stop command */
static int handle_stop(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp)
{
    container_record_t *record;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container(ctx, req->container_id);
    if (!record) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Container '%s' not found", req->container_id);
        return -1;
    }

    if (record->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Container '%s' is not running", req->container_id);
        return -1;
    }

    /* Send SIGTERM to container */
    if (kill(record->host_pid, SIGTERM) < 0) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, sizeof(resp->message), "Failed to stop container");
        return -1;
    }

    record->state = CONTAINER_STOPPED;
    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message), "Container '%s' stopped", req->container_id);
    return 0;
}

/* Handle client request in supervisor */
static void handle_client_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    ssize_t nread;

    memset(&resp, 0, sizeof(resp));

    nread = recv(client_fd, &req, sizeof(req), 0);
    if (nread != sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Invalid request");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    switch (req.kind) {
    case CMD_START:
        handle_start_container(ctx, &req, &resp, 0);
        break;
    case CMD_RUN:
        handle_start_container(ctx, &req, &resp, 1);
        break;
    case CMD_PS:
        handle_ps(ctx, &resp);
        break;
    case CMD_LOGS:
        handle_logs(ctx, &req, &resp);
        break;
    case CMD_STOP:
        handle_stop(ctx, &req, &resp);
        break;
    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Unknown command");
        break;
    }

    send(client_fd, &resp, sizeof(resp), 0);
}

/* Signal handler */
static void signal_handler(int sig)
{
    if (g_supervisor_ctx) {
        g_supervisor_ctx->should_stop = 1;
    }
}

/* SIGCHLD handler */
static void sigchld_handler(int sig)
{
    int status;
    pid_t pid;

    (void)sig;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_supervisor_ctx)
            continue;

        pthread_mutex_lock(&g_supervisor_ctx->metadata_lock);
        container_record_t *curr = g_supervisor_ctx->containers;
        while (curr) {
            if (curr->host_pid == pid) {
                if (WIFEXITED(status)) {
                    curr->exit_code = WEXITSTATUS(status);
                    curr->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    curr->exit_signal = WTERMSIG(status);
                    curr->state = CONTAINER_KILLED;
                }

                /* Unregister from monitor */
                if (g_supervisor_ctx->monitor_fd >= 0) {
                    unregister_from_monitor(g_supervisor_ctx->monitor_fd, curr->id, pid);
                }
                break;
            }
            curr = curr->next;
        }
        pthread_mutex_unlock(&g_supervisor_ctx->metadata_lock);
    }
}

/*
 * Supervisor main loop
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    struct sockaddr_un addr;
    struct sigaction sa_term, sa_chld;
    fd_set readfds;
    int max_fd;

    (void)rootfs; /* Base rootfs not used in this implementation */

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    g_supervisor_ctx = &ctx;

    /* Initialize metadata lock */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    /* Initialize bounded buffer */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* Create log directory */
    mkdir(LOG_DIR, 0755);

    /* Open monitor device */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr, "Warning: Could not open /dev/container_monitor: %s\n", strerror(errno));
    }

    /* Create UNIX domain socket */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    unlink(CONTROL_PATH);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }

    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        goto cleanup;
    }

    /* Install signal handlers */
    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = signal_handler;
    sigaction(SIGINT, &sa_term, NULL);
    sigaction(SIGTERM, &sa_term, NULL);

    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = sigchld_handler;
    sa_chld.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    /* Start logging thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create");
        goto cleanup;
    }

    fprintf(stderr, "Supervisor started. Listening on %s\n", CONTROL_PATH);

    /* Main event loop */
    while (!ctx.should_stop) {
        FD_ZERO(&readfds);
        FD_SET(ctx.server_fd, &readfds);
        max_fd = ctx.server_fd;

        struct timeval tv = {.tv_sec = 1, .tv_usec = 0};
        rc = select(max_fd + 1, &readfds, NULL, NULL, &tv);

        if (rc < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (rc == 0)
            continue;

        if (FD_ISSET(ctx.server_fd, &readfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd >= 0) {
                handle_client_request(&ctx, client_fd);
                close(client_fd);
            }
        }
    }

cleanup:
    fprintf(stderr, "Shutting down supervisor...\n");

    /* Cleanup */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    if (ctx.server_fd >= 0) {
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
    }

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    /* Free container list */
    while (ctx.containers) {
        container_record_t *next = ctx.containers->next;
        free(ctx.containers);
        ctx.containers = next;
    }

    pthread_mutex_destroy(&ctx.metadata_lock);
    g_supervisor_ctx = NULL;

    return 0;
}

/*
 * Client-side control request
 */
static int send_control_request(const control_request_t *req)
{
    int sock_fd;
    struct sockaddr_un addr;
    control_response_t resp;
    ssize_t nsent, nrecv;

    sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        fprintf(stderr, "Is the supervisor running?\n");
        close(sock_fd);
        return 1;
    }

    nsent = send(sock_fd, req, sizeof(*req), 0);
    if (nsent != sizeof(*req)) {
        perror("send");
        close(sock_fd);
        return 1;
    }

    nrecv = recv(sock_fd, &resp, sizeof(resp), 0);
    if (nrecv != sizeof(resp)) {
        perror("recv");
        close(sock_fd);
        return 1;
    }

    close(sock_fd);

    if (resp.status == 0) {
        printf("%s\n", resp.message);
        return 0;
    } else {
        fprintf(stderr, "Error: %s\n", resp.message);
        return 1;
    }
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

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
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

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
