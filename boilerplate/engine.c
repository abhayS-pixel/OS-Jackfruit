#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <time.h>
#include <signal.h>
#include <sys/stat.h>

#define META_FILE "containers.db"
#define LOG_DIR "logs"

typedef struct {
    char id[32];
    pid_t pid;
    long start;
    char state[16];
    char log[128];
} container;

/* ---------- HELPERS ---------- */

void ensure_logs() {
    mkdir(LOG_DIR, 0755);
}

void save_container(container c) {
    FILE *f = fopen(META_FILE, "a");
    fprintf(f, "%s %d %ld %s %s\n",
        c.id, c.pid, c.start, c.state, c.log);
    fclose(f);
}

/* ---------- RUN ---------- */

void run_container(char *id, char *rootfs, char *cmd) {

    char log_path[128];
    sprintf(log_path, "logs/%s.log", id);

    int fd = open(log_path, O_CREAT | O_WRONLY | O_APPEND, 0644);

    pid_t pid = fork();

    if (pid == 0) {
        dup2(fd, 1);
        dup2(fd, 2);
        close(fd);

        chroot(rootfs);
        chdir("/");

        execl("/bin/sh", "sh", "-c", cmd, NULL);
        perror("exec failed");
        exit(1);
    }

    container c;
    strcpy(c.id, id);
    c.pid = pid;
    c.start = time(NULL);
    strcpy(c.state, "running");
    strcpy(c.log, log_path);

    save_container(c);

    printf("Container %s started with PID %d\n", id, pid);
}

/* ---------- PS ---------- */

void print_ps() {
    FILE *f = fopen(META_FILE, "r");
    if (!f) {
        printf("No containers\n");
        return;
    }

    char id[32], state[16], log[128];
    pid_t pid;
    long start;

    printf("ID\tPID\tSTATE\tSTART\tLOG\n");

    while (fscanf(f, "%s %d %ld %s %s",
                  id, &pid, &start, state, log) == 5) {

        int status;
        pid_t r = waitpid(pid, &status, WNOHANG);

        if (r > 0) {
            if (WIFEXITED(status))
                strcpy(state, "exited");
            else
                strcpy(state, "killed");
        }

        char t[32];
        strftime(t, 32, "%H:%M:%S", localtime(&start));

        printf("%s\t%d\t%s\t%s\t%s\n",
               id, pid, state, t, log);
    }

    fclose(f);
}

/* ---------- LOGS ---------- */

void show_logs(char *id) {
    char path[128];
    sprintf(path, "logs/%s.log", id);

    FILE *f = fopen(path, "r");
    if (!f) {
        printf("No logs\n");
        return;
    }

    char buf[256];
    while (fgets(buf, sizeof(buf), f))
        printf("%s", buf);

    fclose(f);
}

/* ---------- STOP ---------- */

void stop_container(char *id) {
    FILE *f = fopen(META_FILE, "r");

    char cid[32], state[16], log[128];
    pid_t pid;
    long start;

    while (fscanf(f, "%s %d %ld %s %s",
                  cid, &pid, &start, state, log) == 5) {

        if (strcmp(cid, id) == 0) {
            kill(pid, SIGTERM);
            printf("Container %s stopped\n", id);
        }
    }

    fclose(f);
}

/* ---------- MAIN ---------- */

int main(int argc, char *argv[]) {

    if (argc < 2) {
        printf("Usage: run/ps/logs/stop\n");
        return 1;
    }

    ensure_logs();

    if (strcmp(argv[1], "run") == 0) {
        if (argc < 5) {
            printf("Usage: run <id> <rootfs> <cmd>\n");
            return 1;
        }
        run_container(argv[2], argv[3], argv[4]);
    }

    else if (strcmp(argv[1], "ps") == 0) {
        print_ps();
    }

    else if (strcmp(argv[1], "logs") == 0) {
        show_logs(argv[2]);
    }

    else if (strcmp(argv[1], "stop") == 0) {
        stop_container(argv[2]);
    }

    else if (strcmp(argv[1], "supervisor") == 0) {
        printf("Supervisor started (dummy)\n");
        while (1) sleep(1);
    }

    return 0;
}
