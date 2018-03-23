#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

u_int8_t is_exit;

static void sigdown(int signo) {
    is_exit = 1;
}

static void sigreap(int signo) {
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;
}

int main(void) {

    if (sigaction(SIGINT, &(struct sigaction){.sa_handler = sigdown}, NULL))
        return 1;
    if (sigaction(SIGTERM, &(struct sigaction){.sa_handler = sigdown}, NULL))
        return 2;
    if (sigaction(SIGCHLD, &(struct sigaction){.sa_handler = sigreap, .sa_flags = SA_NOCLDSTOP}, NULL))
        return 3;

    while (!is_exit)
        pause();

    return 0;
}
