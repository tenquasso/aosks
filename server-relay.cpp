#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

constexpr uint16_t LOCAL_PORT = 1082;
constexpr uint16_t UPSTREAM_PORT = 1081;
const char* UPSTREAM_IP = "172.28.16.1";
constexpr int MAX_EVENTS = 64;

struct Session;

struct Endpoint {
    Session* session = nullptr;
    int fd = -1;
    bool is_upstream = false;
};

struct Session {
    Endpoint client;
    Endpoint upstream;
};

void close_session(int epfd, Session* s) {
    if (!s) return;
    if (s->client.fd >= 0) {
        epoll_ctl(epfd, EPOLL_CTL_DEL, s->client.fd, nullptr);
        close(s->client.fd);
    }
    if (s->upstream.fd >= 0) {
        epoll_ctl(epfd, EPOLL_CTL_DEL, s->upstream.fd, nullptr);
        close(s->upstream.fd);
    }
    delete s;
}

int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int main() {
    // ignore sigpipe when client closes unexpectedly
    signal(SIGPIPE, SIG_IGN);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(LOCAL_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0 ||
        listen(server_fd, 128) < 0) {
        perror("bind/listen failed");
        close(server_fd);
        return 1;
    }

    int epfd = epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    printf("relay listening on 127.0.0.1:%u -> %s:%u\n", LOCAL_PORT, UPSTREAM_IP, UPSTREAM_PORT);

    epoll_event events[MAX_EVENTS];
    char buf[4096];

    while (true) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == server_fd) {
                int client_fd = accept(server_fd, nullptr, nullptr);
                if (client_fd < 0) continue;

                int upstream_fd = socket(AF_INET, SOCK_STREAM, 0);
                sockaddr_in up_addr{};
                up_addr.sin_family = AF_INET;
                up_addr.sin_port = htons(UPSTREAM_PORT);
                inet_pton(AF_INET, UPSTREAM_IP, &up_addr.sin_addr);

                if (connect(upstream_fd, (sockaddr*)&up_addr, sizeof(up_addr)) < 0) {
                    perror("upstream connect failed");
                    close(client_fd);
                    close(upstream_fd);
                    continue;
                }

                set_nonblock(client_fd);
                set_nonblock(upstream_fd);

                Session* s = new Session();
                s->client = {s, client_fd, false};
                s->upstream = {s, upstream_fd, true};

                epoll_event cev{};
                cev.events = EPOLLIN;
                cev.data.ptr = &s->client;
                epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &cev);

                epoll_event uev{};
                uev.events = EPOLLIN;
                uev.data.ptr = &s->upstream;
                epoll_ctl(epfd, EPOLL_CTL_ADD, upstream_fd, &uev);
            } else {
                Endpoint* ep = static_cast<Endpoint*>(events[i].data.ptr);
                Session* s = ep->session;
                Endpoint* peer = ep->is_upstream ? &s->client : &s->upstream;

                ssize_t bytes = recv(ep->fd, buf, sizeof(buf), 0);
                if (bytes <= 0) {
                    close_session(epfd, s);
                } else {
                    send(peer->fd, buf, bytes, 0);
                }
            }
        }
    }

    close(server_fd);
    return 0;
}
