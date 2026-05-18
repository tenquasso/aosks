#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

constexpr uint16_t LOCAL_PORT = 1082;
constexpr uint16_t UPSTREAM_PORT = 1081;
constexpr int MAX_EVENTS = 1024;
constexpr size_t SPLICE_LEN = 65536;

static int epfd = -1;

in_addr_t get_gateway_ip() {
    FILE* fp = fopen("/proc/net/route", "r");
    if (!fp) return INADDR_NONE;

    char line[256];
    fgets(line, sizeof(line), fp);

    in_addr_t best_gw = INADDR_NONE;
    unsigned int best_metric = UINT32_MAX;

    while (fgets(line, sizeof(line), fp)) {
        char iface[32];
        unsigned int dest = 0, gw = 0, flags = 0, metric = UINT32_MAX;
        if (sscanf(line, "%31s %x %x %x %*u %*u %u",
                   iface, &dest, &gw, &flags, &metric) != 5)
            continue;

        if (dest == 0 && (flags & 0x3) == 0x3 &&
            gw != 0 && metric < best_metric) {
            best_gw = static_cast<in_addr_t>(gw);
            best_metric = metric;
        }
    }
    fclose(fp);
    return best_gw;
}

struct Session;

struct Endpoint {
    Session* session = nullptr;
    int fd = -1;
    bool is_upstream = false;
    bool read_eof = false;
    bool write_eof = false;
};

struct Session {
    Endpoint client;
    Endpoint upstream;
    int pipe_c2u[2] = {-1, -1};
    int pipe_u2c[2] = {-1, -1};
    ssize_t c2u_buffered = 0;
    ssize_t u2c_buffered = 0;
    bool closing = false;
};

void close_session(Session* s) {
    if (!s) return;
    if (s->client.fd >= 0) {
        epoll_ctl(epfd, EPOLL_CTL_DEL, s->client.fd, nullptr);
        close(s->client.fd);
    }
    if (s->upstream.fd >= 0) {
        epoll_ctl(epfd, EPOLL_CTL_DEL, s->upstream.fd, nullptr);
        close(s->upstream.fd);
    }
    for (int fd : s->pipe_c2u) if (fd >= 0) close(fd);
    for (int fd : s->pipe_u2c) if (fd >= 0) close(fd);
    delete s;
}

void mark_for_close(Session* s, Session** list, int& count) {
    if (!s || s->closing) return;
    s->closing = true;
    if (count < MAX_EVENTS) {
        list[count++] = s;
    }
}

bool pump_splice(Endpoint& src, Endpoint& dst,
                 int pipefd[2], ssize_t& buffered) {
    for (;;) {
        while (buffered > 0) {
            ssize_t n = splice(pipefd[0], nullptr, dst.fd, nullptr,
                               static_cast<size_t>(buffered),
                               SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
            if (n > 0) {
                buffered -= n;
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
            return false;
        }

        if (src.read_eof) {
            if (!dst.write_eof) {
                shutdown(dst.fd, SHUT_WR);
                dst.write_eof = true;
            }
            return true;
        }

        ssize_t n = splice(src.fd, nullptr, pipefd[1], nullptr,
                           SPLICE_LEN, SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
        if (n > 0) {
            buffered += n;
            continue;
        }
        if (n == 0) {
            src.read_eof = true;
            continue;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
        return false;
    }
}

void handle_accept(int server_fd) {
    for (;;) {
        int client_fd = accept4(server_fd, nullptr, nullptr,
                                SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            perror("accept4");
            break;
        }

        in_addr_t gateway = get_gateway_ip();
        if (gateway == INADDR_NONE) {
            fprintf(stderr, "default gateway not found\n");
            close(client_fd);
            continue;
        }

        int upstream_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (upstream_fd < 0) {
            close(client_fd);
            continue;
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(UPSTREAM_PORT);
        addr.sin_addr.s_addr = gateway;

        connect(upstream_fd, (sockaddr*)&addr, sizeof(addr));

        Session* s = new Session();
        s->client = {s, client_fd, false, false, false};
        s->upstream = {s, upstream_fd, true, false, false};

        if (pipe2(s->pipe_c2u, O_NONBLOCK | O_CLOEXEC) < 0 ||
            pipe2(s->pipe_u2c, O_NONBLOCK | O_CLOEXEC) < 0) {
            close_session(s);
            continue;
        }

        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
        ev.data.ptr = &s->client;
        epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev);

        ev.data.ptr = &s->upstream;
        epoll_ctl(epfd, EPOLL_CTL_ADD, upstream_fd, &ev);
    }
}

int main() {
    signal(SIGPIPE, SIG_IGN);

    int server_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    int one = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(LOCAL_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0 ||
        listen(server_fd, SOMAXCONN) < 0) {
        perror("bind/listen failed");
        close(server_fd);
        return 1;
    }

    epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) {
        perror("epoll_create1");
        close(server_fd);
        return 1;
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = nullptr;
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    printf("server relay listening on 127.0.0.1:%u\n", LOCAL_PORT);

    epoll_event events[MAX_EVENTS];

    for (;;) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        Session* to_close[MAX_EVENTS];
        int close_count = 0;

        for (int i = 0; i < n; ++i) {
            if (events[i].data.ptr == nullptr) {
                handle_accept(server_fd);
                continue;
            }

            Endpoint* ep = static_cast<Endpoint*>(events[i].data.ptr);
            Session* s = ep->session;
            if (!s || s->closing) continue;

            if (!pump_splice(s->client, s->upstream,
                             s->pipe_c2u, s->c2u_buffered) ||
                !pump_splice(s->upstream, s->client,
                             s->pipe_u2c, s->u2c_buffered)) {
                mark_for_close(s, to_close, close_count);
                continue;
            }

            if (s->client.read_eof && s->upstream.read_eof &&
                s->c2u_buffered == 0 && s->u2c_buffered == 0) {
                mark_for_close(s, to_close, close_count);
            }
        }

        for (int i = 0; i < close_count; ++i) {
            close_session(to_close[i]);
        }
    }

    close(epfd);
    close(server_fd);
    return 0;
}
