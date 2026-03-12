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
};

struct Session {
    Endpoint client;
    Endpoint upstream;
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
    delete s;
}

void mark_for_close(Session* s, Session** list, int& count) {
    if (!s || s->closing) return;
    s->closing = true;
    if (count < MAX_EVENTS) {
        list[count++] = s;
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
        s->client = {s, client_fd, false};
        s->upstream = {s, upstream_fd, true};

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

    // edge-triggered to reduce epoll wakeups
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = nullptr;
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    printf("server relay listening on 127.0.0.1:%u\n", LOCAL_PORT);

    epoll_event events[MAX_EVENTS];
    char buf[4096];

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

            Endpoint* peer = ep->is_upstream ? &s->client : &s->upstream;

            // drain socket until EAGAIN
            bool dead = false;
            while (true) {
                ssize_t bytes = recv(ep->fd, buf, sizeof(buf), 0);
                if (bytes > 0) {
                    send(peer->fd, buf, bytes, 0);
                    continue;
                }
                if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    break;
                }
                if (bytes < 0 && errno == EINTR) {
                    continue;
                }
                dead = true;
                break;
            }

            if (dead) {
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
