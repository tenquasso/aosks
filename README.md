personal use

zero-copy tcp relay.

listens on 127.0.0.1:1082 and forwards traffic to the default gateway on port 1081. resolves gateway ip automatically from /proc/net/route and uses linux splice() for zero-copy forwarding.

build & run:
```bash
make
./server-relay
```
