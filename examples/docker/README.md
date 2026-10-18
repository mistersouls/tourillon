# Docker Compose example

This example runs one Tourillon node and a Tourctl container for client commands.
It requires Docker and Docker Compose and uses the Linux ARM64 or x86_64
bundles published on GitHub.

This is a local single-node example, not a production deployment. See
[current capabilities and limitations](../../docs/status.md).

## Start

From this directory:

```sh
docker-compose up --build -d
```

The default version is `0.1.0.dev1`. To select another version published for
both products:

```sh
VERSION=0.1.0.dev1 docker-compose up --build -d
```

The `init` service generates certificates and configuration files on the first
run. Node data and configuration are stored in a persistent volume. Tourctl
stays running with `cat` so you can use `exec` to run client commands.

## Use the client

Open a shell in the client container, as shown in the README animation:

```sh
docker-compose exec tourctl sh
export TOURCONF=/example/tourconf.toml
tourctl put greeting "Hello, Tourillon!"
tourctl get greeting
tourctl delete greeting
exit
```

Alternatively, run each command from the host:

```sh
docker-compose exec tourctl tourctl put greeting "Hello, Tourillon!" --tourconf /example/tourconf.toml
docker-compose exec tourctl tourctl get greeting --tourconf /example/tourconf.toml
docker-compose exec tourctl tourctl delete greeting --tourconf /example/tourconf.toml
```

## Logs and shutdown

```sh
docker-compose logs -f tourillon
docker-compose down
```

`down` preserves data and configuration. To delete the volume, including
certificates and all example data:

```sh
docker-compose down --volumes
```

Ports 7000 and 7001 are published only on `127.0.0.1`. This single-node example
is not a production configuration. Do not share the volume: it contains
private keys.

With Compose v2, replace `docker-compose` with `docker compose`.

If you used the earlier `demo` volume, the renamed `example` volume starts
with new data and certificates. The old volume is not automatically migrated
or deleted.
