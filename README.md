# aster

Aster [![LOC](https://tokei.rs/b1/github/wayslog/aster)](https://github.com/wayslog/aster)
======================

Aster is a lightweight, fast but powerful cache proxy written in rust.

It supports memcache/redis singleton/redis cluster protocol all in one. Aster can proxy with two models:

1. proxy mode: the same as [twemproxy](https://github.com/twitter/twemproxy).
2. cluster mode: proxy for redis cluster. You can use non-cluster redis client access the redis cluster.(Inspired with [Corvus](https://github.com/eleme/corvus))

## Usage

```bash
cargo build --all --release && RUST_LOG=libaster=info RUST_BACKTRACE=1 ./target/release/aster default.toml
```

## Configuration

```
[[clusters]]
# name of the cluster. Each cluster means one front-end port.

name="test-redis-cluster"

# listen_addr means the cluster font end serve address.

listen_addr="0.0.0.0:9001"

# cache_type only support memcache|redis|redis_cluster

cache_type="redis_cluster"

# servers means cache backend. support two format:
# for cache_type is memcache or redis, you can set it as:
#
#   servers = [
#       "127.0.0.1:7001:10 redis-1",
#       "127.0.0.1:7002:10 redis-2",
#       "127.0.0.1:7003:10 redis-3"]
#
# as you can see, the format is consisted with:
#
#       "${addr}:hash_weight ${node_alias}"
#
# And, for redis_cluster you can set the item as:
#
# servers = ["127.0.0.1:7000", "127.0.0.1:7001"]
#
# which means the seed nodes to connect to redis cluster.

servers = ["127.0.0.1:7000", "127.0.0.1:7001"]

# Work thread number, it's suggested as the number of your cpu(hyper-thread) number.

thread = 1

# ReadTimeout is the socket read timeout which effects all in the socket in millisecond

read_timeout = 2000

# WriteTimeout is the socket write timeout which effects all in the socket in millisecond

write_timeout = 2000

############################# Cluster Mode Special #######################################################
# fetch means fetch interval for backend cluster to keep cluster info become newer.
# default 10 * 60 seconds

fetch = 600


# read_from_slave is the feature make slave balanced readed by client and ignore side effects.
read_from_slave = true

############################# Proxy Mode Special #######################################################
# ping_fail_limit means when ping fail reach the limit number, the node will be ejected from the cluster
# until the ping is ok in future.
# if ping_fali_limit == 0, means that close the ping eject feature.

ping_fail_limit=3

# ping_interval means the interval of each ping was send into backend node in millisecond.

ping_interval=10000
```

## changelog

see [CHANGELOG.md](/CHANGELOG.md)