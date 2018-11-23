Aster [![Build Status](https://travis-ci.org/wayslog/aster.svg?branch=master)](https://travis-ci.org/wayslog/asswecan)
======================

Aster is a light, fast and powerful cache proxy written by rust.

It supports memcache/redis singleton/redis cluster protocol all in one. Aster can proxy with two models means:

1. proxy mode: the same with [twemproxy](https://github.com/twitter/twemproxy) but support multi-threads.
2. cluster mode: proxy for redis cluster. Make redis cluster can be used to simple redis client.(which means that you can use redis non-cluster client access the redis cluster api).(inspire with [Corvus](https://github.com/eleme/corvus))

## Usage

```bash
cargo build --release && AS_CFG="as.toml" RUST_LOG=libaster=info RUST_BACKTRACE=1 ./target/release/aster
```

## Configuration

```
[[clusters]]
# name of the cluster. Each cluster means one front-end port.
#
name="test-redis-cluster"

# listen_addr means the cluster font end serve address.
#
listen_addr="0.0.0.0:9001"

# cache_type only support memcache|redis|redis_cluster
#
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
#
servers = ["127.0.0.1:7000", "127.0.0.1:7001"]

# Work thread number, it's suggested as the number of your cpu(hyper-thread) number.
#
thread = 1

############################# Cluster Mode Special #######################################################
# fetch means fetch interval for backend cluster to keep cluster info become newer.
# default 10 * 60 seconds
#
fetch = 600

############################# Proxy Mode Special #######################################################
# ping_fail_limit means when ping fail reach the limit number, the node will be ejected from the cluster
# until the ping is ok in future.
# if ping_fali_limit == 0, means that close the ping eject feature.
#
ping_fail_limit=3

# ping_interval means the interval of each ping was send into backend node in millisecond.
ping_interval=10000
```
