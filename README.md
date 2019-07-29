Aster [![Build Status](https://travis-ci.org/wayslog/aster.svg?branch=master)](https://travis-ci.org/wayslog/aster) [![LOC](https://tokei.rs/b1/github/wayslog/aster)](https://github.com/wayslog/aster)
======================

Aster is a light, fast and powerful cache proxy written in rust.

It supports `memcache`/`redis standalone`/`redis cluster` protocol all in one. Aster be use to proxy two cache models:

1. proxy mode(aka: standalone): the same as [twemproxy](https://github.com/twitter/twemproxy) but with multi-threads.
2. cluster mode: proxys requests to Redis cluster. Make redis cluster can be used by simple redis client. (that you can use the client without cluster support to access the redis cluster).(Inspired with [Corvus](https://github.com/eleme/corvus))

## Usage

```bash
cargo build --release && AS_CFG=as.toml RUST_LOG=libaster=info RUST_BACKTRACE=1 ./target/release/aster
```

## Configuration

```toml
[[clusters]]
# name of the cluster. Each cluster must has the unique name.

name="test-redis-cluster"

# listen_addr means the cluster font end serve address.

listen_addr="0.0.0.0:9001"

# cache_type only support memcache|redis|redis_cluster|memcache_binary

cache_type="redis_cluster"

# servers means cache backend. support two format:
# for cache_type is memcache or redis, you can set it as:
#
#   servers = [
#       "127.0.0.1:7001:10 redis-1",
#       "127.0.0.1:7002:10 redis-2",
#       "127.0.0.1:7003:10 redis-3"]
#
# as you can see, the format is considered as:
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

# server_retry_timeout means that when a node is removed from the proxy consist hash ring, 
# aster pinger will sleep until the timeout (ms) reached and trying to re-add the node again.

server_retry_timeout=600000
```
