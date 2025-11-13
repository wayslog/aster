# aster

Aster [![LOC](https://tokei.rs/b1/github/wayslog/aster)](https://github.com/wayslog/aster)
======================

Aster is a lightweight, fast but powerful cache proxy written in Rust.

当前版本聚焦在 Redis 生态：

1. **Standalone 模式**：使用一致性哈希 + 连接池代理普通 Redis 集群。
2. **Redis Cluster 模式**：兼容普通 Redis 客户端访问原生 cluster，自动处理 `MOVED/ASK` 与拓扑刷新。

## Usage

```bash
cargo build --release
./target/release/aster-proxy --config ./default.toml
```

更多命令行参数与配置说明可参考 [docs/usage.md](docs/usage.md)。

## Integration Tests (Docker Compose)

项目提供基于 Docker Compose 的端到端集成测试：

```bash
docker compose -f docker/docker-compose.integration.yml up --build integration-tests
```

该命令会启动一组 Redis 实例（包括 Redis Cluster），构建并运行 aster-proxy，并使用 `redis-cli` 对代理进行验收（读写命令）。

## Configuration

```
[[clusters]]
# name of the cluster. Each cluster means one front-end port.

name="test-redis-cluster"

# listen_addr means the cluster font end serve address.

listen_addr="0.0.0.0:9001"

# cache_type only support redis|redis_cluster

cache_type="redis_cluster"

# servers means cache backend. support two format:
# for cache_type is redis, you can set it as:
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

# backup_request duplicates slow reads to replica nodes when enabled.
# trigger_slow_ms decides the fixed delay (set "default" or remove field to rely on moving average).
# multiplier is applied to the rolling average latency to determine another trigger threshold.
backup_request = { enabled = false, trigger_slow_ms = 5, multiplier = 2.0 }

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
