# 使用说明

## 构建与运行

```bash
cargo build --release
./target/release/aster-proxy --config ./default.toml
```

可选参数：

- `--ip <ADDR>`：覆盖对外暴露的 IP，供 Redis Cluster 的 `MOVED/ASK` 地址使用。
- `--metrics <PORT>`：Prometheus 指标监听端口，默认 `2110`。
- `--reload`：当前版本暂未实现动态热加载，开启后会打印提醒并忽略。

## 配置说明

配置文件为 TOML，包含若干 `[[clusters]]` 项，每个集群对应一个监听端口和后端集群。关键字段：

- `name`：集群名称。
- `listen_addr`：代理监听地址，如 `0.0.0.0:6379`。
- `cache_type`：`redis` 或 `redis_cluster`。
- `servers`：后端节点列表。
  - `redis` 模式：`host:port:weight alias`，可选权重和别名。
  - `redis_cluster` 模式：`host:port` 作为 seed 节点。
- `hash_tag`：一致性 hash 标签，例如 `{}`。
- `read_timeout` / `write_timeout`：后端超时（毫秒）。
- `read_from_slave`：Cluster 模式下允许从 replica 读取。
- `backup_request`：Cluster 模式下用于配置“副本兜底读”策略的表，包含：
  - `enabled`：是否开启该策略（默认 `false`）。
  - `trigger_slow_ms`：固定延迟阈值（毫秒，可写 `"default"` 关闭固定阈值），超过该延迟仍未返回则发送副本备份请求。
  - `multiplier`：相对阈值，等于“master 累计平均耗时 × multiplier”；当满足固定阈值或相对阈值任意条件即派发备份请求。
- `backup_request` 的三个字段均可通过 `CONFIG SET cluster.<name>.backup-request-*` 在线调整。
- `slowlog_log_slower_than`：慢查询阈值（微秒，默认 `10000`，设为 `-1` 关闭记录）。
- `slowlog_max_len`：慢查询日志最大保留条数（默认 `128`）。
- `hotkey_sample_every`：热点 Key 采样间隔（默认 `32`，越大代表对请求采样越稀疏）。
- `hotkey_sketch_width` / `hotkey_sketch_depth`：热点 Key 频率估算所用 Count-Min Sketch 宽度与深度，决定误差与内存占用。
- `hotkey_capacity`：HeavyKeeper 桶容量，用于保留候选热点 Key 数量上限。
- `hotkey_decay`：HeavyKeeper 衰减系数，取值 `(0, 1]`，越接近 `1` 越倾向保留历史数据。
- `auth` / `password`：前端 ACL，详见下文。
- `backend_auth` / `backend_password`：后端 ACL 认证，详见下文。

> 提示：代理原生支持 `SLOWLOG GET/LEN/RESET`，并按集群维度汇总慢查询；配置上述阈值和长度即可控制记录行为。
>
> 热点 Key 分析可通过 `HOTKEY ENABLE|DISABLE|GET|RESET` 控制，相关采样参数可在配置文件或 `CONFIG SET cluster.<name>.hotkey-*` 中动态调整。

示例参见仓库根目录的 `default.toml`。

### ACL 配置

前端代理支持 Redis ACL 语法，与旧版 `password` 字段兼容：

```toml
# 仅配置 default 用户（兼容旧版）
password = "frontend-secret"

# 或使用更完整的 ACL 表：
auth = { password = "frontend-default", users = [
    { username = "ops", password = "ops-secret" },
    { username = "audit", password = "audit-secret" },
] }
```

后端在握手阶段会发送 `AUTH`，可配置用户名或纯密码：

```toml
# 仅密码
backend_password = "backend-secret"

# 用户名 + 密码
backend_auth = { username = "proxy", password = "backend-secret" }
```

所有模式都会在认证失败时拒绝客户端命令，并确保与后端建立的连接已经通过 `AUTH`。

## Docker Compose 集成测试

仓库在 `docker/docker-compose.integration.yml` 中提供了端到端测试环境：

```bash
docker compose -f docker/docker-compose.integration.yml up --build integration-tests
```

该命令会：

1. 启动一个 Redis standalone 实例与 3 节点 Redis Cluster。
2. 构建并运行 aster-proxy（监听 6380 / 6381）。
3. 执行 `docker/integration-test.sh`，通过 `redis-cli` 验证代理在两个模式下的读写能力，并覆盖 BLPOP 阻塞与 SUBSCRIBE 推送场景。

测试完成后可使用 `docker compose -f docker/docker-compose.integration.yml down -v` 清理资源。

## 指标

代理默认启动 http server 提供 `/metrics`，可用于 Prometheus 抓取，指标名称沿用旧版 aster（如 `aster_front_connection`、`aster_total_timer` 等）。
