# 新版 Tokio 代理设计草稿

> 目标：以 Tokio 1.x + async/await 重写 aster，仅支持 Redis standalone 和 Redis Cluster 模式，并兼容现有配置/CLI 行为与监控语义。

## 1. 整体架构

- **主入口 (`libaster::run`)**
  - 初始化日志、解析 CLI (`clap` v4)。
  - 读取配置（`serde` + `toml`），验证至少包含一个集群。
  - 构建 **Tokio multi-thread runtime**（`tokio::runtime::Builder::new_multi_thread`），显式控制 worker 数量与 IO/Timer 驱动。
  - 创建全局状态（metrics 注册、共享 executor handles 等）。
  - 对每个 cluster 使用 `tokio::task::JoinSet` 派发独立任务；网络和后台任务在 runtime 内统一调度。
  - 启动 Prometheus HTTP 服务与系统监控采集任务。

- **并发与同步**
  - 共享结构（如 `Router`、`ConnectionManager`、metrics 注册表）使用高效并发原语：
    - `parking_lot::Mutex/RwLock` 或 `tokio::sync::RwLock` 视是否需 async 访问而定。
    - 对频繁读写的映射（例如连接池索引）可选用 `dashmap`。
  - 任务间通信通过 `tokio::sync::{mpsc, watch}`，避免阻塞。

## 2. 配置与元信息

- `config` 模块
  - `Config` / `ClusterConfig` 保留字段，但：
    - `CacheType` 仅保留 `Redis` 与 `RedisCluster`。
    - 添加 `serde(default)` 以兼容缺省值。
  - 提供 `load(path) -> Result<Config>` 与 `validate()`。

- `meta` 模块
  - 使用 `tokio::task_local!` 或 `std::thread_local!` 保存 cluster 名称/IP/端口。
  - `load_meta` 负责推导对外 IP（读取 `HOST` 环境或使用 `local_ipaddress` crate）。

## 3. Metrics 与监控

- 依赖 `prometheus` + `axum`（或 `hyper`) 暴露 `/metrics`。
  - 推荐 `axum` + `tokio::signal` 优雅关闭。
- 指标保留原有语义：
  - `aster_front_connection`, `aster_front_connection_incr`, `aster_version`, `aster_memory_usage`, `aster_cpu_usage`, `aster_thread_count`, `aster_global_error`, `aster_total_timer`, `aster_remote_timer`。
  - 重新实现 `Tracker`，内部使用 `tokio::time::Instant` + `Drop`。
- 系统监控：
  - 使用 `sysinfo` 最新版本（0.29+），放到异步定时任务中，避免 rayon 依赖。

## 4. Redis 协议支持

- `protocol::redis`
  - 采用 `tokio_util::codec::{Framed, LengthDelimitedCodec}` 组合或手写 RESP 解析。
  - 保留命令抽象 `Command`/`Response`, 支持：
    - key 提取与 hash tag。
    - MOVED/ASK 解析。
    - 多 key 拆分与合并响应。
  - 考虑复用现有 RESP 解析逻辑（重写成 async 风格），或引入成熟 crate（如 `redis-protocol`）减少工作量。

- `protocol::cluster_slots`
  - 提供 `fetch_cluster_slots(connection) -> Future<ClusterLayout>`。
  - `ClusterLayout` 记录 master/slave slot 覆盖情况。

## 5. Standalone Redis 代理

- **核心结构**
  - `StandaloneCluster`
    - 维护一致性 hash ring（使用 `hash_ring` crate 或自实现）。
    - 管理后端池：`HashMap<NodeId, BackendConnHandle>`。
    - 提供 `dispatch(request) -> Future<Response>`。

- **前端处理**
  - `tokio::net::TcpListener` + `Framed` 处理客户端连接。
  - 对每个连接，启动 `async fn handle_client(...)`：
    - 读取请求 -> 根据 key 选择后端 -> 通过 pipeline 写入 -> 等待响应。
    - 支持批量：暂定使用并发 `send` + `JoinSet`，或简化为顺序处理，后续优化。

- **后端连接管理（连接池）**
  - 为每个后端节点维护一个连接池。
  - 客户端连接建立时，在池中为该客户端分配一个**会话句柄**，句柄内部为该客户端维护到目标节点的“专属”连接，保证同一客户端内命令顺序不被打乱。
    - 建议以 `ClientId`（例如使用 `Arc<AtomicU64>` 生成的自增 ID）为 key，池内保持 `HashMap<ClientId, BackendConn>`.
    - `BackendConn` 内部仍使用 `Framed<TcpStream, RedisCodec>` 顺序收发，保持 pipeline 有序。
  - 当客户端断开时释放句柄，其连接可以回收到池中（或延迟关闭）。
  - 连接失败自动重连（带指数退避），重连期间对该客户端的请求返回错误或阻塞等待，视具体策略而定。
  - 连接池本身由 `Router` 管理，使用 `tokio::sync::RwLock` 或 `dashmap` 保证多线程 runtime 下的高并发安全。

- **健康检查**
  - `tokio::time::interval` 定时发送 PING。
  - 连续失败达到阈值时，将节点标记为 down（从 hash ring 移除）。
  - 成功后恢复。

- **配置热重载（可选）**
  - 使用 `notify` crate (v6+) 监听配置文件变化。
  - 解析新配置后 diff，与旧值比较：
    - 新增节点 -> 建立连接。
    - 删除节点 -> 优雅关闭连接。
  - 只在 CLI `--reload` 启用时运行。

## 6. Redis Cluster 代理

- **结构**
  - `ClusterProxy`
    - `slots: Arc<RwLock<SlotMap>>` 维护 slot -> 主/从节点映射。
    - `connections: ConnectionManager` 管理节点连接池（内部使用并发安全容器）。
    - `fetch_trigger: watch::Sender<Trigger>` 用于刷新拓扑。
  - `ConnectionManager`
    - `HashMap<NodeAddr, BackendConn>`。
    - `BackendConn` 与 standalone 共用实现，但需要支持 `ASKING`/`READONLY`。

- **初始化**
  - 从 `servers` 列表轮询 seed 节点，执行 `CLUSTER SLOTS`。
  - 初始化 master/slave 映射，建立到所有 master（可选 slave）连接。

- **请求路径**
  - 客户端连接处理与 standalone 类似，但增加 cluster 逻辑：
    - 根据 key 计算 slot（CRC16）。
    - 获取目标 master / 可选 replica。
    - 使用客户端的 `ClientId` 从连接池中获取专属后端连接（若不存在则建立），通过该连接发送请求。
  - MOVED/ASK 处理：
    - 后端返回 MOVED -> 更新 slot 映射，向 `fetch_trigger` 发送刷新信号，再次转发。
    - ASK -> 临时发送到目标节点，并在请求前加入 `ASKING`。

- **拓扑刷新**
  - 异步任务监听 `fetch_trigger`（watch channel）和定时器。
  - 调用 `fetch_cluster_slots` 获取最新布局，diff 并更新：
    - slot 映射、连接池（增加/删除节点）。
  - 重试策略：指数退避、避免对同一错误频繁触发。

## 7. 错误处理与日志

- 使用 `thiserror` 定义统一错误类型 `ProxyError`。
- 对于用户可见错误，通过 RESP 错误信息返回。
- 日志基于 `tracing` + `tracing-subscriber`，同时在 metrics 中累积错误计数。

## 8. 依赖清单（初步）

```toml
[dependencies]
anyhow = "1"
axum = "0.7"
clap = { version = "4", features = ["derive"] }
futures = "0.3"
prometheus = "0.13"
redis-protocol = "4"         # or custom RESP codec
serde = { version = "1", features = ["derive"] }
serde_json = "1"
serde_yaml = "0.9"           # optional, for CLI YAML?
serde_with = "3"
thiserror = "1"
tokio = { version = "1", features = ["full"] }
tokio-util = "0.7"
toml = "0.8"
tracing = "0.1"
tracing-subscriber = "0.3"
notify = "6"                 # optional (reload)
sysinfo = "0.29"
parking_lot = "0.12"
uuid = { version = "1", features = ["v4"], optional = true }
hdrhistogram = "7"           # for latency tracking (if needed)

[dev-dependencies]
tokio-test = "0.4"
```

> 具体选择会在实现阶段根据需要微调，如改用 `hyper` 暴露指标或自定义 RESP。

## 9. 后续步骤

1. 基于此设计补充详细模块划分与接口定义。
2. 按模块实现：配置 & CLI → Metrics → 协议解析 → 后端连接 → Standalone → Cluster → 集成。
3. 添加端到端测试（使用 `redis` crate 模拟客户端）。
4. 更新文档与 README，说明新版限制（不再支持 Memcached）。
