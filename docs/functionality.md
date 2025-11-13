# aster 代理功能综述

## 目标与定位
- 提供一个让普通 Redis 客户端可以访问 Redis Cluster 的代理层，同时兼容单实例 Redis。
- 老版本同时支持 Memcached / Twemproxy 模式，但根据最新需求，**重写版本不再保留 Memcached 相关能力**。

## 二进制入口与 CLI
- 可执行文件 `aster-proxy`，入口函数在 `bin/proxy.rs` 调用 `libaster::run()`。
- CLI 通过 `clap` YAML (`src/cli.yml`) 定义：
  - `--config <FILE>`：必填，指定配置文件（默认 `default.toml`）。
  - `--ip <ADDR>`：覆盖 CLUSTER 命令中上报的本地 IP。
  - `--metrics <PORT>`：Prometheus 暴露端口，默认 2110。
  - `--reload`：启用配置热加载（仅 standalone 代理模式）。
  - `--version`：打印版本。

## 配置结构 (`default.toml` 及 `com::Config`)
- 配置由多个 `[[clusters]]` 构成，每个集群实例独立监听端口、维护后端连接。
- 核心字段：
  - `name`：集群名称。
  - `listen_addr`：前端监听地址（IP:Port）。
  - `cache_type`：`redis` 或 `redis_cluster`（Memcached 类型将删除）。
  - `servers`：
    - `redis`：列表项为 `"host:port:weight alias"` 或 `"host:port"`，支持节点权重和别名。
    - `redis_cluster`：列表项为 `"host:port"`，作为 cluster seed 节点。
  - `thread`：实例的工作线程数量（每线程一个 runtime）。
  - `read_timeout` / `write_timeout`：后端连接超时（毫秒）。
  - `hash_tag`：一致性 hash 标签（仅 standalone redis）。
  - cluster 模式特有：`fetch_interval`（拓扑刷新）、`read_from_slave`（读流量走副本）。
  - standalone 模式特有：`ping_fail_limit`、`ping_interval`、`ping_succ_interval` 控制健康检查。
- 运行时还会从环境变量 `ASTER_DEFAULT_THREAD` 读取默认线程数。

## 运行流程（Legacy）
1. 启动日志（`env_logger`），加载配置并断言至少存在一个 cluster。
2. 针对每个 cluster：
   - 根据 `cache_type` 选择 standalone（redis/twemproxy）或 cluster 模式。
   - 为每个工作线程 clone 配置并启动 `tokio::runtime::current_thread::Builder`。
   - 初始化线程本地的 `Meta`（cluster 名称/IP/端口）。
3. 启动 Prometheus HTTP 服务与系统资源采集线程（`actix-web` + `sysinfo`）。

## Redis Standalone 代理（Twemproxy 模式）
- 入口：`proxy::standalone::Cluster<T>`，`T` 为具体协议命令类型，重写时仅保留 `redis::Cmd`。
- 功能点：
  - 一致性 hash (`ketama.rs`)，支持节点权重、别名映射；根据 key（支持 hash tag）确定后端。
  - 前端 `front.rs` 负责：
    - 读取客户端请求、批量派发到后端；
    - 处理多 key 命令拆分、子命令合并（在 Redis 协议中由 `Cmd` 封装）。
  - 后端 `back.rs`：
    - 维护 pipeline 队列、转发命令、按顺序回收响应；
    - 支持命令超时、连接失败时重试/重建连接；
    - Blackhole 模式在后端不可用时将请求直接置为错误。
  - 健康检查 `ping.rs`：
    - 定时发送 `PING`；连续失败达到阈值时剔除节点，成功后恢复；
    - 失败期间仍尝试重连。
  - 配置热重载 `reload.rs`（watch 文件变化），对比新旧配置后动态增删节点。
- 现有实现使用 `tokio::codec` + `futures 0.1` channel，后端连接通过 `gethostbyname` 解析。

## Redis Cluster 代理
- 初始化：
  - `cluster::init::Initializer` 遍历 seed 节点发送 `CLUSTER SLOTS`；
  - 解析 master/replica 布局，建立初始连接池。
- 前端：
  - `cluster::front::Front` 处理客户端请求分派、响应回写；
  - 支持多条命令批量处理，遇到错误触发拓扑刷新。
- 后端：
  - `cluster::back::Back` 管理单节点 pipeline：
    - 维护发送队列、接收 RESP、超时检测；
    - 识别 MOVED/ASK 并通过 channel 交由 `redirect::RedirectHandler` 处理。
  - 副本连接发送 `READONLY`，支持按配置从 replica 读取。
- 拓扑刷新：
  - `fetcher::Fetch` 根据定时器、错误或 MOVED 触发重新获取 `CLUSTER SLOTS`；
  - 刷新成功后更新 slot->节点映射与后端连接池。
- 订阅与阻塞命令：
  - SUBSCRIBE / PSUBSCRIBE 会进入独占会话，按频道哈希槽选择节点，并在 MOVED / ASK 时自动重连与重放订阅；
  - BLPOP 等阻塞类命令复用独占连接，避免被 pipeline 请求阻塞。
- 备份读（backup request）：仅在 Redis Cluster 模式下可选启用；当 master 读命令在配置阈值上仍未返回时，会复制该请求至对应 replica，优先向客户端返回更快的副本响应，同时继续跟踪 master 延迟以动态更新阈值。
- 依赖大量 `Rc<RefCell<>>`、`futures::unsync::mpsc`，并使用 `tokio::runtime::current_thread`.

## 协议与命令抽象
- `protocol::redis`：
  - RESP 解析（`resp` 模块）、命令封装（`Cmd`），实现：
    - 命令分类（读/写/控制）、key hash、MOVED/ASK 处理、Reply 构造。
    - 多 key 命令拆分 / 聚合、超时跟踪、延迟统计。
  - Redis Cluster 特有辅助：`new_cluster_slots_cmd`、`slots_reply_to_replicas` 等。
- `protocol::mc`（将被移除）：Memcached 文本/二进制协议解析和命令封装。

## 监控与可观测性
- `metrics` 模块（Prometheus）：
  - 指标：前端连接数、命令耗时（总/远端）、错误计数、版本、线程数、进程 CPU/内存。
  - HTTP 暴露：使用 `actix-web` 在 `/metrics` 输出指标。
  - `metrics::Tracker` 用 Drop 统计直方图，前后端在关键路径打点。
- 系统资源采集（`measure_system`）周期性读取当前进程 CPU/内存。

## 限制与遗留问题
- 依赖 Tokio 0.1 / futures 0.1 / actix-web 1.0 等老版本生态，与现代 async/await 不兼容。
- 广泛使用 `Rc<RefCell<>>`、`lazy_static`、`failure`、`net2` 等已不推荐的组件。
- 多线程模型基于 `std::thread` + per-thread current-thread runtime，不利于升级。
- 配置热重载依赖 `hotwatch`（blocking 模式），难以在 async 环境直接复用。

## 重写范围与保留内容
- **功能保留**：
  - Redis Standalone 代理：一致性 hash、节点权重、健康检查、配置热重载（若继续需要）。
  - Redis Cluster 代理：CLUSTER SLOTS 初始化与刷新、MOVED/ASK 处理、读写分离、metrics。
  - CLI 配置项、TOML 结构、Prometheus 指标（语义和主要指标名称）。
  - 线程本地元信息（供日志/CLUSTER 命令使用）。
- **功能调整**：
  - 移除 Memcached 相关解析、命令、配置枚举值。
  - 迁移到 Tokio 最新版本（1.x async/await），减少阻塞与线程漂移。
  - 采用现代错误处理（`thiserror`/`anyhow`）与 async channel/stream。
