# Realm Panel (`realm-panel`)

基于 [`zhboner/realm`](https://github.com/zhboner/realm) 二次开发的增强型管理与可观测性面板。本子项目以独立 Cargo Workspace 放在 `realm_panel/` 目录下，通过 `path` 依赖直接复用上游 `realm` / `realm_core` / `realm_io` / `realm_syscall` / `realm_lb` / `realm_hook` 库，**不修改上游源码**。

## 核心特性

1. **零侵入上游源码扩展**
   - 保持标准 `realm` TOML/JSON 配置文件完全兼容，可随时与原版配置双向导入导出。
2. **稳定配置热重载（规则变更强制断连）**
   - 支持 Web UI 修改、外部文件监控防抖（350ms）与 `SIGHUP` 信号触发热重载。
   - 变更前预校验配置语法、监听端口冲突与传输参数；对新增/修改/禁用/删除规则做差量比对。
   - 修改、禁用或删除某条规则时通过 `CancellationToken` **立即强制断开该规则上的活跃 TCP/UDP 会话**，未变动的其它转发规则保持连接零中断。
   - 自动保存最近 20 份配置快照，支持一键回滚。
3. **嵌入式 Web UI（单文件二进制）**
   - 前端基于 React 18 + TypeScript + Tailwind CSS + ECharts 构建，通过 `rust-embed` 直接打包进 `realm-panel` 单二进制中。
   - 内置 Argon2 + HMAC-SHA256 鉴权，默认监听 `127.0.0.1:18080`。
4. **双向流量统计（实时 + 7 天 / 30 天）**
   - TCP 基于 Linux `splice` 零拷贝实现入站（`rx`）/ 出站（`tx`）实时精确计量（遇到不支持的流自动回退到带计量内存拷贝）；UDP 基于 `realm_io::mmsg` 批量收发计量。
   - 持久化至嵌入式 SQLite（`WAL` 模式），提供 `1h`（秒级）、`24h`（分钟级）、`7d`（分钟级）、`30d`（小时级）双向流量、峰值速率与单端点排名视图。
5. **实时入口设备与速度监控**
   - 按 `(来源 IP, 协议)` 实时聚合每个转发规则的接入设备信息：在线状态、活跃连接数、最近来源端口、实时入/出方向速率（`rx_bps` / `tx_bps`）、累计流量及在线时长（断连保留 60s 观察窗口）。
   - 支持在规则列表行内展开查看接入设备，或在实时大屏直接筛选排查。
6. **性能开销统计**
   - 每秒采集进程 CPU%、RSS/VSZ 内存、打开文件描述符与 `nofile` 上限、OS 线程数、Tokio 工作线程与活跃任务数，并与宿主机总 CPU / 内存占用同图对比，持久化保留 35 天历史趋势。
7. **转发目的端口可达性探测**
   - 定时（可按规则配置探测间隔与超时）及手动立即探测主目标与 `extra_remotes`，输出 DNS 解析耗时、TCP 连接 RTT、UDP 路由探测结果及连续失败计数，异常恢复均有日志与历史记录。
8. **详细可观测日志**
   - 基于 `tracing` + `tracing-log` 统一收拢面板与上游 `realm_core` 的 `log::*` 事件，输出控制台与按天轮转 JSON 日志文件（`realm-panel.log`），支持 Web UI 实时日志流推送、按级别/规则/关键词检索与运行期动态切换日志级别。

---

## 快速运行

### 方式一：从 GitHub Releases / Actions 下载预编译产物

在仓库的 **Releases** 页面下载 `dev-latest`（或正式版本标签）中的 `realm-panel-linux-x86_64.tar.gz`，解压后即可直接运行：

```bash
tar -xzf realm-panel-linux-x86_64.tar.gz
chmod +x realm-panel-linux-x86_64
./realm-panel-linux-x86_64 --config ./realm.toml --web-listen 127.0.0.1:18080 --admin-pass 'YourStrongPass!'
```

### 方式二：源码编译

环境要求：Node.js >= 20、Rust (`stable`)、C 工具链（`gcc` / `cmake`，用于编译 `aws-lc-rs` 与内置 `rusqlite`）。

```bash
# 1. 构建嵌入式 Web UI 前端资源（rust-embed 编译期会打包 frontend/dist）
cd realm_panel/frontend
npm ci
npm run build

# 2. 编译 Release 单文件二进制
cd ..
cargo build --release
```

编译产物位于 `realm_panel/target/release/realm-panel`。

---

## 命令行参数与环境变量

| 参数 | 环境变量 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `-c, --config <PATH>` | `REALM_CONFIG` | `./realm.toml` | 标准 `realm` 配置文件（`.toml` / `.json`）或目录 |
| `-w, --web-listen <ADDR>` | `REALM_WEB_LISTEN` | `127.0.0.1:18080` | Web UI 与 REST/WS API 监听地址 |
| `--db <PATH>` | `REALM_PANEL_DB` | `./realm-panel.db` | SQLite 历史流量、性能、探测与快照数据库路径 |
| `--log-dir <DIR>` | `REALM_LOG_DIR` | `./logs` | 按天轮转结构化 JSON 日志目录 |
| `--log-level <LEVEL>` | `REALM_LOG_LEVEL` | `info` | 初始日志过滤级别（`trace`/`debug`/`info`/`warn`/`error`） |
| `--admin-user <USER>` | `REALM_ADMIN_USER` | `admin` | 首次启动初始化的管理员用户名 |
| `--admin-pass <PASS>` | `REALM_ADMIN_PASS` | 首次未传则自动生成并在启动日志打印 | 首次启动初始化的管理员密码 |
| `-p, --pipe-page <N>` | — | — | Linux `splice` 管道容量（单位 4K 页） |
| `-n, --nofile <N>` | — | — | 设置最大打开文件描述符数量限制 |
| `-j, --pre-conn-hook <SO>` | — | — | 加载前置连接 Hook 动态库（`.so`） |
| `-d, --daemon` | — | `false` | 以后台守护进程方式运行（Unix） |

> [!IMPORTANT]
> 默认 Web UI 仅绑定 `127.0.0.1:18080`。若需公网访问，建议配合 Nginx / Caddy 反代启用 HTTPS 或通过 SSH 隧道转发，并在首次登录后及时修改强密码。

## 运行测试

```bash
cd realm_panel
cargo test
```
