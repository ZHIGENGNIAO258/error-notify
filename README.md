# 报错日志推送（error-notify）

MaiBot 插件：监控日志中的 **ERROR / CRITICAL** 报错，按周期（默认 30 分钟）聚合为一条摘要，通过 [Server酱](https://sct.ftqq.com/) 推送到手机；所有报错的**完整记录**（含异常堆栈）同步保留在本插件目录的 `errors.log` 中——推送缺失不丢记录。

## 为什么需要监听日志文件

MaiBot 的插件运行在独立 Runner 子进程中，插件 SDK 没有日志类 Hook / 事件（内置 Hook 清单与 EventType 事件均无日志点），因此无法在插件进程内直接获取主进程日志。本插件采用**监听日志文件**方案：增量读取 `logs/app_*.log.jsonl`（与 WebUI 日志面板同源的 JSONL 文件），零认证依赖、字段最全（含 `exception` 堆栈）。

## 工作原理

```
日志文件 (logs/app_*.log.jsonl)         插件进程
┌─────────────────────────┐   轮询    ┌──────────────────────────┐
│ ERROR / CRITICAL 行      │ ───────→ │ ① 实时追加写入 errors.log │
└─────────────────────────┘           │ ② 待推送缓冲（窗口内聚合）│
                                     └──────────┬───────────────┘
                                                │ 每 30 分钟（对齐整点/半点）
                                                ▼
                                      Server酱 POST …/SCTxxxx.send
```

- **扫描**：默认 5 秒轮询一次（可配 1~60 秒）；每次只对当前日志文件做 `stat`，文件未变化时零 IO；日志轮转（5MB/30 份）自动跟随最新文件，字节游标持久化，重启不重复处理。
- **推送语义（过期不候）**：每个错误带 `created_date`（本地日期）。每次推送只包含**当日**的错误；跨叶午窗口内昨日错误、超出每日上限后的错误**一律不再补推**（只留本地 `errors.log`）。每日推送计数在 00:00 自动重置，次日只推送次日新错误。
- **聚合**：同一错误（级别+模块+事件主题）在窗口内重复出现会合并为一条并标注 `×N`。

## 安装

1. 将整个 `error-notify` 文件夹放入 MaiBot 的 `plugins/` 目录（Docker 部署即容器内 `plugins/` 下）；
2. 重启 MaiBot 或在 WebUI 插件页重载插件；
3. 插件无第三方 Python 依赖（仅标准库），无需额外安装。

## 配置（WebUI 插件配置页，或编辑插件目录 `config.toml`）

配置为 `[plugin]` / `[serverchan]` / `[push]` 三段结构（MaiBot 插件配置规范要求配置模型必须包含 `[plugin]` 节及 `plugin.config_version`）：

```toml
[plugin]
config_version = "1.0.0"      # 配置版本（勿删）
enabled = true                # 插件总开关
logs_dir = "/MaiMBot/logs"    # MaiBot 日志目录（容器内路径）
scan_interval_sec = 5.0       # 日志扫描间隔（秒，1~60）
include_warning = false       # 是否同时推送 WARNING 级别
backfill_on_start = false     # 启动时回扫最近 backfill_minutes 分钟内的报错
backfill_minutes = 10         # 启动回扫时间范围（分钟）

[serverchan]
serverchan_sendkey = ""       # Server酱 SendKey（SCT 开头，留空只归档不推送）
serverchan_api_base = "https://sctapi.ftqq.com"  # 老版默认值，一般无需修改

[push]
flush_interval_min = 30       # 推送聚合周期（分钟），对齐整点/半点
daily_push_limit = 3          # 每日推送条数上限，超限后仅本地归档、次日不补推
max_entries_per_push = 50     # 单次推送最多逐条列出的错误数
desc_max_len = 4000           # 推送正文最大字符数（超出截断）
entry_summary_len = 200       # 单条错误摘要最大字符数
```

| 字段 | 默认 | 说明 |
|---|---|---|
| `plugin.enabled` | `true` | 插件总开关 |
| `plugin.logs_dir` | `/MaiMBot/logs` | MaiBot 日志目录（容器内路径）。自动探测顺序：本配置值 → `/MaiMBot/logs` → 插件上级目录 `logs/` → 工作目录 `logs/`；Docker 下通常无需修改 |
| `plugin.scan_interval_sec` | `5` | 日志扫描间隔（秒，1~60） |
| `plugin.include_warning` | `false` | 是否同时推送 WARNING 级别 |
| `plugin.backfill_on_start` | `false` | 启动时回扫最近 `backfill_minutes` 分钟内的报错（弥补重启窗口） |
| `plugin.backfill_minutes` | `10` | 启动回扫时间范围（分钟） |
| `serverchan.serverchan_sendkey` | 空 | Server酱 SendKey（`SCT` 开头，在 https://sct.ftqq.com/ 获取）。留空则只记录本地归档、不推送 |
| `serverchan.serverchan_api_base` | `https://sctapi.ftqq.com` | Server酱 API 地址，一般无需修改 |
| `push.flush_interval_min` | `30` | 推送聚合周期（分钟），按周期整数倍对齐（30 分钟即整点/半点） |
| `push.daily_push_limit` | `3` | 每日推送条数上限。按你的 Server酱套餐额度调整（免费版通常每日 3~5 条）；超限后仅记录本地归档 |
| `push.max_entries_per_push` | `50` | 单次推送最多逐条列出的错误数 |
| `push.desc_max_len` | `4000` | 推送正文最大字符数，超出截断（完整内容见 `errors.log`） |
| `push.entry_summary_len` | `200` | 单条错误摘要的最大字符数 |

> ⚠️ **从旧版本升级**：如果你的插件目录已存在旧版生成的扁平结构 `config.toml`（无 `[plugin]` 节），请先**删除该文件**，再重载插件，Runner 会按新配置模型重新生成。

## 本地文件

插件运行时会在插件目录下生成（均已加入 `.gitignore`）：

- `errors.log` — 完整错误归档（JSONL 一行一条：timestamp / level / logger_name / module / lineno / event / exception / created_date）；10MB 自动轮转，保留 3 份（`errors.log.1` ~ `.3`）；
- `state.json` — 文件游标与当日推送计数；
- `config.toml` — 运行时配置（Runner 依据配置模型生成）。

## 验证方法

1. **快速联调**：在 WebUI 插件配置把 `flush_interval_min` 临时改为 `1`，`serverchan_sendkey` 填入你的 SendKey；
2. 触发一条报错（或在 MaiBot 日志目录手动追加一行模拟，`timestamp` 必须为当前时间，因为插件启动后只处理新产生的错误、不重推历史）：
   ```bash
   echo '{"timestamp":"2026-01-01T00:00:00+08:00","level":"error","logger_name":"demo","module":"demo.py","lineno":1,"event":"测试报错"}' >> logs/app_00000000_000000_test.log.jsonl
   ```
   （把文件名替换为你实际的 `logs/app_*.log.jsonl`，把时间戳改为当前时间。）
3. 等待 1 个推送周期，手机应收到 `[MaiBot报错] N条…` 的聚合推送；
4. 检查插件目录 `errors.log` 已包含该条完整记录；
5. 确认无误后把 `flush_interval_min` 改回 `30`。

离线单元测试（无需 MaiBot 环境，机器上有 Python 3.11+ 即可）：

```bash
python tests/smoke_test.py
```

## 故障排查

| 现象 | 处理 |
|---|---|
| 日志提示「未找到日志目录」 | 在配置中填写容器内实际路径，如 `/MaiMBot/logs` |
| 日志提示「已达当日推送上限」 | 属预期；完整记录在 `errors.log`；次日自动恢复推送 |
| 手机没收到推送 | 确认 `serverchan_sendkey` 已填；查看插件日志中「Server酱推送失败」的返回码；确认 Server酱套餐当日额度未用尽 |
| 推送正文被截断 | 属预期（`desc_max_len`）；完整记录在 `errors.log` |

## 已适配环境

- MaiBot 主程序 1.0.0+，插件 SDK 2.5.0+
- 日志为 JSONL 格式（`logs/app_*.log.jsonl`，MaiBot 默认 `file_log_level=DEBUG`，无需修改配置）
- 用户确认的部署形态：Docker，容器内日志路径 `/MaiMBot/logs`（宿主机 `/vol1/1000/Docker/maim-bot/data/MaiMBot/logs` 由挂载映射，插件运行在容器内，配置用容器内路径）

## License

MIT
