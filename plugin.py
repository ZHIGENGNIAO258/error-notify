"""MaiBot 报错日志推送插件。

监控 MaiBot 的 JSONL 日志文件（logs/app_*.log.jsonl）中的 ERROR / CRITICAL
报错，实时写入本插件目录下的 errors.log 完整归档，并按配置周期（默认 30 分钟）
通过 Server酱 聚合推送一条摘要到手机。

设计要点：
- MaiBot 插件运行在独立 Runner 子进程中，SDK 没有日志类 Hook / 事件，
  Host 主进程的日志无法在插件进程内直接获取，因此采用"监听日志文件"方案。
- 推送语义为"过期不候"：跨日期的积压错误、超出每日条数上限的错误都不会
  在之后补推，完整内容始终保留在本地 errors.log 中。
"""

from __future__ import annotations

import asyncio
import json
import logging
import logging.handlers
import time
import urllib.parse
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Any

from maibot_sdk import CONFIG_RELOAD_SCOPE_SELF, Field, MaiBotPlugin, PluginConfigBase

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

APP_LOG_PATTERN = "app_*.log.jsonl"
ARCHIVE_FILE = "errors.log"
STATE_FILE = "state.json"

ARCHIVE_MAX_BYTES = 10 * 1024 * 1024  # 10MB
ARCHIVE_BACKUP_COUNT = 3

TARGET_LEVELS = {"ERROR", "CRITICAL", "WARNING"}

PUSH_MAX_PENDING = 5000  # 待推送缓冲上限，超出丢弃最旧条目（本地已归档）


class PluginSectionConfig(PluginConfigBase):
    """插件基础配置"""

    __ui_label__ = "基础设置"
    __ui_icon__ = "settings"
    __ui_order__ = 0

    config_version: str = Field(default="1.0.0", description="配置版本")
    enabled: bool = Field(default=True, description="是否启用插件")
    logs_dir: str = Field(
        default="/MaiMBot/logs",
        description="MaiBot 日志目录（容器内路径，如 /MaiMBot/logs）",
        json_schema_extra={"placeholder": "/MaiMBot/logs", "group": "basic"},
    )
    scan_interval_sec: float = Field(
        default=5.0,
        description="日志扫描间隔（秒，1~60）",
        json_schema_extra={"placeholder": "5", "group": "basic"},
    )
    include_warning: bool = Field(
        default=False,
        description="是否同时推送 WARNING 级别日志",
        json_schema_extra={"group": "basic"},
    )
    backfill_on_start: bool = Field(
        default=False,
        description="启动时回扫最近一段时间内已发生的报错（弥补重启窗口）",
        json_schema_extra={"group": "basic"},
    )
    backfill_minutes: int = Field(
        default=10,
        description="启动回扫的时间范围（分钟）",
        json_schema_extra={"group": "basic"},
    )


class ServerChanSectionConfig(PluginConfigBase):
    """Server酱 配置"""

    __ui_label__ = "Server酱"
    __ui_icon__ = "send"
    __ui_order__ = 1

    serverchan_sendkey: str = Field(
        default="",
        description="Server酱 SendKey（SCT 开头；留空则仅记录本地归档、不推送）",
        json_schema_extra={"placeholder": "SCT...", "group": "serverchan"},
    )
    serverchan_api_base: str = Field(
        default="https://sctapi.ftqq.com",
        description="Server酱 API 地址（老版默认 sctapi.ftqq.com）",
        json_schema_extra={"group": "serverchan"},
    )


class PushSectionConfig(PluginConfigBase):
    """推送策略配置"""

    __ui_label__ = "推送策略"
    __ui_icon__ = "notifications"
    __ui_order__ = 2

    flush_interval_min: int = Field(
        default=30,
        description="推送聚合周期（分钟），按周期整数倍对齐（30 分钟即整点/半点）",
        json_schema_extra={"group": "push"},
    )
    daily_push_limit: int = Field(
        default=3,
        description="每日推送条数上限，超限后仅记录本地归档，次日不补推",
        json_schema_extra={"group": "push"},
    )
    max_entries_per_push: int = Field(
        default=50,
        description="单次推送最多逐条列出的错误数",
        json_schema_extra={"group": "push"},
    )
    desc_max_len: int = Field(
        default=4000,
        description="推送正文最大字符数（超出截断，完整记录见 errors.log）",
        json_schema_extra={"group": "push"},
    )
    entry_summary_len: int = Field(
        default=200,
        description="单条错误摘要的最大字符数",
        json_schema_extra={"group": "push"},
    )


class ErrorNotifyConfig(PluginConfigBase):
    """报错日志推送配置"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    serverchan: ServerChanSectionConfig = Field(default_factory=ServerChanSectionConfig)
    push: PushSectionConfig = Field(default_factory=PushSectionConfig)


class ErrorNotifyPlugin(MaiBotPlugin):
    """监控日志报错并通过 Server酱 聚合推送。"""

    config_model = ErrorNotifyConfig

    def __init__(self) -> None:
        super().__init__()
        self._plugin_dir = Path(__file__).resolve().parent
        self._archive_logger: logging.Logger | None = None

        self._scan_task: asyncio.Task | None = None
        self._flush_task: asyncio.Task | None = None
        self._stopping = False

        self._logs_dir: Path | None = None
        self._current_file: Path | None = None
        self._file_offset = 0
        self._min_ts = 0.0  # 时间过滤下界（unix 秒），防止恢复游标时重推历史

        self._pending: list[dict[str, Any]] = []
        self._today = date.today()
        self._pushed_today = 0
        self._sendkey_warned = False
        self._state_data: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def on_load(self) -> None:
        self._setup_archive()
        cfg = self.config
        self._logs_dir = self._resolve_logs_dir(cfg.plugin.logs_dir)
        self._min_ts = self._compute_min_ts(cfg)
        self._load_state()
        # 启动先扫描一次：确定当前日志文件、恢复游标并处理积压增量
        if self._logs_dir is not None:
            try:
                self._scan_once()
            except Exception:
                self.ctx.logger.exception("首次日志扫描失败")

        self._scan_task = asyncio.create_task(self._scan_loop(), name="error-notify-scan")
        self._flush_task = asyncio.create_task(self._flush_loop(), name="error-notify-flush")

        self.ctx.logger.info(
            "报错日志推送已加载，日志目录=%s，推送周期=%d 分钟，每日上限=%d",
            self._logs_dir or "(未找到，请在配置中填写 logs_dir)",
            int(cfg.push.flush_interval_min),
            int(cfg.push.daily_push_limit),
        )
        if not cfg.serverchan.serverchan_sendkey:
            self.ctx.logger.warning("未配置 serverchan_sendkey，仅记录本地错误归档，暂不推送")

    async def on_unload(self) -> None:
        self._stopping = True
        tasks = [t for t in (self._scan_task, self._flush_task) if t is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        # 卸载时未推送的缓冲直接丢弃（本地已归档，过期不候）
        self._pending.clear()
        self._save_state()
        self.ctx.logger.info("报错日志推送已卸载")

    async def on_config_update(self, scope: str, config_data: dict, version: str) -> None:
        if scope != CONFIG_RELOAD_SCOPE_SELF:
            return
        cfg = self.config
        self._logs_dir = self._resolve_logs_dir(cfg.plugin.logs_dir)
        self._min_ts = self._compute_min_ts(cfg)
        # 日志目录可能变化，重置文件游标，从新目录开头重新发现
        self._current_file = None
        self._file_offset = 0
        self._sendkey_warned = False
        self.ctx.logger.info("报错日志推送配置已更新: version=%s, logs_dir=%s", version, self._logs_dir)

    # ------------------------------------------------------------------
    # 日志扫描
    # ------------------------------------------------------------------

    async def _scan_loop(self) -> None:
        while not self._stopping:
            try:
                if self.config.plugin.enabled and self._logs_dir is not None:
                    self._scan_once()
            except Exception:
                self.ctx.logger.exception("错误日志扫描失败")
            interval = max(0.5, min(60.0, float(self.config.plugin.scan_interval_sec)))
            await asyncio.sleep(interval)

    def _scan_once(self) -> None:
        log_dir = self._logs_dir
        if log_dir is None or not log_dir.is_dir():
            return
        try:
            files = [
                p
                for p in log_dir.iterdir()
                if p.is_file() and p.name.startswith("app_") and p.name.endswith(".log.jsonl")
            ]
        except OSError:
            return
        if not files:
            return

        try:
            current = max(files, key=lambda p: p.stat().st_mtime)
        except OSError:
            return
        if current != self._current_file:
            old_file = self._current_file
            old_offset = self._file_offset
            # 首次发现或发生轮转：先补读旧文件可能残留的尾行（轮转瞬间写入）
            if old_file is not None:
                self._drain_tail(old_file, old_offset)
            self._current_file = current
            # 尝试从持久化状态恢复新文件的字节游标；否则从 0 开始
            # （时间过滤 _min_ts 会兜底，防止恢复失败时重推历史错误）
            if str(current) == str(self._state_data.get("file") or ""):
                try:
                    self._file_offset = max(0, int(self._state_data.get("offset") or 0))
                except (TypeError, ValueError):
                    self._file_offset = 0
            else:
                self._file_offset = 0

        try:
            size = current.stat().st_size
        except OSError:
            return
        if self._file_offset > size:
            self._file_offset = 0  # 文件被截断/替换
        if size <= self._file_offset:
            return

        try:
            with open(current, "r", encoding="utf-8", errors="replace") as f:
                f.seek(self._file_offset)
                chunk = f.read()
                self._file_offset = f.tell()
        except OSError:
            return

        for line in chunk.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            self._handle_entry(entry)

        self._save_state()

    def _drain_tail(self, path: Path, offset: int) -> None:
        """轮转瞬间补读旧文件的残留尾行，避免切换文件时丢失最后几行。"""
        if offset <= 0:
            return
        try:
            size = path.stat().st_size
        except OSError:
            return
        if size <= offset:
            return
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                f.seek(offset)
                chunk = f.read()
        except OSError:
            return
        for line in chunk.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            self._handle_entry(entry)

    def _handle_entry(self, entry: dict) -> None:
        if not isinstance(entry, dict):
            return
        level = str(entry.get("level") or "").strip().upper()
        if level not in TARGET_LEVELS:
            return
        if level == "WARNING" and not self.config.plugin.include_warning:
            return

        ts = self._parse_ts(entry.get("timestamp"))
        if ts <= 0:
            ts = time.time()
        if ts < self._min_ts:
            return

        record = {
            "timestamp": entry.get("timestamp") or datetime.fromtimestamp(ts).isoformat(timespec="seconds"),
            "ts": ts,
            "level": level,
            "logger_name": str(entry.get("logger_name") or entry.get("module") or "-"),
            "module": str(entry.get("module") or ""),
            "lineno": entry.get("lineno"),
            "event": str(entry.get("event") or ""),
            "exception": str(entry.get("exception") or ""),
            "created_date": datetime.fromtimestamp(ts).date().isoformat(),
        }
        self._write_archive(record)
        self._pending.append(record)
        if len(self._pending) > PUSH_MAX_PENDING:
            # 只丢"待推送"副本，本地归档不受影响
            self._pending = self._pending[-PUSH_MAX_PENDING:]

    # ------------------------------------------------------------------
    # 周期推送
    # ------------------------------------------------------------------

    async def _flush_loop(self) -> None:
        while not self._stopping:
            interval = max(1, int(self.config.push.flush_interval_min))
            delay = self._seconds_to_next_boundary(interval)
            # +1s 余量，让当前窗口完整闭合
            await asyncio.sleep(delay + 1.0)
            try:
                await self._flush_once()
            except Exception:
                self.ctx.logger.exception("周期推送执行失败")

    async def _flush_once(self) -> None:
        cfg = self.config
        if not cfg.plugin.enabled:
            return
        pending = self._pending
        self._pending = []
        if not pending:
            return

        today = date.today()
        if today != self._today:
            # 跨日：自然重置计数；昨日残留条目按"过期不候"丢弃（仅本地归档）
            self._today = today
            self._pushed_today = 0

        target = [r for r in pending if r.get("created_date") == today.isoformat()]
        if not target:
            return

        if self._pushed_today >= int(cfg.push.daily_push_limit):
            self.ctx.logger.warning(
                "已达当日推送上限（%d），本批 %d 条错误不再推送（完整记录见 %s）",
                int(cfg.push.daily_push_limit),
                len(target),
                self._plugin_dir / ARCHIVE_FILE,
            )
            return

        sendkey = str(cfg.serverchan.serverchan_sendkey or "").strip()
        if not sendkey:
            if not self._sendkey_warned:
                self._sendkey_warned = True
                self.ctx.logger.warning("未配置 serverchan_sendkey，本批 %d 条错误仅记录本地归档", len(target))
            return

        title, desp = self._build_push_message(target, cfg)
        ok = await self._send_serverchan(cfg, sendkey, title, desp)
        if ok:
            self._pushed_today += 1
            self._save_state()
            self.ctx.logger.info(
                "已推送 %d 条错误（今日第 %d/%d 次）", len(target), self._pushed_today, int(cfg.push.daily_push_limit)
            )

    def _build_push_message(self, target: list[dict], cfg: ErrorNotifyConfig) -> tuple[str, str]:
        # 同一错误在窗口内重复出现时合并为一条并计数
        merged: dict[tuple, dict] = {}
        for r in target:
            key = (r["level"], r["logger_name"], r["event"][:80])
            item = merged.get(key)
            if item is None:
                merged[key] = {"record": r, "count": 1}
            else:
                item["count"] += 1
        ordered = sorted(merged.values(), key=lambda x: x["record"]["ts"])

        error_n = sum(1 for r in target if r["level"] == "ERROR")
        critical_n = sum(1 for r in target if r["level"] == "CRITICAL")
        warning_n = sum(1 for r in target if r["level"] == "WARNING")

        summary_len = max(20, int(cfg.push.entry_summary_len))
        max_entries = max(1, int(cfg.push.max_entries_per_push))
        lines: list[str] = []
        for item in ordered[:max_entries]:
            record = item["record"]
            t = datetime.fromtimestamp(record["ts"]).strftime("%H:%M:%S")
            summary = record["event"] or (record["exception"].splitlines()[0] if record["exception"] else "-")
            summary = summary.strip()[:summary_len]
            location = record["logger_name"]
            if record.get("module"):
                location = f"{record['module']}:{record.get('lineno') or '?'}"
            suffix = f" ×{item['count']}" if item["count"] > 1 else ""
            lines.append(f"- `{t}` **[{record['level']}]** {location} — {summary}{suffix}")

        start_ts = ordered[0]["record"]["ts"] if ordered else target[0]["ts"]
        start_str = datetime.fromtimestamp(start_ts).strftime("%m-%d %H:%M")
        parts = [f"**{start_str} 起** 共 {len(target)} 条（ERROR {error_n} / CRITICAL {critical_n} / WARNING {warning_n}）", ""]
        parts.extend(lines)
        leftover = len(ordered) - max_entries
        if leftover > 0:
            parts.append(f"\n…另有 {leftover} 种不同错误未列出，完整记录见插件目录 errors.log")

        desp = "\n".join(parts)
        max_len = max(200, int(cfg.push.desc_max_len))
        if len(desp) > max_len:
            desp = desp[:max_len] + "\n\n…(正文已截断，完整记录见插件目录 errors.log)"

        title = f"[MaiBot报错] {len(target)}条 E{error_n}/C{critical_n}"
        return title, desp

    async def _send_serverchan(self, cfg: ErrorNotifyConfig, sendkey: str, title: str, desp: str) -> bool:
        url = str(cfg.serverchan.serverchan_api_base or "").strip().rstrip("/") + "/" + sendkey + ".send"
        payload = urllib.parse.urlencode({"title": title, "desp": desp}).encode("utf-8")
        last_error: Exception | None = None
        for attempt, backoff in enumerate((0, 2, 6)):
            if attempt:
                await asyncio.sleep(backoff)
            try:
                request = urllib.request.Request(
                    url,
                    data=payload,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "User-Agent": "MaiBot-ErrorNotify/1.0",
                    },
                )
                response = await asyncio.to_thread(urllib.request.urlopen, request, timeout=10)
                body = response.read(4096).decode("utf-8", "replace")
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}")
                try:
                    data = json.loads(body)
                except ValueError:
                    data = {}
                code = data.get("code", data.get("errno", 0))
                if code != 0:
                    raise RuntimeError(f"Server酱返回 code={code}: {body[:200]}")
                return True
            except Exception as exc:  # noqa: BLE001 - 推送失败需要完整兜底
                last_error = exc
        self.ctx.logger.error("Server酱推送失败（已放弃重试）: %s，完整记录见 %s", last_error, self._plugin_dir / ARCHIVE_FILE)
        return False

    # ------------------------------------------------------------------
    # 本地错误归档
    # ------------------------------------------------------------------

    def _setup_archive(self) -> None:
        logger = logging.getLogger(f"{__name__}.archive")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        handler = logging.handlers.RotatingFileHandler(
            self._plugin_dir / ARCHIVE_FILE,
            maxBytes=ARCHIVE_MAX_BYTES,
            backupCount=ARCHIVE_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        self._archive_logger = logger

    def _write_archive(self, record: dict) -> None:
        if self._archive_logger is None:
            return
        try:
            line = json.dumps(record, ensure_ascii=False)
            self._archive_logger.info(line)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 状态持久化（文件游标 + 当日推送计数）
    # ------------------------------------------------------------------

    def _state_path(self) -> Path:
        return self._plugin_dir / STATE_FILE

    def _load_state(self) -> None:
        path = self._state_path()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        self._state_data = data
        if str(data.get("date") or "") == date.today().isoformat():
            try:
                self._pushed_today = int(data.get("pushed") or 0)
            except (TypeError, ValueError):
                self._pushed_today = 0
        else:
            self._pushed_today = 0
            self._today = date.today()
        # 文件字节游标槽位由 _scan_once 在发现当前日志文件后恢复

    def _save_state(self) -> None:
        path = self._state_path()
        data = {
            "file": str(self._current_file) if self._current_file is not None else "",
            "offset": self._file_offset,
            "date": self._today.isoformat(),
            "pushed": self._pushed_today,
        }
        try:
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            tmp.replace(path)
        except OSError:
            pass

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    def _resolve_logs_dir(self, configured: str) -> Path | None:
        candidates: list[Path] = []
        if configured and str(configured).strip():
            candidates.append(Path(str(configured).strip()))
        candidates.extend(
            [
                Path("/MaiMBot/logs"),
                self._plugin_dir.parent.parent / "logs",
                Path.cwd() / "logs",
            ]
        )
        seen: set[str] = set()
        for candidate in candidates:
            try:
                resolved = candidate.resolve()
            except OSError:
                continue
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            if resolved.is_dir():
                return resolved
        self.ctx.logger.warning(
            "未找到 MaiBot 日志目录（尝试过: %s），请在插件配置中填写 logs_dir", "、".join(str(c) for c in candidates)
        )
        return None

    def _compute_min_ts(self, cfg: ErrorNotifyConfig) -> float:
        now = time.time()
        if cfg.plugin.backfill_on_start:
            minutes = max(1, int(cfg.plugin.backfill_minutes))
            return now - minutes * 60
        return now

    @staticmethod
    def _parse_ts(value: Any) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        if not isinstance(value, str) or not value.strip():
            return 0.0
        text = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            try:
                dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return 0.0
        if dt.tzinfo is not None:
            dt = dt.astimezone()
        return dt.timestamp()

    @staticmethod
    def _seconds_to_next_boundary(minutes: int) -> float:
        now = datetime.now()
        step = max(1, minutes) * 60
        past = now.hour * 3600 + now.minute * 60 + now.second + now.microsecond / 1_000_000
        return (int(past // step) + 1) * step - past


def create_plugin() -> ErrorNotifyPlugin:
    return ErrorNotifyPlugin()
