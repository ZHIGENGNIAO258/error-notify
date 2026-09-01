"""MaiBot 报错日志推送插件。

监控 MaiBot 的 JSONL 日志文件（logs/app_*.log.jsonl）中的 ERROR / CRITICAL
报错，实时写入本插件目录 errors/ 下的按天归档文件，并按配置周期（默认 30 分钟）
通过 Server酱 聚合推送一条摘要到手机。

设计要点：
- MaiBot 插件运行在独立 Runner 子进程中，SDK 没有日志类 Hook / 事件，
  Host 主进程的日志无法在插件进程内直接获取，因此采用"监听日志文件"方案。
- 推送语义为"过期不候"：跨日期的积压错误、超出每日条数上限的错误都不会
  在之后补推，完整内容始终保留在本地 errors/ 归档中。
- 通知阈值：同一错误在一个推送周期内出现次数达到 min_occurrences（默认 3）
  才会进入通知；未达到阈值的错误只记录本地归档。
- 配置模型定义在 config.py（WebUI 配置页元数据见该文件）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from maibot_sdk import CONFIG_RELOAD_SCOPE_SELF, MaiBotPlugin
from .config import ARCHIVE_DIR_NAME, ARCHIVE_KEEP_DAYS, ErrorNotifyConfig

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

APP_LOG_PATTERN = "app_*.log.jsonl"
STATE_FILE = "state.json"

TARGET_LEVELS = {"ERROR", "CRITICAL", "WARNING"}

PUSH_MAX_PENDING = 5000  # 待推送缓冲上限，超出丢弃最旧条目（本地已归档）


class ErrorNotifyPlugin(MaiBotPlugin):
    """监控日志报错并通过 Server酱 聚合推送。"""

    config_model = ErrorNotifyConfig

    def __init__(self) -> None:
        super().__init__()
        self._plugin_dir = Path(__file__).resolve().parent

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
        self._ensure_archive_dir()
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
            "报错日志推送已加载，日志目录=%s，推送周期=%d 分钟，通知阈值=%d，每日上限=%d",
            self._logs_dir or "(未找到，请在配置中填写 logs_dir)",
            int(cfg.push.flush_interval_min),
            int(cfg.push.min_occurrences),
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
        # 命中忽略关键词的错误只归档、不进入推送缓冲（见 _matches_ignore）
        if self._matches_ignore(record):
            return
        self._pending.append(record)
        if len(self._pending) > PUSH_MAX_PENDING:
            # 只丢"待推送"副本，本地归档不受影响
            self._pending = self._pending[-PUSH_MAX_PENDING:]

    def _matches_ignore(self, record: dict) -> bool:
        """关键词命中判断：大小写不敏感，匹配 event/exception/logger_name/module。

        命中意味着"这类报错不影响实际使用"：仍写入 errors/ 归档，但不进入推送。
        """
        keywords = list(getattr(self.config.plugin, "ignore_keywords", None) or [])
        if not keywords:
            return False
        haystack = " ".join(
            str(record.get(field) or "") for field in ("event", "exception", "logger_name", "module")
        ).lower()
        return any(str(kw).strip().lower() in haystack for kw in keywords if str(kw).strip())

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

        message = self._build_push_message(target, cfg)
        if message is None:
            self.ctx.logger.info(
                "推送窗口内 %d 条错误均未达到通知阈值（%d 次），仅记录本地归档",
                len(target),
                int(cfg.push.min_occurrences),
            )
            return

        if self._pushed_today >= int(cfg.push.daily_push_limit):
            self.ctx.logger.warning(
                "已达当日推送上限（%d），本批 %d 条错误不再推送（完整记录见 %s/）",
                int(cfg.push.daily_push_limit),
                len(target),
                self._plugin_dir / ARCHIVE_DIR_NAME,
            )
            return

        sendkey = str(cfg.serverchan.serverchan_sendkey or "").strip()
        if not sendkey:
            if not self._sendkey_warned:
                self._sendkey_warned = True
                self.ctx.logger.warning("未配置 serverchan_sendkey，本批 %d 条错误仅记录本地归档", len(target))
            return

        title, desp = message
        ok = await self._send_serverchan(cfg, sendkey, title, desp)
        if ok:
            self._pushed_today += 1
            self._save_state()
            self.ctx.logger.info(
                "已推送 %d 条错误（今日第 %d/%d 次）", len(target), self._pushed_today, int(cfg.push.daily_push_limit)
            )

    def _build_push_message(
        self, target: list[dict], cfg: ErrorNotifyConfig
    ) -> tuple[str, str] | None:
        """聚合窗口内错误，返回 (title, desp)；若均未达通知阈值则返回 None。"""
        min_occurrences = max(1, int(cfg.push.min_occurrences))

        # 同一错误在窗口内重复出现时合并为一条并计数
        merged: dict[tuple, dict] = {}
        for record in target:
            key = (record["level"], record["logger_name"], record["event"][:80])
            item = merged.get(key)
            if item is None:
                merged[key] = {"record": record, "count": 1}
            else:
                item["count"] += 1
        ordered = sorted(merged.values(), key=lambda x: x["record"]["ts"])

        # 只通知达到阈值（≥ min_occurrences 次）的错误类别
        qualified = [item for item in ordered if item["count"] >= min_occurrences]
        if not qualified:
            return None

        total_events = sum(item["count"] for item in qualified)
        error_n = sum(item["count"] for item in qualified if item["record"]["level"] == "ERROR")
        critical_n = sum(item["count"] for item in qualified if item["record"]["level"] == "CRITICAL")
        warning_n = sum(item["count"] for item in qualified if item["record"]["level"] == "WARNING")

        summary_len = max(20, int(cfg.push.entry_summary_len))
        max_entries = max(1, int(cfg.push.max_entries_per_push))
        lines: list[str] = []
        for item in qualified[:max_entries]:
            record = item["record"]
            t = datetime.fromtimestamp(record["ts"]).strftime("%H:%M:%S")
            summary = record["event"] or (record["exception"].splitlines()[0] if record["exception"] else "-")
            summary = summary.strip()[:summary_len]
            location = record["logger_name"]
            if record.get("module"):
                location = f"{record['module']}:{record.get('lineno') or '?'}"
            suffix = f" ×{item['count']}" if item["count"] > 1 else ""
            lines.append(f"- `{t}` **[{record['level']}]** {location} — {summary}{suffix}")

        start_ts = qualified[0]["record"]["ts"]
        start_str = datetime.fromtimestamp(start_ts).strftime("%m-%d %H:%M")
        parts = [
            f"**{start_str} 起** 达标 {len(qualified)} 类 / {total_events} 条（ERROR {error_n} / CRITICAL {critical_n} / WARNING {warning_n}）",
            "",
        ]
        parts.extend(lines)
        leftover = len(qualified) - max_entries
        if leftover > 0:
            parts.append(f"\n…另有 {leftover} 种不同错误未列出，完整记录见插件目录 {ARCHIVE_DIR_NAME}/")

        desp = "\n".join(parts)
        max_len = max(200, int(cfg.push.desc_max_len))
        if len(desp) > max_len:
            desp = desp[:max_len] + f"\n\n…(正文已截断，完整记录见插件目录 {ARCHIVE_DIR_NAME}/)"

        title = f"[MaiBot报错] {total_events}条 E{error_n}/C{critical_n}"
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
        self.ctx.logger.error("Server酱推送失败（已放弃重试）: %s，完整记录见 %s/", last_error, self._plugin_dir / ARCHIVE_DIR_NAME)
        return False

    # ------------------------------------------------------------------
    # 本地错误归档（errors/ 目录，按天分文件）
    # ------------------------------------------------------------------

    def _ensure_archive_dir(self) -> None:
        try:
            archive_dir = self._plugin_dir / ARCHIVE_DIR_NAME
            archive_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            self.ctx.logger.warning("无法创建错误归档目录 %s", self._plugin_dir / ARCHIVE_DIR_NAME)
            return
        self._cleanup_old_archives()

    def _cleanup_old_archives(self) -> None:
        """清理超过保留天数的归档文件（文件名按日期命名）。"""
        try:
            archive_dir = self._plugin_dir / ARCHIVE_DIR_NAME
            if not archive_dir.is_dir():
                return
            cutoff = date.today() - timedelta(days=ARCHIVE_KEEP_DAYS)
            for path in archive_dir.iterdir():
                if not path.is_file() or not path.name.endswith(".log"):
                    continue
                try:
                    file_date = date.fromisoformat(path.name[: -len(".log")])
                except ValueError:
                    continue
                if file_date < cutoff:
                    path.unlink(missing_ok=True)
        except OSError:
            pass

    def _write_archive(self, record: dict) -> None:
        """将错误记录追加写到当日归档文件：errors/<YYYY-MM-DD>.log。"""
        try:
            archive_dir = self._plugin_dir / ARCHIVE_DIR_NAME
            archive_dir.mkdir(parents=True, exist_ok=True)
            path = archive_dir / f"{record['created_date']}.log"
            line = json.dumps(record, ensure_ascii=False)
            with open(path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
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
        """仅处理插件加载之后产生的日志，避免游标缺失/轮转时重推历史。

        说明：启动回扫（backfill_on_start / backfill_minutes）功能已按需关闭，
        config.py 中相应配置字段已移除。如需恢复，取消下方注释并在
        config.py 的 PluginSectionConfig 中加回 backfill_on_start /
        backfill_minutes 字段。
        """
        # if cfg.plugin.backfill_on_start:
        #     minutes = max(1, int(cfg.plugin.backfill_minutes))
        #     return time.time() - minutes * 60
        del cfg
        return time.time()

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
