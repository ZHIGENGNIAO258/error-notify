"""离线冒烟测试：不依赖 MaiBot 运行环境，mock 掉 maibot_sdk 后
直接加载 plugin.py 验证核心逻辑：

1. 日志扫描（级别过滤、小写 level、坏行容错、时间过滤）
2. 状态游标恢复（重启后不重复处理）
3. errors.log 归档内容
4. 推送聚合、摘要截断、每日上限、跨日期"过期不候"
5. 对齐下一个推送边界的时间计算

用法:  python tests/smoke_test.py
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import shutil
import sys
import tempfile
import time
import types
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# mock maibot_sdk
# ---------------------------------------------------------------------------

_plugin_logger = logging.getLogger("smoke")


class FakeMaiBotPlugin:
    def __init__(self) -> None:
        self.ctx = types.SimpleNamespace(logger=_plugin_logger)
        self._cfg: Any = None

    @property
    def config(self) -> Any:
        if self._cfg is None:
            raise RuntimeError("config not injected")
        return self._cfg

    @config.setter
    def config(self, value: Any) -> None:
        self._cfg = value

    def get_plugin_config_data(self) -> dict:
        return {}


class FakePluginConfigBase:
    pass


def fake_field(
    default: Any = None,
    description: str = "",
    json_schema_extra: dict | None = None,
    default_factory: Any = None,
) -> Any:
    if default_factory is not None:
        return default_factory()
    return default


_sdk = types.ModuleType("maibot_sdk")
_sdk.MaiBotPlugin = FakeMaiBotPlugin
_sdk.PluginConfigBase = FakePluginConfigBase
_sdk.Field = fake_field
_sdk.CONFIG_RELOAD_SCOPE_SELF = "self"
sys.modules["maibot_sdk"] = _sdk

# ---------------------------------------------------------------------------
# 加载被测插件
# ---------------------------------------------------------------------------

_PLUGIN_PATH = Path(__file__).resolve().parent.parent / "plugin.py"
_spec = importlib.util.spec_from_file_location("error_notify_plugin", _PLUGIN_PATH)
_plugin_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_plugin_mod)


def make_cfg(**overrides: Any) -> types.SimpleNamespace:
    plugin: dict[str, Any] = {
        "config_version": "1.0.0",
        "enabled": True,
        "logs_dir": "",
        "scan_interval_sec": 5.0,
        "include_warning": False,
        "backfill_on_start": False,
        "backfill_minutes": 10,
    }
    serverchan: dict[str, Any] = {
        "serverchan_sendkey": "SCTTESTKEY",
        "serverchan_api_base": "https://sctapi.ftqq.com",
    }
    push: dict[str, Any] = {
        "flush_interval_min": 30,
        "daily_push_limit": 3,
        "max_entries_per_push": 50,
        "desc_max_len": 4000,
        "entry_summary_len": 200,
    }
    for key, value in overrides.items():
        if key in plugin:
            plugin[key] = value
        elif key in serverchan:
            serverchan[key] = value
        elif key in push:
            push[key] = value
        else:
            raise KeyError(f"未知配置字段: {key}")
    return types.SimpleNamespace(
        plugin=types.SimpleNamespace(**plugin),
        serverchan=types.SimpleNamespace(**serverchan),
        push=types.SimpleNamespace(**push),
    )


def write_log(log_dir: Path, name: str, lines: list[dict | str]) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / name
    payload = "\n".join(json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x for x in lines)
    path.write_text(payload + "\n", encoding="utf-8")
    return path


def iso_ts(seconds: float) -> str:
    return datetime.fromtimestamp(seconds).astimezone().isoformat(timespec="seconds")


def now_minus(seconds: int) -> str:
    return iso_ts(time.time() - seconds)


def make_plugin(tmp: Path, cfg: types.SimpleNamespace) -> _plugin_mod.ErrorNotifyPlugin:
    plugin = _plugin_mod.ErrorNotifyPlugin()
    plugin._plugin_dir = tmp / "plugin_dir"
    plugin._plugin_dir.mkdir(parents=True, exist_ok=True)
    plugin.config = cfg
    plugin._setup_archive()
    return plugin


def test_scan_and_archive(tmp: Path) -> None:
    logs = tmp / "logs"
    cfg = make_cfg(backfill_on_start=True, backfill_minutes=60, include_warning=False)
    f = write_log(
        logs,
        "app_20260718_000000_main.log.jsonl",
        [
            {"timestamp": now_minus(3600), "level": "info", "logger_name": "chat", "event": "普通信息"},
            {"timestamp": now_minus(300), "level": "error", "logger_name": "maisaka.planner", "module": "planner.py", "lineno": 56, "event": "LLM 请求失败: timeout"},
            {"timestamp": now_minus(200), "level": "CRITICAL", "logger_name": "adapter", "module": "adapter.py", "lineno": 9, "event": "连接断开", "exception": "Traceback (most recent call last):\nValueError: boom"},
            {"timestamp": now_minus(100), "level": "WARNING", "logger_name": "x", "event": "重试"},
            "this is not json {{{",
        ],
    )
    cfg.plugin.logs_dir = str(logs)
    plugin = make_plugin(tmp, cfg)
    plugin._logs_dir = plugin._resolve_logs_dir(cfg.plugin.logs_dir)
    assert plugin._logs_dir is not None and plugin._logs_dir == logs.resolve(), "日志目录解析失败"
    plugin._min_ts = plugin._compute_min_ts(cfg)
    plugin._load_state()
    plugin._scan_once()

    # WARNING 未开启包含时只命中 ERROR/CRITICAL
    assert len(plugin._pending) == 2, f"期望 2 条待推送，实际 {len(plugin._pending)}"
    levels = sorted(r["level"] for r in plugin._pending)
    assert levels == ["CRITICAL", "ERROR"], levels
    # 小写 error 已被归一
    assert plugin._pending[0]["logger_name"] == "maisaka.planner"

    archive_file = plugin._plugin_dir / "errors.log"
    assert archive_file.exists(), "errors.log 未生成"
    archived = [json.loads(x) for x in archive_file.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(archived) == 2
    boom = [r for r in archived if r["level"] == "CRITICAL"][0]
    assert "ValueError: boom" in boom["exception"], "异常堆栈未完整归档"
    assert boom["created_date"] == date.today().isoformat()
    print("[OK] test_scan_and_archive")


def test_time_filter_backfill_off(tmp: Path) -> None:
    logs = tmp / "logs2"
    cfg = make_cfg(backfill_on_start=False)
    write_log(
        logs,
        "app_20260718_000000_main.log.jsonl",
        [
            {"timestamp": now_minus(60), "level": "ERROR", "logger_name": "old", "event": "历史错误"},
        ],
    )
    cfg.plugin.logs_dir = str(logs)
    plugin = make_plugin(tmp, cfg)
    plugin._logs_dir = plugin._resolve_logs_dir(cfg.plugin.logs_dir)
    plugin._min_ts = plugin._compute_min_ts(cfg)
    plugin._load_state()
    plugin._scan_once()
    # 关闭回扫：历史错误不进入推送
    assert plugin._pending == [], f"历史错误不应被推送: {plugin._pending}"
    archive_file = plugin._plugin_dir / "errors.log"
    if archive_file.exists():
        archived = [json.loads(x) for x in archive_file.read_text(encoding="utf-8").splitlines() if x.strip()]
        # 历史错误也跳过归档（与推送同口径）
        assert archived == [], archived
    print("[OK] test_time_filter_backfill_off")


def test_cursor_resume(tmp: Path) -> None:
    """重启后从持久化游标继续，不重复处理已读内容。"""
    logs = tmp / "logs3"
    cfg = make_cfg(backfill_on_start=True, backfill_minutes=60)
    log_file = write_log(
        logs,
        "app_20260718_000000_main.log.jsonl",
        [
            {"timestamp": now_minus(600), "level": "ERROR", "logger_name": "a", "event": "错误A"},
            {"timestamp": now_minus(500), "level": "ERROR", "logger_name": "b", "event": "错误B"},
        ],
    )
    cfg.plugin.logs_dir = str(logs)

    # 第一次实例：处理全部
    plugin = make_plugin(tmp, cfg)
    plugin._logs_dir = plugin._resolve_logs_dir(cfg.plugin.logs_dir)
    plugin._min_ts = plugin._compute_min_ts(cfg)
    plugin._load_state()
    plugin._scan_once()
    assert len(plugin._pending) == 2
    plugin._save_state()
    offset_after_first = plugin._file_offset
    assert offset_after_first > 0

    # 追加新错误
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps({"timestamp": now_minus(10), "level": "ERROR", "logger_name": "c", "event": "错误C"}) + "\n")

    # 第二次实例（模拟重启）：恢复游标，只处理新增
    plugin2 = make_plugin(tmp, cfg)
    plugin2._logs_dir = plugin2._resolve_logs_dir(cfg.plugin.logs_dir)
    plugin2._min_ts = plugin2._compute_min_ts(cfg)
    plugin2._load_state()
    plugin2._scan_once()
    events = [r["event"] for r in plugin2._pending]
    assert events == ["错误C"], f"游标恢复失败: {events}"
    print("[OK] test_cursor_resume")


def test_rotation_drain(tmp: Path) -> None:
    """轮转瞬间旧文件新写入的尾行也应被补读。"""
    logs = tmp / "logs4"
    cfg = make_cfg(backfill_on_start=True, backfill_minutes=60)
    old_file = write_log(
        logs,
        "app_20260718_000000_main.log.jsonl",
        [
            {"timestamp": now_minus(600), "level": "ERROR", "logger_name": "a", "event": "错误A"},
        ],
    )
    cfg.plugin.logs_dir = str(logs)
    plugin = make_plugin(tmp, cfg)
    plugin._logs_dir = plugin._resolve_logs_dir(cfg.plugin.logs_dir)
    plugin._min_ts = plugin._compute_min_ts(cfg)
    plugin._load_state()
    plugin._scan_once()
    assert len(plugin._pending) == 1

    # 模拟轮转：旧文件切换前最后写入一行，随后新日志文件出现（mtime 更新）
    with open(old_file, "a", encoding="utf-8") as f:
        f.write(json.dumps({"timestamp": now_minus(100), "level": "CRITICAL", "logger_name": "c", "event": "错误C尾行"}) + "\n")
    time.sleep(0.02)  # 保证新文件 mtime 更新
    write_log(
        logs,
        "app_20260718_003000_main.log.jsonl",
        [
            {"timestamp": now_minus(200), "level": "ERROR", "logger_name": "b", "event": "错误B"},
        ],
    )

    plugin._scan_once()
    events = [r["event"] for r in plugin._pending]
    assert events == ["错误A", "错误C尾行", "错误B"], f"轮转补读失败: {events}"
    assert plugin._current_file is not None and plugin._current_file.name.startswith("app_20260718_003000")
    print("[OK] test_rotation_drain")


def test_flush_build_and_limits(tmp: Path) -> None:
    cfg = make_cfg(flush_interval_min=30, daily_push_limit=1, desc_max_len=300, entry_summary_len=50)
    now = time.time()
    plugin = make_plugin(tmp, cfg)
    calls: list[tuple] = []

    async def fake_send(cfg_, sendkey, title, desp):
        calls.append((sendkey, title, desp))
        return True

    plugin._send_serverchan = fake_send  # type: ignore[method-assign]
    plugin._logs_dir = tmp / "logs"

    today = date.today().isoformat()
    plugin._pending = [
        {"ts": now - 100, "timestamp": iso_ts(now - 100), "level": "ERROR", "logger_name": "m1", "module": "m1.py", "lineno": 1, "event": "错误一" + "x" * 200, "exception": "", "created_date": today},
        {"ts": now - 90, "timestamp": iso_ts(now - 90), "level": "ERROR", "logger_name": "m1", "module": "m1.py", "lineno": 1, "event": "错误一" + "x" * 200, "exception": "", "created_date": today},
        {"ts": now - 80, "timestamp": iso_ts(now - 80), "level": "CRITICAL", "logger_name": "m2", "module": "m2.py", "lineno": 2, "event": "错误二", "exception": "Traceback", "created_date": today},
        {"ts": now - 70, "timestamp": iso_ts(now - 70), "level": "ERROR", "logger_name": "yesterday", "module": "y.py", "lineno": 3, "event": "昨天的错误", "exception": "", "created_date": (date.today() - timedelta(days=1)).isoformat()},
    ]
    asyncio.run(plugin._flush_once())

    assert len(calls) == 1, f"应只推送 1 次（昨日条目丢弃）: {len(calls)}"
    sendkey, title, desp = calls[0]
    assert sendkey == "SCTTESTKEY"
    # 聚合：相同的"错误一"合并 ×2；昨日错误不出现
    assert "×2" in desp, desp
    assert "昨天的错误" not in desp, desp
    assert "共 3 条（ERROR 2 / CRITICAL 1 / WARNING 0）" in desp, desp
    assert len(desp) <= 300 + 100, len(desp)  # 截断生效
    assert plugin._pushed_today == 1

    # 第二次 flush：已达每日上限 1 → 丢弃不推（本地归档语义）
    plugin._pending = [{"ts": now - 50, "timestamp": iso_ts(now - 50), "level": "ERROR", "logger_name": "m3", "module": "m3.py", "lineno": 4, "event": "错误三", "exception": "", "created_date": today}]
    asyncio.run(plugin._flush_once())
    assert len(calls) == 1, "超限后不应再推送"
    print("[OK] test_flush_build_and_limits")


def test_sendkey_empty(tmp: Path) -> None:
    cfg = make_cfg(serverchan_sendkey="")
    plugin = make_plugin(tmp, cfg)
    sent: list[tuple] = []

    async def fake_send(cfg_, sendkey, title, desp):
        sent.append((sendkey, title, desp))
        return True

    plugin._send_serverchan = fake_send  # type: ignore[method-assign]
    plugin._logs_dir = tmp / "logs"
    today = date.today().isoformat()
    plugin._pending = [{"ts": time.time(), "timestamp": iso_ts(time.time()), "level": "ERROR", "logger_name": "m", "module": "m.py", "lineno": 1, "event": "e", "exception": "", "created_date": today}]
    asyncio.run(plugin._flush_once())
    assert sent == [], "SendKey 为空时不应发起推送"
    print("[OK] test_sendkey_empty")


def test_boundary() -> None:
    # 14:15 → 距 14:30 边界 900s；14:30:00 整 → 距 15:00 边界 1800s
    now = datetime.now()
    step_sec = _plugin_mod.ErrorNotifyPlugin._seconds_to_next_boundary(30)
    now_sec = now.hour * 3600 + now.minute * 60 + now.second + now.microsecond / 1e6
    rem = now_sec % 1800
    expect = 1800 - rem
    assert abs(step_sec - expect) < 1e-6, (step_sec, expect)
    # 周期改为 60 分钟 → 对齐整点
    step_60 = _plugin_mod.ErrorNotifyPlugin._seconds_to_next_boundary(60)
    expect_60 = 3600 - (now_sec % 3600)
    assert abs(step_60 - expect_60) < 1e-6, (step_60, expect_60)
    print("[OK] test_boundary")


def test_parse_ts() -> None:
    p = _plugin_mod.ErrorNotifyPlugin._parse_ts
    assert p("2026-07-18T14:30:05+00:00") > 0
    assert p("2026-07-18 14:30:05") > 0
    assert p("not-a-date") == 0.0
    assert p(123456.0) == 123456.0
    assert p(None) == 0.0
    print("[OK] test_parse_ts")


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]
    except AttributeError:
        pass
    tmp = Path(tempfile.mkdtemp(prefix="error-notify-test-"))
    try:
        test_scan_and_archive(tmp / "case_scan")
        test_time_filter_backfill_off(tmp / "case_filter")
        test_cursor_resume(tmp / "case_cursor")
        test_rotation_drain(tmp / "case_rotation")
        test_flush_build_and_limits(tmp / "case_flush")
        test_sendkey_empty(tmp / "case_sendkey")
        test_boundary()
        test_parse_ts()
        print("\n全部冒烟测试通过 ✓")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
