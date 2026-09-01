"""离线冒烟测试：不依赖 MaiBot 运行环境，mock 掉 maibot_sdk 后
以包方式加载 plugin.py（含相对导入 .config）验证核心逻辑：

1. 日志扫描（级别过滤、小写 level、坏行容错、仅处理加载后的新错误）
2. 状态游标恢复（重启后不重复处理）
3. errors/ 按天归档内容与旧文件清理
4. 推送聚合、摘要截断、每日上限、跨日期"过期不候"
5. 通知阈值：同一错误达到 min_occurrences 才推送
6. 对齐下一个推送边界的时间计算
7. WARN 视同 ERROR 白名单：命中关键词的 WARNING 按 ERROR 推送，未命中维持原规则
8. serverchan_api_base 校验（仅 https、拒绝内网/回环，非官方域名告警）

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
    ge: Any = None,
    le: Any = None,
    **_: Any,
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
# 以包方式加载被测插件（支持 plugin.py 中的相对导入 from .config import ...）
# ---------------------------------------------------------------------------

_BASE_DIR = Path(__file__).resolve().parent.parent
_PLUGIN_PATH = _BASE_DIR / "plugin.py"
_pkg = types.ModuleType("error_notify_pkg")
_pkg.__path__ = [str(_BASE_DIR)]
sys.modules["error_notify_pkg"] = _pkg

_spec = importlib.util.spec_from_file_location("error_notify_pkg.plugin", _PLUGIN_PATH)
_plugin_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_plugin_mod)

CONFIG_MOD = sys.modules["error_notify_pkg.config"]
ARCHIVE_DIR_NAME = CONFIG_MOD.ARCHIVE_DIR_NAME


def make_cfg(**overrides: Any) -> types.SimpleNamespace:
    plugin: dict[str, Any] = {
        "config_version": "1.3.0",
        "enabled": True,
        "logs_dir": "",
        "scan_interval_sec": 5.0,
        "include_warning": False,
        "ignore_keywords": [],
        "warning_as_error_keywords": [],
    }
    serverchan: dict[str, Any] = {
        "serverchan_sendkey": "SCTTESTKEY",
        "serverchan_api_base": "https://sctapi.ftqq.com",
    }
    push: dict[str, Any] = {
        "flush_interval_min": 30,
        "daily_push_limit": 3,
        "min_occurrences": 3,
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


def make_plugin(tmp: Path, cfg: types.SimpleNamespace) -> Any:
    plugin = _plugin_mod.ErrorNotifyPlugin()
    plugin._plugin_dir = tmp / "plugin_dir"
    plugin._plugin_dir.mkdir(parents=True, exist_ok=True)
    plugin.config = cfg
    return plugin


def archive_path(plugin: Any) -> Path:
    return plugin._plugin_dir / ARCHIVE_DIR_NAME / f"{date.today().isoformat()}.log"


def test_scan_and_archive(tmp: Path) -> None:
    logs = tmp / "logs"
    cfg = make_cfg()
    write_log(
        logs,
        "app_20260718_000000_main.log.jsonl",
        [
            {"timestamp": now_minus(300), "level": "info", "logger_name": "chat", "event": "普通信息"},
            {"timestamp": now_minus(200), "level": "error", "logger_name": "maisaka.planner", "module": "planner.py", "lineno": 56, "event": "LLM 请求失败: timeout"},
            {"timestamp": now_minus(100), "level": "CRITICAL", "logger_name": "adapter", "module": "adapter.py", "lineno": 9, "event": "连接断开", "exception": "Traceback (most recent call last):\nValueError: boom"},
            {"timestamp": now_minus(90), "level": "WARNING", "logger_name": "x", "event": "重试"},
            "this is not json {{{",
        ],
    )
    cfg.plugin.logs_dir = str(logs)
    plugin = make_plugin(tmp, cfg)
    plugin._logs_dir = plugin._resolve_logs_dir(cfg.plugin.logs_dir)
    assert plugin._logs_dir is not None and plugin._logs_dir == logs.resolve(), "日志目录解析失败"
    plugin._min_ts = 0.0  # 测试所有时间戳都放行；时间过滤由专门用例覆盖
    plugin._load_state()
    plugin._scan_once()

    # WARNING 未开启包含时只命中 ERROR/CRITICAL
    assert len(plugin._pending) == 2, f"期望 2 条待推送，实际 {len(plugin._pending)}"
    levels = sorted(r["level"] for r in plugin._pending)
    assert levels == ["CRITICAL", "ERROR"], levels
    # 小写 error 已被归一
    assert plugin._pending[0]["logger_name"] == "maisaka.planner"

    archived_file = archive_path(plugin)
    assert archived_file.exists(), f"{archived_file.name} 未生成"
    archived = [json.loads(x) for x in archived_file.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(archived) == 2
    boom = [r for r in archived if r["level"] == "CRITICAL"][0]
    assert "ValueError: boom" in boom["exception"], "异常堆栈未完整归档"
    assert boom["created_date"] == date.today().isoformat()
    print("[OK] test_scan_and_archive")


def test_only_process_new_errors(tmp: Path) -> None:
    """插件只处理加载之后产生的错误（启动前的历史错误不处理、不归档）。"""
    logs = tmp / "logs2"
    cfg = make_cfg()
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
    plugin._min_ts = plugin._compute_min_ts(cfg)  # 固定为当前时刻
    plugin._load_state()
    plugin._scan_once()
    assert plugin._pending == [], f"历史错误不应被推送: {plugin._pending}"
    archived_file = archive_path(plugin)
    if archived_file.exists():
        archived = [json.loads(x) for x in archived_file.read_text(encoding="utf-8").splitlines() if x.strip()]
        assert archived == [], archived
    print("[OK] test_only_process_new_errors")


def test_cursor_resume(tmp: Path) -> None:
    """重启后从持久化游标继续，不重复处理已读内容。"""
    logs = tmp / "logs3"
    cfg = make_cfg()
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
    plugin._min_ts = 0.0
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
    plugin2._min_ts = 0.0
    plugin2._load_state()
    plugin2._scan_once()
    events = [r["event"] for r in plugin2._pending]
    assert events == ["错误C"], f"游标恢复失败: {events}"
    print("[OK] test_cursor_resume")


def test_rotation_drain(tmp: Path) -> None:
    """轮转瞬间旧文件新写入的尾行也应被补读。"""
    logs = tmp / "logs4"
    cfg = make_cfg()
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
    plugin._min_ts = 0.0
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


def test_min_occurrences(tmp: Path) -> None:
    """同一错误达到通知阈值才推送；未达到的仅归档；阈值可配置。"""
    cfg = make_cfg(min_occurrences=3, daily_push_limit=10)
    plugin = make_plugin(tmp, cfg)
    calls: list[tuple] = []

    async def fake_send(cfg_, sendkey, title, desp):
        calls.append((sendkey, title, desp))
        return True

    plugin._send_serverchan = fake_send  # type: ignore[method-assign]
    plugin._logs_dir = tmp / "logs"

    now = time.time()
    today = date.today().isoformat()

    def rec(event: str, logger: str = "m", level: str = "ERROR", ts: float | None = None, day: str = today) -> dict:
        t = ts if ts is not None else now - 100
        return {
            "ts": t, "timestamp": iso_ts(t), "level": level, "logger_name": logger,
            "module": "m.py", "lineno": 1, "event": event, "exception": "", "created_date": day,
        }

    # 错误A×2（未达阈值 3）、错误B×3（达到）、昨日错误×1（过期不候）
    plugin._pending = [
        rec("错误A"), rec("错误A"),
        rec("错误B"), rec("错误B"), rec("错误B"),
        rec("昨天的错误", day=(date.today() - timedelta(days=1)).isoformat()),
    ]
    asyncio.run(plugin._flush_once())
    assert len(calls) == 1, f"应只推送 1 次: {len(calls)}"
    sendkey, title, desp = calls[0]
    assert sendkey == "SCTTESTKEY"
    assert title == "[MaiBot报错] 3条 E3/C0", title
    assert "错误B" in desp and "×3" in desp, desp
    assert "错误A" not in desp, f"未达阈值的错误不应出现: {desp}"
    assert "昨天的错误" not in desp, desp
    assert "达标 1 类 / 3 条" in desp, desp
    assert plugin._pushed_today == 1

    # 新窗口：只有错误A×2，仍未达阈值 → 不推送、不消耗每日额度
    plugin._pending = [rec("错误A"), rec("错误A")]
    asyncio.run(plugin._flush_once())
    assert len(calls) == 1, "未达阈值不应推送"
    assert plugin._pushed_today == 1, "未达阈值不应消耗每日额度"

    # min_occurrences=1：全部通知（兼容旧行为）
    cfg.push.min_occurrences = 1
    plugin._pending = [rec("错误A")]
    asyncio.run(plugin._flush_once())
    assert len(calls) == 2, "阈值=1 时应推送"
    assert "错误A" in calls[-1][2], calls[-1][2]
    print("[OK] test_min_occurrences")


def test_ignore_keywords(tmp: Path) -> None:
    """命中忽略关键词的错误仅归档、不推送；大小写不敏感；可匹配正文/堆栈/模块名。"""
    logs = tmp / "logs_ignore"
    cfg = make_cfg(ignore_keywords=["WEBUI.WEBSOCKET", "重复插件 ID", "napcat-adapter"])
    write_log(
        logs,
        "app_20260718_000000_main.log.jsonl",
        [
            # 命中 logger_name（大写关键词，验证大小写不敏感）
            {"timestamp": now_minus(100), "level": "ERROR", "logger_name": "webui.websocket", "module": "src.webui.routers.websocket.manager", "lineno": 80, "event": "统一 WebSocket 发送失败: connection=abc, error="},
            # 命中 event 正文
            {"timestamp": now_minus(90), "level": "ERROR", "logger_name": "plugin_runtime.integration", "module": "src.plugin_runtime.integration", "lineno": 395, "event": "检测到重复插件 ID，冲突插件将被隔离，其余插件继续加载: maibot-team.napcat-adapter: 插件 ID 重复"},
            # 命中 module（src.plugin_runtime 在 module 中仅对第二条……这里验证 module 字段单独命中）
            {"timestamp": now_minus(80), "level": "ERROR", "logger_name": "plugin_runtime.group.core", "module": "src.plugin_runtime.host.supervisor", "lineno": 1762, "event": "插件注册失败: maibot-team.napcat-adapter"},
            # 不命中任何关键词
            {"timestamp": now_minus(70), "level": "ERROR", "logger_name": "maisaka.planner", "module": "planner.py", "lineno": 56, "event": "LLM 请求失败: timeout", "exception": "Traceback\nopenai.Timeout"},
        ],
    )
    cfg.plugin.logs_dir = str(logs)
    plugin = make_plugin(tmp, cfg)
    plugin._logs_dir = plugin._resolve_logs_dir(cfg.plugin.logs_dir)
    plugin._min_ts = 0.0
    plugin._load_state()
    plugin._scan_once()

    # 只推送未命中的 1 条
    events = [r["event"] for r in plugin._pending]
    assert events == ["LLM 请求失败: timeout"], f"忽略过滤失败: {events}"

    # 归档仍保留全部 4 条（仅不推送、仍归档）
    archived_file = archive_path(plugin)
    assert archived_file.exists()
    archived = [json.loads(x) for x in archived_file.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(archived) == 4, f"归档不应被过滤: {len(archived)}"
    # 无关键词时全部进入推送缓冲：由 test_scan_and_archive 覆盖（默认 ignore_keywords=[]）
    print("[OK] test_ignore_keywords")


def test_warning_promote(tmp: Path) -> None:
    """WARN 视同 ERROR 白名单：命中的 WARNING 按 ERROR 处理并推送，未命中维持原规则。"""
    logs = tmp / "logs_promote"
    cfg = make_cfg(
        include_warning=False,
        warning_as_error_keywords=["napcat"],
        min_occurrences=1,
        daily_push_limit=10,
    )
    write_log(
        logs,
        "app_20260718_000000_main.log.jsonl",
        [
            # 命中白名单：NapCat 断连（WARN）→ 提升为 ERROR
            {
                "timestamp": now_minus(100),
                "level": "WARNING",
                "logger_name": "plugin.maibot-team.napcat-adapter",
                "module": "src.adapters.napcat",
                "lineno": 1,
                "event": "NapCat 适配器连接已断开: ws://172.22.0.2:3001，未收到更多 WebSocket 消息，连接已结束；将在 5 秒后重连",
            },
            # 未命中：普通 WARN → 不推送（include_warning=false）
            {"timestamp": now_minus(90), "level": "WARNING", "logger_name": "x", "event": "普通重试告警"},
            # ERROR 正常
            {"timestamp": now_minus(80), "level": "ERROR", "logger_name": "m", "module": "m.py", "lineno": 1, "event": "报错E"},
        ],
    )
    cfg.plugin.logs_dir = str(logs)
    plugin = make_plugin(tmp / "promote_a", cfg)
    plugin._logs_dir = plugin._resolve_logs_dir(cfg.plugin.logs_dir)
    plugin._min_ts = 0.0
    plugin._load_state()
    plugin._scan_once()

    # pending：napcat WARN 已提升为 ERROR + 普通 ERROR，共 2 条；普通 WARN 不进
    assert len(plugin._pending) == 2, [r["event"] for r in plugin._pending]
    napcat = [r for r in plugin._pending if "napcat" in r["logger_name"]][0]
    assert napcat["level"] == "ERROR", napcat
    assert napcat["promoted_from"] == "WARNING", napcat
    assert all(r["event"] != "普通重试告警" for r in plugin._pending)

    # 归档同样记录提升后的级别与原始级别
    archived_file = archive_path(plugin)
    archived = [json.loads(x) for x in archived_file.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(archived) == 2, archived
    arch_napcat = [r for r in archived if "napcat" in r["logger_name"]][0]
    assert arch_napcat["level"] == "ERROR" and arch_napcat["promoted_from"] == "WARNING", arch_napcat

    # 推送：提升项计入 ERROR 统计
    calls: list[tuple] = []

    async def fake_send(cfg_, sendkey, title, desp):
        calls.append((sendkey, title, desp))
        return True

    plugin._send_serverchan = fake_send  # type: ignore[method-assign]
    asyncio.run(plugin._flush_once())
    assert len(calls) == 1, calls
    _, title, desp = calls[0]
    assert title == "[MaiBot报错] 2条 E2/C0", title
    assert "[ERROR]" in desp and "NapCat" in desp, desp

    # include_warning=true：命中仍提升为 ERROR，未命中保持 WARNING 并推送
    cfg2 = make_cfg(include_warning=True, warning_as_error_keywords=["napcat"], min_occurrences=1)
    logs2 = tmp / "logs_promote2"
    write_log(
        logs2,
        "app_20260718_000000_main.log.jsonl",
        [
            {"timestamp": now_minus(100), "level": "WARNING", "logger_name": "plugin.maibot-team.napcat-adapter", "event": "NapCat 适配器连接已断开"},
            {"timestamp": now_minus(90), "level": "WARNING", "logger_name": "x", "event": "普通重试告警"},
        ],
    )
    cfg2.plugin.logs_dir = str(logs2)
    plugin2 = make_plugin(tmp / "promote_b", cfg2)
    plugin2._logs_dir = plugin2._resolve_logs_dir(cfg2.plugin.logs_dir)
    plugin2._min_ts = 0.0
    plugin2._load_state()
    plugin2._scan_once()
    assert len(plugin2._pending) == 2, [r["event"] for r in plugin2._pending]
    by_event = {r["event"]: r for r in plugin2._pending}
    assert by_event["NapCat 适配器连接已断开"]["level"] == "ERROR"
    assert by_event["普通重试告警"]["level"] == "WARNING"
    assert "promoted_from" not in by_event["普通重试告警"]

    # ignore_keywords 优先：命中白名单但同时也命中忽略关键词 → 归档不推送
    cfg3 = make_cfg(include_warning=False, warning_as_error_keywords=["napcat"], ignore_keywords=["napcat"])
    logs3 = tmp / "logs_promote3"
    write_log(
        logs3,
        "app_20260718_000000_main.log.jsonl",
        [
            {"timestamp": now_minus(100), "level": "WARNING", "logger_name": "plugin.maibot-team.napcat-adapter", "event": "NapCat 适配器连接已断开"},
        ],
    )
    cfg3.plugin.logs_dir = str(logs3)
    plugin3 = make_plugin(tmp / "promote_c", cfg3)
    plugin3._logs_dir = plugin3._resolve_logs_dir(cfg3.plugin.logs_dir)
    plugin3._min_ts = 0.0
    plugin3._load_state()
    plugin3._scan_once()
    assert plugin3._pending == [], plugin3._pending
    archived_files = archive_path(plugin3)
    archived3 = [json.loads(x) for x in archived_files.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(archived3) == 1 and archived3[0]["promoted_from"] == "WARNING", archived3
    print("[OK] test_warning_promote")


def test_flush_build_and_limits(tmp: Path) -> None:
    cfg = make_cfg(min_occurrences=1, daily_push_limit=1, desc_max_len=300, entry_summary_len=50)
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
    assert "达标 2 类 / 3 条（ERROR 2 / CRITICAL 1 / WARNING 0）" in desp, desp
    assert len(desp) <= 300 + 100, len(desp)  # 截断生效
    assert plugin._pushed_today == 1

    # 第二次 flush：已达每日上限 1 → 丢弃不推（本地归档语义）
    plugin._pending = [{"ts": now - 50, "timestamp": iso_ts(now - 50), "level": "ERROR", "logger_name": "m3", "module": "m3.py", "lineno": 4, "event": "错误三", "exception": "", "created_date": today}]
    asyncio.run(plugin._flush_once())
    assert len(calls) == 1, "超限后不应再推送"
    print("[OK] test_flush_build_and_limits")


def test_sendkey_empty(tmp: Path) -> None:
    cfg = make_cfg(min_occurrences=1, serverchan_sendkey="")
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


def test_api_base_validation() -> None:
    """接口地址校验：仅 https；拒绝回环/内网/保留地址；非官方域名通过但告警。"""
    v = _plugin_mod.ErrorNotifyPlugin._validate_api_base

    # 官方域名：通过且无需告警
    for official in (
        "https://sctapi.ftqq.com",
        "https://sct.ftqq.com",
        "https://sc3.ft07.com",
        "https://abc.push.ft07.com",
        "https://sctapi.ftqq.com/",
    ):
        assert v(official) == (True, ""), v(official)

    # 非官方域名：通过但带告警文案
    ok, msg = v("https://push.example.com")
    assert ok and "官方域名" in msg and "请确认服务可信" in msg, (ok, msg)
    ok, msg = v("https://not-ftqq.com")  # 精确后缀匹配，不误判 xftqq.com
    assert ok and "官方域名" in msg, (ok, msg)

    # 拒绝：非 https / 空值 / 缺主机 / 回环 / 内网 / 链路本地 / 保留地址
    for bad in (
        "http://sctapi.ftqq.com",
        "sctapi.ftqq.com",  # 缺 scheme
        "",
        "https://",
        "https://localhost",
        "https://foo.localhost",
        "https://nas.local",  # mDNS 内网名
        "https://127.0.0.1",
        "https://[::1]",
        "https://0.0.0.0",
        "https://10.0.0.5",
        "https://192.168.1.1",
        "https://172.16.0.9",
        "https://169.254.169.254",  # 云厂商元数据服务地址
        "https://100.64.0.1",  # CGNAT
    ):
        ok, msg = v(bad)
        assert not ok, f"{bad!r} 应被拒绝: {msg}"
    print("[OK] test_api_base_validation")


def test_api_base_block_flush(tmp: Path) -> None:
    """接口地址非法时，_flush_once 不发起推送、不消耗每日额度。

    使用真实 _send_serverchan：校验失败时在发起网络请求前返回 False，
    不会把 SendKey / 错误内容发到任何地址。
    """
    cfg = make_cfg(min_occurrences=1, serverchan_api_base="https://169.254.169.254")
    plugin = make_plugin(tmp, cfg)
    plugin._logs_dir = tmp / "logs"
    today = date.today().isoformat()
    plugin._pending = [
        {
            "ts": time.time(),
            "timestamp": iso_ts(time.time()),
            "level": "ERROR",
            "logger_name": "m",
            "module": "m.py",
            "lineno": 1,
            "event": "e",
            "exception": "",
            "created_date": today,
        }
    ]
    asyncio.run(plugin._flush_once())
    assert plugin._pushed_today == 0, "非法接口地址不应推送、不应消耗每日额度"
    print("[OK] test_api_base_block_flush")


def test_load_warns_api_base(tmp: Path) -> None:
    """加载时：非法地址报 ERROR；非官方域名报 WARNING；官方域名不告警。"""
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append  # type: ignore[method-assign]
    _plugin_logger.addHandler(handler)
    _plugin_logger.setLevel(logging.INFO)
    try:
        cfg = make_cfg(serverchan_api_base="https://push.example.com")
        plugin = make_plugin(tmp, cfg)
        plugin._log_api_base_warning(cfg)
        warns = [r.getMessage() for r in records if r.levelno >= logging.WARNING]
        assert any("非官方" in m for m in warns), warns

        records.clear()
        cfg = make_cfg(serverchan_api_base="http://192.168.1.1")
        plugin = make_plugin(tmp, cfg)
        plugin._log_api_base_warning(cfg)
        errs = [r.getMessage() for r in records if r.levelno >= logging.ERROR]
        assert any("配置无效" in m for m in errs), errs

        records.clear()
        cfg = make_cfg()
        plugin = make_plugin(tmp, cfg)
        plugin._log_api_base_warning(cfg)
        assert records == [], f"官方域名不应产生任何日志: {[r.getMessage() for r in records]}"
    finally:
        _plugin_logger.removeHandler(handler)
    print("[OK] test_load_warns_api_base")


def test_archive_cleanup(tmp: Path) -> None:
    """启动清理：只保留最近 ARCHIVE_KEEP_DAYS 天的归档文件。"""
    cfg = make_cfg()
    plugin = make_plugin(tmp, cfg)
    archive_dir = plugin._plugin_dir / ARCHIVE_DIR_NAME
    archive_dir.mkdir(parents=True, exist_ok=True)
    old_day = (date.today() - timedelta(days=30)).isoformat()
    keep_day = date.today().isoformat()
    (archive_dir / f"{old_day}.log").write_text("x\n", encoding="utf-8")
    (archive_dir / f"{keep_day}.log").write_text("x\n", encoding="utf-8")
    (archive_dir / "not-a-date.log").write_text("x\n", encoding="utf-8")
    plugin._cleanup_old_archives()
    remaining = sorted(p.name for p in archive_dir.iterdir())
    assert remaining == [f"{keep_day}.log", "not-a-date.log"], remaining
    print("[OK] test_archive_cleanup")


def test_boundary() -> None:
    # 周期对齐计算：距下一个周期整数倍边界的秒数
    now = datetime.now()
    step_sec = _plugin_mod.ErrorNotifyPlugin._seconds_to_next_boundary(30)
    now_sec = now.hour * 3600 + now.minute * 60 + now.second + now.microsecond / 1e6
    rem = now_sec % 1800
    expect = 1800 - rem
    assert abs(step_sec - expect) < 1e-6, (step_sec, expect)
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
        test_only_process_new_errors(tmp / "case_new")
        test_cursor_resume(tmp / "case_cursor")
        test_rotation_drain(tmp / "case_rotation")
        test_min_occurrences(tmp / "case_threshold")
        test_ignore_keywords(tmp / "case_ignore")
        test_warning_promote(tmp / "case_promote")
        test_flush_build_and_limits(tmp / "case_flush")
        test_sendkey_empty(tmp / "case_sendkey")
        test_api_base_validation()
        test_api_base_block_flush(tmp / "case_base_block")
        test_load_warns_api_base(tmp / "case_base_warn")
        test_archive_cleanup(tmp / "case_cleanup")
        test_boundary()
        test_parse_ts()
        print("\n全部冒烟测试通过 ✓")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
