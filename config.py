"""报错日志推送插件配置模型。

配置结构（config.toml 生成时的节名与字段一一对应）：
  [plugin]        插件总开关、日志目录与扫描节奏
  [serverchan]    Server酱 接口配置
  [push]          推送策略（聚合周期、每日上限、通知阈值等）

WebUI 展示元数据说明（见官方《配置管理》文档与 SDK config.py 的
``_build_field_schema``）：
  - 节级：``__ui_label__`` / ``__ui_icon__`` / ``__ui_order__``
  - 字段级：``json_schema_extra`` 支持 label / hint / placeholder / group /
    order / disabled / hidden / input_type / step 等键，数值范围由
    ``Field(ge=..., le=...)`` 声明（WebUI 会渲染 min/max 并做输入校验）。
"""

from __future__ import annotations

from typing import ClassVar

from maibot_sdk import Field, PluginConfigBase

# ── 非 WebUI 常量（如需调整直接改源码，无需改 config.toml）──────────────
ARCHIVE_KEEP_DAYS = 7
"""错误归档文件保留天数，超出自动清理。"""

ARCHIVE_DIR_NAME = "errors"
"""错误归档目录名（插件目录下，按天分文件）。"""


class PluginSectionConfig(PluginConfigBase):
    """插件总开关、日志目录与扫描节奏。"""

    __ui_label__: ClassVar[str] = "基础设置"
    __ui_icon__: ClassVar[str] = "settings"
    __ui_order__: ClassVar[int] = 0

    config_version: str = Field(
        default="1.2.0",
        description="插件配置结构版本号。",
        json_schema_extra={
            "label": "配置版本",
            "hint": "MaiBot 加载策略要求 [plugin] 段必须包含 config_version；修改配置结构时同步递增。",
            "disabled": True,
            "hidden": True,
            "order": 99,
        },
    )
    enabled: bool = Field(
        default=True,
        description="是否启用报错推送。",
        json_schema_extra={
            "label": "启用插件",
            "hint": "关闭后仅停止监控与推送，已归档的错误记录不受影响。",
            "order": 0,
            "group": "basic",
        },
    )
    logs_dir: str = Field(
        default="/MaiMBot/logs",
        description="MaiBot 日志目录（容器内路径）。",
        json_schema_extra={
            "label": "日志目录",
            "hint": "Docker 部署通常为 /MaiMBot/logs；自动探测顺序：本值→/MaiMBot/logs→插件上级 logs/→工作目录 logs/。",
            "placeholder": "/MaiMBot/logs",
            "order": 1,
            "group": "basic",
        },
    )
    scan_interval_sec: float = Field(
        default=5.0,
        ge=1.0,
        le=60.0,
        description="日志扫描间隔（秒）。",
        json_schema_extra={
            "label": "扫描间隔（秒）",
            "hint": "1~60 秒。扫描采用 stat 短路，文件无变化时零 IO；推送是周期性的，无需调得太小。",
            "order": 2,
            "group": "basic",
            "step": 1,
        },
    )
    include_warning: bool = Field(
        default=False,
        description="是否同时推送 WARNING 级别日志。",
        json_schema_extra={
            "label": "包含 WARNING",
            "hint": "默认只推送 ERROR/CRITICAL；开启后 WARNING 也进入统计与推送。",
            "order": 3,
            "group": "basic",
        },
    )
    ignore_keywords: list[str] = Field(
        default_factory=list,
        description="忽略关键词列表：命中（大小写不敏感，匹配报错正文/堆栈/模块名）的错误不推送，但仍归档。",
        json_schema_extra={
            "label": "忽略关键词",
            "hint": "每条一个关键词（如 webui.websocket、重复插件 ID）。命中即不推送，errors/ 归档保留。",
            "order": 4,
            "group": "basic",
            "min_items": 0,
            "max_items": 100,
        },
    )


class ServerChanSectionConfig(PluginConfigBase):
    """Server酱 接口配置。"""

    __ui_label__: ClassVar[str] = "Server酱"
    __ui_icon__: ClassVar[str] = "send"
    __ui_order__: ClassVar[int] = 1

    serverchan_sendkey: str = Field(
        default="",
        description="Server酱 SendKey（SCT 开头）。",
        json_schema_extra={
            "label": "SendKey",
            "hint": "在 https://sct.ftqq.com 登录后获取；留空则只记录本地错误归档、不推送。",
            "placeholder": "SCT...",
            "input_type": "password",
            "order": 0,
        },
    )
    serverchan_api_base: str = Field(
        default="https://sctapi.ftqq.com",
        description="Server酱推送接口基址。",
        json_schema_extra={
            "label": "接口地址",
            "hint": "老版默认 sctapi.ftqq.com，一般无需修改。",
            "order": 1,
        },
    )


class PushSectionConfig(PluginConfigBase):
    """推送策略配置。"""

    __ui_label__: ClassVar[str] = "推送策略"
    __ui_icon__: ClassVar[str] = "notifications"
    __ui_order__: ClassVar[int] = 2

    flush_interval_min: int = Field(
        default=30,
        ge=1,
        le=1440,
        description="推送聚合周期（分钟）。",
        json_schema_extra={
            "label": "聚合周期（分钟）",
            "hint": "按周期整数倍对齐（30 分钟即整点/半点）；每周期结束聚合推送一次，无报错不推。",
            "order": 0,
            "group": "schedule",
            "step": 1,
        },
    )
    daily_push_limit: int = Field(
        default=3,
        ge=1,
        le=100,
        description="每日推送条数上限。",
        json_schema_extra={
            "label": "每日推送上限",
            "hint": "超过上限后仅记录本地归档、次日不补推；免费版老版 Server酱通常每日 3~5 条，按你的额度调整。",
            "order": 1,
            "group": "schedule",
            "step": 1,
        },
    )
    min_occurrences: int = Field(
        default=3,
        ge=1,
        le=100,
        description="同一错误的通知阈值（次数）。",
        json_schema_extra={
            "label": "通知阈值（次数）",
            "hint": "同一错误在一个推送周期内出现次数达到该值才会进入通知；未达到的仅记录本地归档。设 1 表示全部通知。",
            "order": 2,
            "group": "schedule",
            "step": 1,
        },
    )
    max_entries_per_push: int = Field(
        default=50,
        ge=1,
        le=200,
        description="单次推送最多逐条列出的错误数。",
        json_schema_extra={
            "label": "单次最多列出条数",
            "hint": "超出时只列前 N 条，并提示其余见本地归档。",
            "order": 3,
            "group": "content",
            "step": 1,
        },
    )
    desc_max_len: int = Field(
        default=4000,
        ge=200,
        le=20000,
        description="推送正文最大字符数。",
        json_schema_extra={
            "label": "正文长度上限（字符）",
            "hint": "超出截断，完整记录见插件目录 errors/ 归档。",
            "order": 4,
            "group": "content",
            "step": 1,
        },
    )
    entry_summary_len: int = Field(
        default=200,
        ge=20,
        le=500,
        description="单条错误摘要的最大字符数。",
        json_schema_extra={
            "label": "摘要长度（字符）",
            "hint": "推送正文中每条错误的事件摘要截断长度。",
            "order": 5,
            "group": "content",
            "step": 1,
        },
    )


class ErrorNotifyConfig(PluginConfigBase):
    """报错日志推送插件完整配置。"""

    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    serverchan: ServerChanSectionConfig = Field(default_factory=ServerChanSectionConfig)
    push: PushSectionConfig = Field(default_factory=PushSectionConfig)
