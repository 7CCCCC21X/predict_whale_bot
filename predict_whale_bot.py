#!/usr/bin/env python3
"""
Predict.fun 大额成交 / 大额盘口 Telegram 监控机器人。

Railway 部署：
  startCommand = "python -u predict_whale_bot.py"

推荐先用 MODE=matches，只监控真实成交大单。
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import random
import re
import signal
import sys
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
from dotenv import load_dotenv


LOG = logging.getLogger("predict_whale_bot")


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_decimal(name: str, default: str) -> Decimal:
    raw = os.getenv(name, default).strip()
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise RuntimeError(f"环境变量 {name} 不是合法数字: {raw}") from exc


def parse_user_id_list(raw: str) -> "frozenset[int]":
    """解析逗号分隔的 Telegram user_id 列表。空字符串/无效项被忽略。"""
    out: "set[int]" = set()
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.add(int(token))
        except ValueError:
            LOG.warning("ALLOWED_USER_IDS 里有非整数项已忽略: %r", token)
    return frozenset(out)


@dataclass(frozen=True)
class Config:
    predict_api_base: str
    predict_api_key: str

    tg_bot_token: str
    tg_chat_id: str

    threshold_usdt: Decimal
    usdt_wei_decimals: int
    shares_wei_decimals: int

    poll_interval_sec: float
    matches_page_size: int
    matches_max_pages: int
    alert_on_startup: bool
    max_seen_ids: int
    seen_state_path: str
    runtime_state_path: str

    request_timeout_sec: float
    api_max_retries: int

    market_url_template: str
    tx_url_template: str

    # 仅这些 user.id 才能改阈值。空集合 = 沿用旧行为（只看 chat_id）。
    allowed_user_ids: frozenset

    # 默认语言：zh / en。运行期可用 /lang 切换。
    default_lang: str
    user_url_template: str

    watch_new_markets: bool
    new_markets_check_sec: int
    seen_markets_path: str

    # 每小时摘要：默认 3600s（1 小时），运行期可用 /set_summary 改并持久化。
    # 设成 0 = 关掉自动摘要（仍可 /summary 手动查）
    summary_interval_sec: int

    @property
    def threshold_usdt_wei(self) -> int:
        scale = Decimal(10) ** self.usdt_wei_decimals
        return int((self.threshold_usdt * scale).to_integral_value(rounding=ROUND_DOWN))

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()

        token = os.getenv("TG_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TG_CHAT_ID", "").strip()

        if not token:
            raise RuntimeError("缺少 TG_BOT_TOKEN。请在 Railway Variables 里填写。")
        if not chat_id:
            raise RuntimeError("缺少 TG_CHAT_ID。请在 Railway Variables 里填写。")

        return cls(
            predict_api_base=os.getenv("PREDICT_API_BASE", "https://api.predict.fun").rstrip("/"),
            predict_api_key=os.getenv("PREDICT_API_KEY", "").strip(),

            tg_bot_token=token,
            tg_chat_id=chat_id,

            threshold_usdt=env_decimal("THRESHOLD_USDT", "1000"),
            usdt_wei_decimals=int(os.getenv("USDT_WEI_DECIMALS", "18")),
            # Predict 的 amountFilled / fee.amount / taker.amount 等"份额量"字段
            # 实测是 18 位小数（份额 × 1e18），跟 USDT 一样。
            shares_wei_decimals=int(os.getenv("SHARES_WEI_DECIMALS", "18")),

            # 0.4s 轮询。Predict 官方限速 240 req/min = 4 RPS。
            # 0.4s × matches = 2.5 RPS = 150/min（占预算 63%）。
            # 剩 90/min 给：新市场（2/min）+ 翻页保险（fetch_new_matches 遇 seen
            # 就停，正常 1 页/轮）+ 偶发 /summary。429 时 get() 自动退避 + 重试。
            # 想进一步压可以 0.3（83%）但翻页冲突会更易触发 429。
            poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "0.4")),
            matches_page_size=int(os.getenv("MATCHES_PAGE_SIZE", "100")),
            matches_max_pages=int(os.getenv("MATCHES_MAX_PAGES", "5")),
            # 默认开：seen 持久化后，重启不会重复推送，所以 startup 告警是安全的。
            # 真正的"首次空 seen"会有特殊路径，不会刷屏。
            # 默认 false：刚启动只 seed 不补推历史。用户体验：bot 一打开就只
            # 推"打开之后"的真实新单，不会一上来就刷一屏旧单。
            alert_on_startup=env_bool("ALERT_ON_STARTUP", False),
            max_seen_ids=int(os.getenv("MAX_SEEN_IDS", "10000")),
            # 默认指向 /data，配合 Railway Volume 才能真正跨重启持久化。
            # /data 没挂载时仍能跑，只是状态文件读写会失败（只 log 警告，不 crash）。
            seen_state_path=os.getenv("SEEN_STATE_PATH", "/data/predict_seen.json").strip(),
            # 用户用 /menu 调过的阈值、Telegram update offset 等运行期状态。
            runtime_state_path=os.getenv("RUNTIME_STATE_PATH", "/data/runtime_state.json").strip(),

            request_timeout_sec=float(os.getenv("REQUEST_TIMEOUT_SEC", "12")),
            api_max_retries=int(os.getenv("API_MAX_RETRIES", "5")),

            # 默认指向 predict.fun 前端 + BNB Chain 浏览器；末尾带推荐 ref 参数。
            # 自定义请保留 {slug} / {address} 占位符；ref 参数可改也可去掉。
            market_url_template=os.getenv(
                "MARKET_URL_TEMPLATE", "https://predict.fun/zh-cn/market/{slug}?ref=B00EA"
            ).strip(),
            tx_url_template=os.getenv(
                "TX_URL_TEMPLATE", "https://bscscan.com/tx/{hash}"
            ).strip(),

            allowed_user_ids=parse_user_id_list(os.getenv("ALLOWED_USER_IDS", "")),

            default_lang=(os.getenv("LANG_BOT") or os.getenv("LANG", "zh")).strip().lower() or "zh",
            # 用户链接：默认指向 predict.fun 自家的 portfolio 页（带 ref）。
            # 想换 BscScan 钱包页就覆盖成 https://bscscan.com/address/{address}
            user_url_template=os.getenv(
                "USER_URL_TEMPLATE",
                "https://predict.fun/zh-cn/portfolio/{address}?ref=B00EA",
            ).strip(),

            watch_new_markets=env_bool("WATCH_NEW_MARKETS", True),
            new_markets_check_sec=int(os.getenv("NEW_MARKETS_CHECK_SEC", "30")),
            seen_markets_path=os.getenv(
                "SEEN_MARKETS_PATH", "/data/predict_seen_markets.json"
            ).strip(),

            summary_interval_sec=int(os.getenv("SUMMARY_INTERVAL_SEC", "3600")),
        )


@dataclass
class RuntimeState:
    """
    运行期可变状态。Telegram 菜单可以改这里的阈值，监控任务每轮读取最新值。
    通过 persist() 写入 RUNTIME_STATE_PATH，重启续跑会自动加载。
    """
    threshold_usdt: Decimal
    usdt_wei_decimals: int
    telegram_offset: int = 0
    lang: str = "zh"
    # 摘要轮播间隔（秒），可以 /set_summary 动态改并写盘
    summary_interval_sec: int = 3600
    # 主告警 chat（持久化里不存；from_config 时从 cfg 注入），用来区分"主频道 vs DM"
    main_chat_id: str = ""
    # 私聊订阅者：chat_id → {"threshold_usdt": str, "lang": "zh"|"en", "created_at", "last_seen"}
    # 每个私聊用户保存自己的阈值/语言，告警时按各自门槛分发。持久化在 runtime_state.json。
    subscribers: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # 内存里维护"本会话每个钱包大单计数"。重启清零（不持久化），FIFO 限 5000。
    cumulative_trades: "OrderedDict[str, int]" = field(default_factory=OrderedDict)
    # 滚动窗口：最近被告警过的成交，用于摘要聚合。重启清零（短窗口数据，不需持久化）。
    recent_matches: List[Dict[str, Any]] = field(default_factory=list)
    # /diag 用：monitor_matches 每轮把最新一轮的统计放这里，让 /diag 一眼看到
    # bot 是不是在工作 / 哪里把告警吞掉了。重启清零，不持久化。
    last_iter_stats: Dict[str, Any] = field(default_factory=dict)
    _path: str = ""  # 仅内部用，不参与持久化

    _CUMULATIVE_CAP = 5000
    _RECENT_MATCHES_CAP = 5000  # ~ 容纳极端情况下的 1 天大单（实际远超够用）

    def bump_cumulative(self, signer: str) -> int:
        """大单触发后递增。返回这是该签名地址本轮第几笔（>=1）。"""
        if not signer:
            return 0
        key = signer.lower()
        n = self.cumulative_trades.get(key, 0) + 1
        self.cumulative_trades[key] = n
        # FIFO 限上限避免长跑内存泄漏
        while len(self.cumulative_trades) > self._CUMULATIVE_CAP:
            self.cumulative_trades.popitem(last=False)
        return n

    def add_match_for_summary(
        self,
        *,
        market_title: str,
        market_slug: str,
        market_id: str,
        value: Decimal,
        signer: str,
        timestamp: datetime,
    ) -> None:
        """每笔被告警的大单都进入这个滚动窗口；摘要任务读它做聚合。"""
        self.recent_matches.append({
            "title": market_title or "?",
            "slug": market_slug or "",
            "mid": market_id or "",
            "value": value,
            "signer": signer or "",
            "ts": timestamp,
        })
        # 上限保护；同时定期把 1 天前的事件也修剪掉
        cutoff = datetime.now(timezone.utc) - timedelta(days=1)
        if len(self.recent_matches) > self._RECENT_MATCHES_CAP or len(self.recent_matches) % 100 == 0:
            self.recent_matches = [m for m in self.recent_matches if m["ts"] >= cutoff]
        if len(self.recent_matches) > self._RECENT_MATCHES_CAP:
            self.recent_matches = self.recent_matches[-self._RECENT_MATCHES_CAP:]

    def summarize_window(self, window_seconds: int) -> Optional[Dict[str, Any]]:
        """聚合最近 window 秒内的成交。无数据返回 None。"""
        if window_seconds <= 0:
            return None
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        within = [m for m in self.recent_matches if m["ts"] >= cutoff]
        if not within:
            return None

        total_value = sum((m["value"] for m in within), Decimal("0"))
        max_match = max(within, key=lambda m: m["value"])

        # 按市场聚合
        bucket: Dict[str, Dict[str, Any]] = {}
        for m in within:
            key = m["slug"] or m["mid"] or m["title"]
            if key not in bucket:
                bucket[key] = {
                    "title": m["title"],
                    "slug": m["slug"],
                    "mid": m["mid"],
                    "value": Decimal("0"),
                    "count": 0,
                }
            bucket[key]["value"] += m["value"]
            bucket[key]["count"] += 1

        top = sorted(bucket.values(), key=lambda x: x["value"], reverse=True)[:3]
        return {
            "count": len(within),
            "total_value": total_value,
            "max_value": max_match["value"],
            "max_title": max_match["title"],
            "top_markets": top,
            "window_seconds": window_seconds,
        }

    @property
    def threshold_usdt_wei(self) -> int:
        scale = Decimal(10) ** self.usdt_wei_decimals
        return int((self.threshold_usdt * scale).to_integral_value(rounding=ROUND_DOWN))

    @classmethod
    def from_config(cls, cfg: Config) -> "RuntimeState":
        # 先按 env 默认值初始化，再用磁盘上的快照覆盖（如果有）。
        lang = cfg.default_lang if cfg.default_lang in {"zh", "en"} else "zh"
        state = cls(
            threshold_usdt=cfg.threshold_usdt,
            usdt_wei_decimals=cfg.usdt_wei_decimals,
            lang=lang,
            summary_interval_sec=cfg.summary_interval_sec,
            main_chat_id=str(cfg.tg_chat_id),
            _path=cfg.runtime_state_path,
        )

        saved = load_runtime_state(cfg.runtime_state_path)
        if saved:
            try:
                if "threshold_usdt" in saved:
                    state.threshold_usdt = Decimal(str(saved["threshold_usdt"]))
                if "telegram_offset" in saved:
                    state.telegram_offset = int(saved["telegram_offset"])
                if "lang" in saved and saved["lang"] in {"zh", "en"}:
                    state.lang = saved["lang"]
                if "summary_interval_sec" in saved:
                    state.summary_interval_sec = int(saved["summary_interval_sec"])
                if "subscribers" in saved and isinstance(saved["subscribers"], dict):
                    # 校验+裁掉异常项；threshold 留 str 形态在 dict 里，用时再转 Decimal
                    valid: Dict[str, Dict[str, Any]] = {}
                    for cid, info in saved["subscribers"].items():
                        if not isinstance(info, dict):
                            continue
                        try:
                            Decimal(str(info.get("threshold_usdt", "0")))
                        except (InvalidOperation, ValueError, TypeError):
                            continue
                        valid[str(cid)] = {
                            "threshold_usdt": str(info.get("threshold_usdt", "0")),
                            "lang": info.get("lang", "zh") if info.get("lang") in {"zh", "en"} else "zh",
                            "created_at": str(info.get("created_at", "")),
                            "last_seen": str(info.get("last_seen", "")),
                        }
                    state.subscribers = valid
                LOG.info(
                    "已从 %s 恢复 runtime state: threshold=%s offset=%s lang=%s summary=%ss subscribers=%d",
                    cfg.runtime_state_path,
                    state.threshold_usdt, state.telegram_offset, state.lang,
                    state.summary_interval_sec, len(state.subscribers),
                )
            except (InvalidOperation, ValueError, TypeError) as exc:
                LOG.warning("runtime state 字段格式异常 (%s): %s — 用 env 默认", saved, exc)

        return state

    def persist(self) -> None:
        """把当前阈值 + offset + lang + summary_interval + subscribers 写盘。失败只 log。"""
        if not self._path:
            return
        save_runtime_state(self._path, {
            "threshold_usdt": str(self.threshold_usdt),
            "telegram_offset": self.telegram_offset,
            "lang": self.lang,
            "summary_interval_sec": self.summary_interval_sec,
            "subscribers": self.subscribers,
        })

    # ---------- 私聊订阅者（每个 chat 自己的阈值 / 语言） ----------

    def is_main_chat(self, chat_id: Any) -> bool:
        return str(chat_id) == self.main_chat_id

    def get_threshold_for(self, chat_id: Any) -> Decimal:
        """主告警频道用全局阈值；其它 chat 用各自订阅记录里的阈值。"""
        key = str(chat_id)
        if key == self.main_chat_id:
            return self.threshold_usdt
        info = self.subscribers.get(key)
        if not info:
            return self.threshold_usdt  # 还没订阅，回退全局
        try:
            return Decimal(str(info.get("threshold_usdt", "0")))
        except (InvalidOperation, ValueError, TypeError):
            return self.threshold_usdt

    def get_lang_for(self, chat_id: Any) -> str:
        key = str(chat_id)
        if key == self.main_chat_id:
            return self.lang
        info = self.subscribers.get(key)
        if info and info.get("lang") in {"zh", "en"}:
            return info["lang"]
        return self.lang

    def upsert_subscriber(
        self,
        chat_id: Any,
        *,
        threshold_usdt: Optional[Decimal] = None,
        lang: Optional[str] = None,
    ) -> Dict[str, Any]:
        """私聊用户首次发命令就注册；之后调阈值/切语言走这里。主告警频道不入表。"""
        key = str(chat_id)
        if key == self.main_chat_id:
            return {}
        now = datetime.now(timezone.utc).isoformat()
        existing = self.subscribers.get(key)
        if not existing:
            existing = {
                # 默认从 env 全局阈值起步
                "threshold_usdt": str(self.threshold_usdt),
                "lang": self.lang,
                "created_at": now,
                "last_seen": now,
            }
            self.subscribers[key] = existing
            LOG.info("[subscribers] 新订阅 chat_id=%s 默认阈值=%s", key, existing["threshold_usdt"])
        existing["last_seen"] = now
        if threshold_usdt is not None:
            existing["threshold_usdt"] = str(threshold_usdt)
        if lang in {"zh", "en"}:
            existing["lang"] = lang
        return existing

    def remove_subscriber(self, chat_id: Any) -> bool:
        key = str(chat_id)
        if key in self.subscribers:
            del self.subscribers[key]
            return True
        return False


def load_runtime_state(path: str) -> Optional[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        LOG.warning("runtime state 文件格式异常 (%s)，期望 object，得到 %s", path, type(data))
    except Exception as exc:
        LOG.warning("加载 runtime state 失败 (%s): %s", path, exc)
    return None


def save_runtime_state(path: str, data: Dict[str, Any]) -> None:
    if not path:
        return
    tmp = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as exc:
        LOG.warning("保存 runtime state 失败 (%s): %s", path, exc)


def check_persistence_writable(cfg: "Config") -> bool:
    """启动时探测一次 RUNTIME_STATE_PATH 是否真的可写。Railway 没挂 volume
    时这里会失败，让 main() 决定是否打告警。"""
    path = cfg.runtime_state_path
    if not path:
        return True  # 用户主动关了持久化
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        probe = f"{path}.probe"
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        os.remove(probe)
        return True
    except Exception as exc:
        LOG.warning("[persistence] %s 不可写: %s", path, exc)
        return False


def to_decimal(value: Any, decimals: int = 18, *, wei_hint: bool = True) -> Decimal:
    """
    把 API 里的数字转成 Decimal。

    - "123.45" 直接作为 Decimal
    - 很大的整数字符串按 wei / 1e18 处理
    - 空值或异常值返回 0
    """
    if value is None:
        return Decimal("0")

    if isinstance(value, Decimal):
        return value

    s = str(value).strip().replace(",", "")

    if not s or s.lower() in {"none", "null", "nan"}:
        return Decimal("0")

    if s.startswith("$"):
        s = s[1:]

    try:
        out = Decimal(s)
    except InvalidOperation:
        return Decimal("0")

    # Predict 的金额参数叫 minValueUsdtWei，通常是整数 wei。
    # 对没有小数点且数量级很大的值做自动缩放。
    if wei_hint and "." not in s and abs(out) >= Decimal(10) ** max(decimals - 2, 1):
        out = out / (Decimal(10) ** decimals)

    return out


def fmt_decimal(x: Decimal, places: int = 2) -> str:
    q = Decimal(10) ** -places
    x = x.quantize(q, rounding=ROUND_DOWN)
    return f"{x:,.{places}f}"


def fmt_money(x: Decimal) -> str:
    """钱的自适应格式：≥ $100 不要小数（$12,480），< $100 保留 2 位（$50.50）。
    告警标题用得到，避免 "$12,480.00" 这种累赘。"""
    if x <= 0:
        return "-"
    if x >= 100:
        return f"${int(x.to_integral_value(rounding=ROUND_DOWN)):,}"
    return f"${fmt_decimal(x, 2)}"


def short_addr(addr: Any) -> str:
    s = str(addr or "")
    if len(s) <= 14:
        return s or "-"
    return f"{s[:6]}…{s[-6:]}"


def load_seen(path: str, max_size: int) -> "OrderedDict[str, None]":
    """从磁盘加载 seen ID 列表，重启后用来过滤已推送过的事件。"""
    seen: "OrderedDict[str, None]" = OrderedDict()
    if not path or not os.path.exists(path):
        return seen
    try:
        with open(path, "r", encoding="utf-8") as f:
            ids = json.load(f)
        if not isinstance(ids, list):
            LOG.warning("seen 状态文件格式异常 (%s)，忽略", path)
            return seen
        for eid in ids[-max_size:]:
            seen[str(eid)] = None
        LOG.info("已从 %s 加载 %d 条 seen", path, len(seen))
    except Exception as exc:
        LOG.warning("加载 seen 状态失败 (%s): %s", path, exc)
    return seen


def save_seen(path: str, seen: "OrderedDict[str, None]") -> None:
    """原子写 seen 列表。失败只 log，不影响监控。"""
    if not path:
        return
    tmp = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(list(seen.keys()), f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as exc:
        LOG.warning("保存 seen 状态失败 (%s): %s", path, exc)


def load_seen_markets(path: str) -> Set[int]:
    """已发过"新市场上线"告警的 market_id 集合。"""
    if not path or not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            ids = json.load(f)
        if not isinstance(ids, list):
            return set()
        out: Set[int] = set()
        for x in ids:
            try:
                out.add(int(x))
            except Exception:
                pass
        LOG.info("已从 %s 加载 %d 条已知市场 ID", path, len(out))
        return out
    except Exception as exc:
        LOG.warning("加载已知市场列表失败 (%s): %s", path, exc)
        return set()


def save_seen_markets(path: str, ids: Set[int]) -> None:
    if not path:
        return
    tmp = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(ids), f)
        os.replace(tmp, path)
    except Exception as exc:
        LOG.warning("保存已知市场列表失败 (%s): %s", path, exc)


def extract_username(event: Dict[str, Any]) -> str:
    """从 taker / event / user 嵌套字段里取 username，找不到返回空串。

    Predict 的实际字段名不一定是 username — 一并尝试 handle / alias /
    nickname / displayName 等常见变体，覆盖更多可能的 schema。
    """
    taker = event.get("taker") if isinstance(event.get("taker"), dict) else {}
    user = taker.get("user") if isinstance(taker.get("user"), dict) else {}
    profile = taker.get("profile") if isinstance(taker.get("profile"), dict) else {}
    account = taker.get("account") if isinstance(taker.get("account"), dict) else {}

    candidates = (
        # taker 直接挂的
        taker.get("username"),
        taker.get("name"),
        taker.get("displayName"),
        taker.get("handle"),
        taker.get("alias"),
        taker.get("nickname"),
        # taker.user.*
        user.get("username"),
        user.get("name"),
        user.get("displayName"),
        user.get("handle"),
        user.get("nickname"),
        # taker.profile.*
        profile.get("username"),
        profile.get("name"),
        profile.get("displayName"),
        profile.get("handle"),
        # taker.account.*
        account.get("username"),
        account.get("name"),
        account.get("handle"),
        # event 顶层
        event.get("takerUsername"),
        event.get("takerName"),
        event.get("username"),
    )
    for c in candidates:
        if c:
            s = str(c).strip()
            # 过滤明显是地址的（0x 开头 + 长度），username 不会长这样
            if s and not (s.startswith("0x") and len(s) >= 20):
                return s
    return ""


def slug_to_label(slug: str) -> str:
    """把 'polymarket-fdv-one-day-after-launch' 转成 'Polymarket Fdv One Day After Launch'。

    用作市场告警里"父问题"的近似显示，让标题不再光秃秃只有一个 "$4B"。
    """
    if not slug:
        return ""
    text = slug.replace("-", " ").replace("_", " ").strip()
    if not text:
        return ""
    return " ".join(w.capitalize() if w else w for w in text.split())


def extract_signer(event: Dict[str, Any]) -> str:
    taker = event.get("taker") if isinstance(event.get("taker"), dict) else {}
    return str(taker.get("signer") or event.get("signer") or "").strip()


def extract_tx_hash(event: Dict[str, Any]) -> str:
    """
    从可能的字段里取出"链上 settlement 交易哈希"，找不到返回空串。

    注意：Predict 把"撮合事件 ID"和"链上结算 tx 哈希"是分开的。一些字段
    （比如 hash / id）只是撮合层面的内部 ID，bscscan 上根本查不到。
    所以候选顺序按"越像 on-chain settlement 的越靠前"排，并显式去重内部 id。
    """
    transaction = event.get("transaction") if isinstance(event.get("transaction"), dict) else None
    settlement = event.get("settlement") if isinstance(event.get("settlement"), dict) else None
    onchain = event.get("onchain") if isinstance(event.get("onchain"), dict) else None

    # 越往前越优先：明确表明是链上结算 / 区块链交易的字段
    candidates = (
        event.get("settlementTxHash"),
        event.get("settlementTransactionHash"),
        (settlement or {}).get("transactionHash"),
        (settlement or {}).get("txHash"),
        (settlement or {}).get("hash"),
        event.get("onchainTxHash"),
        event.get("onchainHash"),
        (onchain or {}).get("transactionHash"),
        (onchain or {}).get("hash"),
        event.get("transactionHash"),
        (transaction or {}).get("transactionHash"),
        (transaction or {}).get("hash"),
        event.get("txHash"),
        event.get("txnHash"),
        event.get("transaction_hash"),
        event.get("hash"),  # 最后兜底；这个常常是撮合层面的内部 ID
    )
    for c in candidates:
        if c:
            s = str(c).strip()
            if s:
                return s
    return ""


def stable_event_id(event: Dict[str, Any]) -> str:
    """
    用稳定的业务字段构造去重 ID。

    旧实现的 fallback 是对整条 event JSON 做 hash，遇到 API 加新字段、字段顺序
    变化、嵌套 market 对象的 title/slug 改名时，就会把同一笔成交当成新成交
    导致重复推送。这里改成只取已知 stable 的字段，按固定顺序拼接后 hash。
    """
    market = event.get("market") or {}
    taker = event.get("taker") if isinstance(event.get("taker"), dict) else {}
    outcome = taker.get("outcome") if isinstance(taker.get("outcome"), dict) else {}
    makers = event.get("makers") or []

    maker_sigs = "|".join(sorted(
        str((m or {}).get("signer") or "")
        for m in makers
        if isinstance(m, dict)
    ))

    parts = [
        extract_tx_hash(event),
        str(event.get("executedAt") or event.get("createdAt") or ""),
        str(market.get("id") or event.get("marketId") or ""),
        str(taker.get("signer") or event.get("signer") or ""),
        str(taker.get("quoteType") or event.get("quoteType") or ""),
        str(outcome.get("indexSet") or outcome.get("name") or ""),
        str(event.get("amountFilled") or taker.get("amount") or ""),
        str(event.get("priceExecuted") or taker.get("price") or ""),
        maker_sigs,
    ]

    raw = "\x1f".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_text(s: Any, limit: int = 180) -> str:
    text = str(s or "").strip()
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return html.escape(text)


TRANSLATIONS: Dict[str, Dict[str, str]] = {
    "zh": {
        "whale_title": "🐳 <b>巨鲸提醒</b>",
        "made_trade": "进行了一笔交易：",
        "buy": "买入",
        "sell": "卖出",
        "trade": "成交",
        "shares": "股",
        "view_market": "📊 查看市场",
        "view_wallet": "👤 查看钱包",
        "view_tx": "🔗 查看交易",
        "anon_wallet": "匿名钱包",
        "match_started": "✅ <b>Predict 成交大单监控已启动</b>",
        "newm_started": "🆕 <b>Predict 新市场监控已启动</b>",
        "threshold": "阈值",
        "interval": "检查间隔",
        "open_menu_hint": "点底部「菜单」或发 /menu 调阈值",
        "menu_title": "🐋 <b>Predict 监控</b>",
        "match_short": "成交阈值",
        "lang_zh": "中文",
        "lang_en": "EN",
        "lang_switched": "已切换到中文",
        "btn_lang_switch": "🌐 EN",
        "btn_refresh": "🔄",
        "btn_summary": "📊 摘要",
        "btn_test": "🧪 测试推送",  # 保留键以备 _send_test_alert 调用，按钮已不挂菜单
        "implied_label": "隐含",
        "fee_label": "手续费",
        "makers_label": "对手方",
        "time_just_now": "刚刚",
        "time_ago_s_fmt": "{n}秒前",
        "time_ago_m_fmt": "{n}分钟前",
        "time_ago_h_fmt": "{n}小时前",
        "cumulative_fmt": "本轮第 {n} 笔",
        "btn_custom": "✏️ 自定义",
        "test_caption": "🧪 <b>测试推送</b>（不是真实成交）",
        # 卡片内字段标签
        "card_side": "方向",
        "card_amount": "数量",
        "card_price": "价格",
        "card_trader": "交易者",
        "card_time": "时间",
        "card_traded": "成交",
        "tier_super": "超级鲸鱼单",
        "tier_big": "大鲸鱼单",
        "tier_mid": "中型鲸鱼单",
        "tier_normal": "普通大单",
        "tier_small": "小单",
        "summary_title": "📊 <b>Predict Whale 摘要</b>",
        "summary_period_fmt": "过去 {label}：",
        "summary_total_count": "总大单",
        "summary_total_value": "总成交额",
        "summary_max_trade": "最大单",
        "summary_top_markets": "最活跃市场",
        "summary_none_fmt": "过去 {label}没有大单",
        "summary_off": "自动摘要已关闭（间隔 = 0），仍可 /summary 手动查",
        "summary_set_ok_fmt": "摘要间隔已设为 <b>{label}</b>",
        "summary_set_off": "已关闭自动摘要（仍可 /summary 手动查）",
        "summary_set_help": (
            "用法：<code>/set_summary 60</code>（分钟）或 <code>/set_summary 0</code> 关闭。\n"
            "支持 <code>/set_summary 30s</code> / <code>/set_summary 2h</code>。"
        ),
        "summary_set_invalid": "❌ 无效时长。1m–24h，或 0 关闭。",
        "shares_unit": "笔",
        "stopped": "🛑 <b>Predict 大额监控已停止</b>",
        "help_text": (
            "🐋 <b>Predict Whale Bot</b>\n\n"
            "<b>私聊 bot</b>：自动注册，能改你<b>自己的</b>门槛、自己的语言。\n"
            "<b>主告警频道</b>：管理员才能改全局阈值。\n\n"
            "<b>菜单按钮</b>\n"
            "💵 预设 $100/$300/$500/$1k\n"
            "✏️ 自定义（最低 $10）\n"
            "🌐 切换中英文 · 🔄 刷新 · 🧪 测试推送（admin）\n\n"
            "<b>命令</b>（任意聊天）\n"
            "<code>/menu</code> · <code>/status</code> · <code>/summary</code>\n"
            "<code>/whoami</code> · <code>/lang zh|en</code>\n"
            "<code>/set_match 100</code>  改阈值（私聊改你自己的；主频道要 admin）\n"
            "<code>/unsubscribe</code>  退订私聊推送\n"
            "<code>/speed</code>  当前 API 占用\n\n"
            "<b>仅管理员</b>\n"
            "<code>/set_summary 60</code> · <code>/benchmark 0.2 15</code>\n"
            "<code>/ramp</code> 阶梯压测（3.2→0.2 找最稳定档位）\n"
            "<code>/subscribers</code> · <code>/test</code>"
        ),
    },
    "en": {
        "whale_title": "🐳 <b>Whale Alert</b>",
        "made_trade": "made a trade:",
        "buy": "BUY",
        "sell": "SELL",
        "trade": "TRADE",
        "shares": "shares",
        "view_market": "📊 Market",
        "view_wallet": "👤 Wallet",
        "view_tx": "🔗 Tx",
        "anon_wallet": "Anon wallet",
        "match_started": "✅ <b>Predict match monitor started</b>",
        "newm_started": "🆕 <b>Predict new-market watcher started</b>",
        "threshold": "Threshold",
        "interval": "Check interval",
        "open_menu_hint": "Tap “Menu” or send /menu to change thresholds",
        "menu_title": "🐋 <b>Predict Whale Bot</b>",
        "match_short": "Match threshold",
        "lang_zh": "中文",
        "lang_en": "EN",
        "lang_switched": "Switched to English",
        "btn_lang_switch": "🌐 中",
        "btn_refresh": "🔄",
        "btn_summary": "📊 Summary",
        "btn_test": "🧪 Test",
        "implied_label": "Implied",
        "fee_label": "Fee",
        "makers_label": "Makers",
        "time_just_now": "just now",
        "time_ago_s_fmt": "{n}s ago",
        "time_ago_m_fmt": "{n}m ago",
        "time_ago_h_fmt": "{n}h ago",
        "cumulative_fmt": "trade #{n} this session",
        "btn_custom": "✏️ Custom",
        "test_caption": "🧪 <b>Test alert</b> (not a real trade)",
        "card_side": "Side",
        "card_amount": "Amount",
        "card_price": "Price",
        "card_trader": "Trader",
        "card_time": "Time",
        "card_traded": "trade",
        "tier_super": "Super whale",
        "tier_big": "Big whale",
        "tier_mid": "Mid whale",
        "tier_normal": "Whale",
        "tier_small": "Small",
        "summary_title": "📊 <b>Predict Whale Summary</b>",
        "summary_period_fmt": "Past {label}:",
        "summary_total_count": "Total trades",
        "summary_total_value": "Total volume",
        "summary_max_trade": "Largest",
        "summary_top_markets": "Top markets",
        "summary_none_fmt": "No large trades in the past {label}",
        "summary_off": "Auto-summary off (interval=0), use /summary on demand",
        "summary_set_ok_fmt": "Summary interval set to <b>{label}</b>",
        "summary_set_off": "Auto-summary off (use /summary on demand)",
        "summary_set_help": (
            "Usage: <code>/set_summary 60</code> (minutes) or <code>/set_summary 0</code> to disable.\n"
            "Also accepts <code>/set_summary 30s</code> / <code>/set_summary 2h</code>."
        ),
        "summary_set_invalid": "❌ Invalid duration. Range 1m–24h, or 0 to disable.",
        "shares_unit": "trades",
        "stopped": "🛑 <b>Predict whale-bot stopped</b>",
        "help_text": (
            "🐋 <b>Predict Whale Bot</b>\n\n"
            "<b>DM the bot</b>: auto-registers; controls <b>your own</b> threshold + language.\n"
            "<b>Main alert chat</b>: admin only for the global threshold.\n\n"
            "<b>Menu buttons</b>\n"
            "💵 Presets $100/$300/$500/$1k\n"
            "✏️ Custom (min $10)\n"
            "🌐 Toggle zh/en · 🔄 Refresh · 🧪 Test (admin)\n\n"
            "<b>Commands</b> (any chat)\n"
            "<code>/menu</code> · <code>/status</code> · <code>/summary</code>\n"
            "<code>/whoami</code> · <code>/lang zh|en</code>\n"
            "<code>/set_match 100</code>  threshold (DM = your own; main chat = admin)\n"
            "<code>/unsubscribe</code>  stop DM alerts\n"
            "<code>/speed</code>  current API usage\n\n"
            "<b>Admin only</b>\n"
            "<code>/set_summary 60</code> · <code>/benchmark 0.2 15</code>\n"
            "<code>/ramp</code> ladder bench (3.2→0.2 to find sweet-spot)\n"
            "<code>/subscribers</code> · <code>/test</code>"
        ),
    },
}


def t(state: "RuntimeState", key: str) -> str:
    """运行期翻译查找。state.lang 不在表里则回退中文。"""
    table = TRANSLATIONS.get(state.lang) or TRANSLATIONS["zh"]
    return table.get(key) or TRANSLATIONS["zh"].get(key) or key


def chunk_html_safely(text: str, limit: int = 3900) -> List[str]:
    """
    按行切块，不会随便切断 HTML 标签或多字节字符。
    极端情况下单行超过 limit，才会硬切；正常的告警每行都很短，不会触发。
    """
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    chunks: List[str] = []
    current: List[str] = []
    size = 0

    for line in text.splitlines(keepends=True):
        # 超长单行：硬切（罕见；正常 alert 行都 < 200 字符）
        while len(line) > limit:
            if current:
                chunks.append("".join(current))
                current = []
                size = 0
            chunks.append(line[:limit])
            line = line[limit:]

        if size + len(line) > limit and current:
            chunks.append("".join(current))
            current = []
            size = 0

        current.append(line)
        size += len(line)

    if current:
        chunks.append("".join(current))

    return chunks


class Telegram:
    def __init__(self, cfg: Config, client: httpx.AsyncClient) -> None:
        self.cfg = cfg
        self.client = client
        self.base = f"https://api.telegram.org/bot{cfg.tg_bot_token}"
        self._lock = asyncio.Lock()

    async def send(
        self,
        text: str,
        *,
        silent: bool = False,
        reply_markup: Optional[Dict[str, Any]] = None,
        chat_id: Optional[Any] = None,
    ) -> Optional[int]:
        """
        发消息。chat_id 可选；不传 → 默认主告警频道（cfg.tg_chat_id）。
        命令应答场景里传入消息源 chat_id，让 bot 在私聊/群里就地回复。
        """
        chunks = chunk_html_safely(text, 3900) or [text]
        last_message_id: Optional[int] = None
        target_chat = str(chat_id) if chat_id is not None else self.cfg.tg_chat_id

        # 串行发送，避免多个监控任务并发触发 Telegram 限流。
        async with self._lock:
            for idx, chunk in enumerate(chunks):
                # markup 只附在最后一片，否则按钮会被前面的分片覆盖。
                markup = reply_markup if idx == len(chunks) - 1 else None
                last_message_id = await self._send_one(
                    chunk, silent=silent, reply_markup=markup, chat_id=target_chat,
                )
                await asyncio.sleep(0.05)

        return last_message_id

    async def _send_one(
        self,
        text: str,
        *,
        silent: bool,
        reply_markup: Optional[Dict[str, Any]] = None,
        chat_id: Optional[str] = None,
    ) -> Optional[int]:
        payload: Dict[str, Any] = {
            "chat_id": chat_id or self.cfg.tg_chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "disable_notification": silent,
        }
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)

        for attempt in range(5):
            try:
                resp = await self.client.post(f"{self.base}/sendMessage", json=payload)
            except Exception as exc:
                LOG.warning("Telegram sendMessage 网络异常: %s", exc)
                await asyncio.sleep(min(2 ** attempt, 10))
                continue

            if resp.status_code == 429:
                retry_after = 1.0
                try:
                    retry_after = float(resp.json().get("parameters", {}).get("retry_after", 1))
                except Exception:
                    pass
                LOG.warning("Telegram 429 限流，等待 %.1fs", retry_after)
                await asyncio.sleep(retry_after + 0.5)
                continue

            if 500 <= resp.status_code < 600:
                LOG.warning("Telegram %s，重试", resp.status_code)
                await asyncio.sleep(min(2 ** attempt, 10))
                continue

            if resp.status_code >= 400:
                body = resp.text[:500]
                # 400 + "can't parse entities" 通常是消息里 HTML 标签或实体被误切。
                # 退一步用纯文本（去掉所有标签）再发一次，确保信息能落地。
                if (
                    resp.status_code == 400
                    and "parse" in body.lower()
                    and payload.get("parse_mode")
                ):
                    LOG.warning("Telegram HTML 解析失败，回退纯文本: %s", body)
                    fallback_payload = dict(payload)
                    fallback_payload.pop("parse_mode", None)
                    fallback_payload.pop("reply_markup", None)  # markup 也可能挂在解析问题上
                    fallback_payload["text"] = re.sub(r"<[^>]*>", "", payload["text"])
                    try:
                        resp2 = await self.client.post(
                            f"{self.base}/sendMessage", json=fallback_payload
                        )
                        if resp2.status_code < 400:
                            try:
                                return int(resp2.json().get("result", {}).get("message_id"))
                            except Exception:
                                return None
                        LOG.error(
                            "Telegram 纯文本回退也失败: %s %s",
                            resp2.status_code, resp2.text[:300],
                        )
                    except Exception as exc:
                        LOG.error("Telegram 纯文本回退异常: %s", exc)
                    return None

                # 4xx 一般是请求本身的问题（比如格式错误），重试也修不了。
                LOG.error("Telegram sendMessage 失败: %s %s", resp.status_code, body)
                return None

            try:
                return int(resp.json().get("result", {}).get("message_id"))
            except Exception:
                return None

        LOG.error("Telegram sendMessage 多次重试仍失败，丢弃消息")
        return None

    async def edit_message(
        self,
        message_id: int,
        text: str,
        *,
        reply_markup: Optional[Dict[str, Any]] = None,
        chat_id: Optional[Any] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "chat_id": str(chat_id) if chat_id is not None else self.cfg.tg_chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)

        try:
            resp = await self.client.post(f"{self.base}/editMessageText", json=payload)
            if resp.status_code >= 400:
                # 常见无害报错：消息内容未变（"message is not modified"）— 直接忽略。
                body = resp.text[:300]
                if "message is not modified" not in body:
                    LOG.warning("editMessageText 失败: %s %s", resp.status_code, body)
        except Exception as exc:
            LOG.warning("editMessageText 异常: %s", exc)

    async def answer_callback_query(
        self, callback_query_id: str, text: str = ""
    ) -> None:
        try:
            await self.client.post(
                f"{self.base}/answerCallbackQuery",
                json={"callback_query_id": callback_query_id, "text": text},
            )
        except Exception as exc:
            LOG.warning("answerCallbackQuery 异常: %s", exc)


class Predict:
    # Predict 官方限速：240 req/min = 4 RPS（mainnet 默认 / testnet）
    RATE_LIMIT_PER_MIN = 240

    def __init__(self, cfg: Config, client: httpx.AsyncClient) -> None:
        self.cfg = cfg
        self.client = client
        # 滑动窗口：API 请求时间戳，用来周期性报实际 req/min
        self._request_log: List[float] = []
        self._rate_log_last = 0.0

    @property
    def headers(self) -> Dict[str, str]:
        if self.cfg.predict_api_key:
            return {"x-api-key": self.cfg.predict_api_key}
        return {}

    def _record_request(self) -> None:
        """记录一次 API 调用，每 5 分钟把窗口内总数 + 平均 RPS 写一条日志。"""
        import time as _time
        now = _time.monotonic()
        self._request_log.append(now)
        # 只保留最近 60 秒
        cutoff = now - 60
        self._request_log = [t for t in self._request_log if t >= cutoff]
        # 每 300 秒报一次
        if now - self._rate_log_last >= 300:
            self._rate_log_last = now
            count_60s = len(self._request_log)
            pct = count_60s * 100 // self.RATE_LIMIT_PER_MIN
            LOG.info(
                "[predict-api] 最近 60s = %d 次调用（限速 %d/min，占用 %d%%）",
                count_60s, self.RATE_LIMIT_PER_MIN, pct,
            )

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        统一的 Predict API GET：429 / 5xx / 网络错误一律指数退避重试。
        - 429：尊重 Retry-After header，否则按 2^attempt（带 jitter）退
        - 5xx：指数退避 + jitter
        - 网络异常：同上
        - 4xx（非 429）：直接抛
        - 重试上限由 API_MAX_RETRIES 控制（默认 5）
        """
        url = f"{self.cfg.predict_api_base}{path}"
        last_exc: Optional[BaseException] = None
        max_attempts = max(1, self.cfg.api_max_retries)
        self._record_request()

        for attempt in range(max_attempts):
            try:
                resp = await self.client.get(url, params=params, headers=self.headers)
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPError) as exc:
                last_exc = exc
                delay = min(30.0, 2 ** attempt + random.random())
                LOG.warning(
                    "Predict API %s 网络异常 (attempt=%d/%d): %s — %.1fs 后重试",
                    path, attempt + 1, max_attempts, exc, delay,
                )
                await asyncio.sleep(delay)
                continue

            if resp.status_code == 429:
                retry_after_raw = resp.headers.get("Retry-After")
                try:
                    delay = float(retry_after_raw) if retry_after_raw else min(30.0, 2 ** attempt)
                except ValueError:
                    delay = min(30.0, 2 ** attempt)
                LOG.warning(
                    "Predict API %s 429 限流 (attempt=%d/%d) — %.1fs 后重试",
                    path, attempt + 1, max_attempts, delay,
                )
                await asyncio.sleep(delay + 0.5)
                continue

            if 500 <= resp.status_code < 600:
                delay = min(30.0, 2 ** attempt + random.random())
                LOG.warning(
                    "Predict API %s %s (attempt=%d/%d) — %.1fs 后重试",
                    path, resp.status_code, attempt + 1, max_attempts, delay,
                )
                await asyncio.sleep(delay)
                continue

            resp.raise_for_status()

            data = resp.json()

            if isinstance(data, dict) and data.get("success") is False:
                raise RuntimeError(f"Predict API 返回失败: {data}")

            if not isinstance(data, dict):
                raise RuntimeError(f"Predict API 返回格式不是 dict: {type(data)}")

            return data

        raise RuntimeError(
            f"Predict API {path} 重试 {max_attempts} 次仍失败"
        ) from last_exc

    async def fetch_new_matches(
        self,
        seen_ids: Set[str],
        page_size: int,
        max_pages: int,
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        从最新往旧翻页拉取成交，遇到已 seen 的事件即停止。

        不传 minValueUsdtWei：实测 Predict 这个参数是按"成交份额数"过滤的，
        会把"低份额 × 高单价"的真大单（如 100 shares × $50 = $5000）一并丢掉。
        因此服务端不过滤，全部交给客户端按 notional value 过滤。

        返回 (按时间倒序排列的新事件, 是否翻满 max_pages 仍未追上)。
        """
        out: List[Dict[str, Any]] = []
        after: Optional[str] = None

        for _ in range(max(1, max_pages)):
            params: Dict[str, Any] = {"first": page_size}
            if after:
                params["after"] = after

            data = await self.get("/v1/orders/matches", params=params)
            rows = list(data.get("data") or data.get("matches") or [])
            if not rows:
                return out, False

            for ev in rows:
                if stable_event_id(ev) in seen_ids:
                    return out, False
                out.append(ev)

            # 不到一页说明已经到尽头
            if len(rows) < page_size:
                return out, False

            page_info = data.get("pageInfo") or {}
            after = (
                data.get("cursor")
                or data.get("nextCursor")
                or page_info.get("endCursor")
            )
            if not after:
                return out, False

        return out, True

    async def fetch_open_markets(self) -> Dict[int, Dict[str, Any]]:
        """
        分页拉取尚未结束的市场。返回 market_id -> 原始市场对象。

        过滤策略：只显式排除"已知关闭/已结算"的状态，未知状态默认收下。
        Predict 的状态字段实际是什么值（OPEN/ACTIVE/LIVE/TRADING/...）不一定，
        只信白名单会把没见过的状态全部丢掉，导致新市场永远不被发现。
        """
        markets: Dict[int, Dict[str, Any]] = {}
        after: Optional[str] = None
        pages = 0
        status_counts: Dict[str, int] = {}
        skipped_invisible = 0
        skipped_closed = 0

        while True:
            pages += 1
            params: Dict[str, Any] = {"first": 100}

            if after:
                params["after"] = after

            data = await self.get("/v1/markets", params=params)
            rows = data.get("data") or data.get("markets") or []

            for m in rows:
                try:
                    mid = int(m.get("id") or m.get("marketId"))
                except Exception:
                    continue

                visible = m.get("isVisible", True)
                trading_status = str(m.get("tradingStatus") or m.get("status") or "").upper()
                status_counts[trading_status or "(none)"] = status_counts.get(trading_status or "(none)", 0) + 1

                if not visible:
                    skipped_invisible += 1
                    continue

                if trading_status in CLOSED_LIKE_STATUSES:
                    skipped_closed += 1
                    continue

                markets[mid] = m

            page_info = data.get("pageInfo") or {}
            after = (
                data.get("cursor")
                or data.get("nextCursor")
                or page_info.get("endCursor")
            )
            has_next = bool(data.get("hasNextPage") or page_info.get("hasNextPage") or after)

            if not has_next or not after or pages >= 100:
                break

        LOG.info(
            "fetch_open_markets: 收 %d 个开放市场 (扫 %d 页，按 tradingStatus 分布 %s，跳过不可见 %d，跳过已结束 %d)",
            len(markets), pages, status_counts, skipped_invisible, skipped_closed,
        )
        return markets


# 显式排除的"已结束"状态。其它任何状态（OPEN/ACTIVE/LIVE/TRADING/PENDING/...）都收。
CLOSED_LIKE_STATUSES = {
    "CLOSED", "CLOSE",
    "RESOLVED", "RESOLVING",
    "CANCELLED", "CANCELED",
    "EXPIRED",
    "SETTLED", "SETTLING",
    "ENDED",
    "ARCHIVED",
}


def market_title_of(m: Dict[str, Any]) -> str:
    mid = m.get("id") or m.get("marketId") or "?"
    return str(
        m.get("title") or m.get("question") or m.get("name") or f"Market {mid}"
    )


PRESET_AMOUNTS = (Decimal("100"), Decimal("300"), Decimal("500"), Decimal("1000"))
MIN_THRESHOLD_USDT = Decimal("10")  # 防止 DM 用户设到 $1 导致刷屏
MAX_THRESHOLD_USDT = Decimal("10000000")


def _menu_text(
    state: RuntimeState, *, mode: str = "", chat_id: Optional[Any] = None
) -> str:
    """紧凑两行：阈值 + 元数据。chat_id 决定显示主频道还是用户私有的阈值/语言。"""
    if chat_id is None or state.is_main_chat(chat_id):
        threshold = state.threshold_usdt
        lang = state.lang
        scope_hint = ""
    else:
        threshold = state.get_threshold_for(chat_id)
        lang = state.get_lang_for(chat_id)
        # 给私聊用户一句话提示这是"个人门槛"
        if lang == "zh":
            scope_hint = "（私聊订阅 · 你的门槛）\n"
        else:
            scope_hint = "(your private subscription)\n"

    # 临时 view-state 用来取翻译；不修改真正的 state.lang
    tmp_state = state if lang == state.lang else dataclass_replace_lang(state, lang)
    lang_label = t(tmp_state, "lang_zh") if lang == "zh" else t(tmp_state, "lang_en")
    return (
        f"{t(tmp_state, 'menu_title')}\n\n"
        f"{scope_hint}"
        f"💵 {t(tmp_state, 'match_short')} <b>${fmt_decimal(threshold, 2)} USDT</b>\n"
        f"🌐 {lang_label}"
    )


def dataclass_replace_lang(state: RuntimeState, lang: str) -> RuntimeState:
    """生成一个 lang 不同的浅拷贝 state，用来给翻译函数传上下文。"""
    if lang == state.lang:
        return state
    # 不能简单复制 OrderedDict / list 的 default_factory 字段；这里只 view 用
    clone = RuntimeState(
        threshold_usdt=state.threshold_usdt,
        usdt_wei_decimals=state.usdt_wei_decimals,
        telegram_offset=state.telegram_offset,
        lang=lang,
        summary_interval_sec=state.summary_interval_sec,
        main_chat_id=state.main_chat_id,
        subscribers=state.subscribers,
        cumulative_trades=state.cumulative_trades,
        recent_matches=state.recent_matches,
        _path="",  # 不让 view 副本碰盘
    )
    return clone


def _menu_keyboard(state: RuntimeState, *, chat_id: Optional[Any] = None) -> Dict[str, Any]:
    """3 行紧凑布局：预设 + 自定义；底部控制按钮。chat_id 决定按钮文案的语言。"""
    lang = state.get_lang_for(chat_id) if chat_id is not None else state.lang
    view = state if lang == state.lang else dataclass_replace_lang(state, lang)

    def amount_label(amt: Decimal) -> str:
        v = int(amt)
        # 万以下显示 $1k/$5k；万以上显示 $10k 这样的紧凑形式
        if v >= 1000:
            return f"${v // 1000}k"
        return f"${v}"

    def preset_row(prefix: str, head_emoji: str) -> List[Dict[str, str]]:
        cells = []
        for i, amt in enumerate(PRESET_AMOUNTS):
            label = amount_label(amt)
            text = f"{head_emoji} {label}" if i == 0 else label
            cells.append({"text": text, "callback_data": f"{prefix}:{int(amt)}"})
        cells.append({"text": t(view, "btn_custom"), "callback_data": f"custom:{prefix}"})
        return cells

    return {
        "inline_keyboard": [
            preset_row("match", "💵"),
            [
                {"text": t(view, "btn_lang_switch"), "callback_data": "lang:toggle"},
                {"text": t(view, "btn_summary"), "callback_data": "summary"},
                {"text": t(view, "btn_refresh"), "callback_data": "refresh"},
            ],
        ]
    }


# 自定义输入提示里的标记文字。_handle_message 通过 reply_to_message 检测到
# 这串后把后续消息当作金额处理，因此别随意改字面。
CUSTOM_PROMPT_MARKER = "请输入自定义"


def _custom_prompt_markup() -> Dict[str, Any]:
    return {
        "force_reply": True,
        "input_field_placeholder": "如 2500",
        "selective": True,
    }


def _persistent_keyboard() -> Dict[str, Any]:
    """聊天框底部常驻按钮。点一下发送「菜单」，bot 当 /menu 处理。"""
    return {
        "keyboard": [[{"text": "菜单"}, {"text": "状态"}]],
        "is_persistent": True,
        "resize_keyboard": True,
    }


# 持久键盘按钮文本到内部命令的映射
KEYBOARD_ALIASES = {
    "菜单": "/menu",
    "menu": "/menu",
    "Menu": "/menu",
    "状态": "/status",
    "status": "/status",
    "Status": "/status",
}


def _parse_amount(text: str) -> Optional[Decimal]:
    raw = text.strip().lstrip("$").replace(",", "").replace("_", "")
    if not raw:
        return None
    try:
        value = Decimal(raw)
    except InvalidOperation:
        return None
    if not (MIN_THRESHOLD_USDT <= value <= MAX_THRESHOLD_USDT):
        return None
    return value


class TelegramBot:
    """Telegram 命令/菜单处理。长轮询 getUpdates，运行期改 RuntimeState 的阈值。"""

    def __init__(
        self,
        cfg: Config,
        state: RuntimeState,
        tg: Telegram,
        client: httpx.AsyncClient,
        predict: "Predict",
    ) -> None:
        self.cfg = cfg
        self.state = state
        self.tg = tg
        self.client = client
        # /speed 和 /benchmark / /ramp 直接复用 predict.client 打 API + 读 _request_log
        self.predict = predict
        self.base = tg.base
        # 命令可以来自任何 chat（私聊 / 群 / 主告警频道）；回复就地。
        # 主告警频道（cfg.tg_chat_id）只用作告警目的地。
        self._main_chat_id = str(cfg.tg_chat_id)
        # 写入类命令（改阈值/测试推送/改摘要间隔）只有这些 user_id 能用。
        # 空集合 = 不限制（兼容老部署，但强烈建议设置 ALLOWED_USER_IDS）。
        self._admin_user_ids = cfg.allowed_user_ids

    def _is_admin(self, user_id: Any) -> bool:
        """是否允许执行写操作。空白名单时回退为"所有人都可"。"""
        if not self._admin_user_ids:
            return True
        try:
            return int(user_id) in self._admin_user_ids
        except (TypeError, ValueError):
            return False

    async def run(self, stop: asyncio.Event) -> None:
        LOG.info("Telegram 命令机器人启动，主告警 chat_id=%s（命令接受来自任意 chat）", self._main_chat_id)

        await self._prepare()

        # 从持久化的 runtime state 恢复 offset，避免重启后重新处理 24h 内的旧命令。
        offset = self.state.telegram_offset
        if offset:
            LOG.info("Telegram 长轮询从 offset=%d 续跑", offset)
        long_poll_timeout = 25
        # 长轮询要求 HTTP 超时大于 polling timeout
        http_timeout = httpx.Timeout(long_poll_timeout + 10)

        while not stop.is_set():
            try:
                resp = await self.client.get(
                    f"{self.base}/getUpdates",
                    params={
                        "timeout": long_poll_timeout,
                        "offset": offset,
                        "allowed_updates": json.dumps(["message", "callback_query"]),
                    },
                    timeout=http_timeout,
                )
            except Exception as exc:
                LOG.warning("Telegram getUpdates 网络异常: %s", exc)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=3)
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                payload = resp.json()
            except Exception:
                LOG.warning(
                    "Telegram getUpdates 返回非 JSON status=%s body=%s",
                    resp.status_code,
                    resp.text[:300],
                )
                await asyncio.sleep(3)
                continue

            if not payload.get("ok"):
                code = payload.get("error_code")
                desc = payload.get("description", "")
                if code == 409:
                    LOG.warning(
                        "Telegram getUpdates 冲突 409：另一个实例占着轮询，或 webhook 还在生效。"
                        "正在重新尝试 deleteWebhook。详情：%s",
                        desc,
                    )
                    await self._delete_webhook()
                elif code == 401:
                    LOG.error("Telegram token 无效（401），命令机器人退出。检查 TG_BOT_TOKEN。")
                    return
                else:
                    LOG.warning("Telegram getUpdates 错误 %s: %s", code, desc)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=3)
                except asyncio.TimeoutError:
                    pass
                continue

            updates = payload.get("result") or []
            for update in updates:
                offset = int(update.get("update_id", 0)) + 1
                try:
                    await self._handle_update(update)
                except Exception:
                    LOG.exception("处理 Telegram update 出错")

            # 处理完一批就把 offset 落盘（即便没有 update 也无所谓，文件 atomic write 很便宜）。
            if updates:
                self.state.telegram_offset = offset
                self.state.persist()

    async def _prepare(self) -> None:
        # webhook 和 getUpdates 互斥。bot 之前设过 webhook 会让 getUpdates 一直 409。
        await self._delete_webhook()

        # 在 Telegram 里登记命令，输入 / 时会有补全提示，新用户更容易发现菜单。
        try:
            resp = await self.client.post(
                f"{self.base}/setMyCommands",
                json={
                    "commands": [
                        {"command": "menu", "description": "打开监控菜单"},
                        {"command": "status", "description": "查看当前阈值"},
                        {"command": "summary", "description": "查看当前窗口摘要"},
                        {"command": "set_match", "description": "设置成交阈值 (USDT，DM 里改自己的)"},
                        {"command": "set_summary", "description": "设置自动摘要间隔（如 60 / 2h / 0）"},
                        {"command": "unsubscribe", "description": "退订私聊推送"},
                        {"command": "speed", "description": "查看当前 API 占用"},
                        {"command": "diag", "description": "全状态诊断快照"},
                        {"command": "benchmark", "description": "速度压测（admin）"},
                        {"command": "ramp", "description": "阶梯压测找最稳定 interval（admin）"},
                        {"command": "subscribers", "description": "列出订阅者（admin）"},
                        {"command": "whoami", "description": "查看自己的 user_id"},
                        {"command": "help", "description": "查看帮助"},
                    ]
                },
                timeout=httpx.Timeout(10),
            )
            if resp.status_code == 200 and resp.json().get("ok"):
                LOG.info("Telegram 命令列表已注册")
        except Exception as exc:
            LOG.warning("setMyCommands 失败（不影响功能）: %s", exc)

    async def _delete_webhook(self) -> None:
        try:
            resp = await self.client.post(
                f"{self.base}/deleteWebhook",
                json={"drop_pending_updates": False},
                timeout=httpx.Timeout(10),
            )
            if resp.status_code == 200 and resp.json().get("ok"):
                LOG.info("Telegram webhook 已清空（如有）")
        except Exception as exc:
            LOG.warning("deleteWebhook 失败（可忽略）: %s", exc)

    async def _handle_update(self, update: Dict[str, Any]) -> None:
        if "message" in update:
            await self._handle_message(update["message"])
        elif "callback_query" in update:
            await self._handle_callback(update["callback_query"])

    async def _handle_message(self, msg: Dict[str, Any]) -> None:
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        chat_type = chat.get("type")
        text = (msg.get("text") or "").strip()
        from_user = msg.get("from") or {}
        user_id = from_user.get("id")
        user_label = from_user.get("username") or from_user.get("first_name") or user_id

        is_admin = self._is_admin(user_id)
        is_main = self.state.is_main_chat(chat_id)
        # 命令在哪发的就回哪。私聊 → 私聊回复，群里 → 群里回复。
        src = chat_id

        # 私聊或非主频道首次发任何命令 → 自动注册订阅者，初始阈值 = 全局
        if not is_main and chat_type == "private" and (text.startswith("/") or text in KEYBOARD_ALIASES):
            self.state.upsert_subscriber(chat_id)
            self.state.persist()

        # 自定义阈值的 force_reply 回填
        reply_to = msg.get("reply_to_message")
        if (
            isinstance(reply_to, dict)
            and (reply_to.get("from") or {}).get("is_bot")
            and CUSTOM_PROMPT_MARKER in (reply_to.get("text") or "")
            and not text.startswith("/")
            and text not in KEYBOARD_ALIASES
        ):
            # 主频道：要 admin（改全局）；私聊：用户改自己的（无需 admin）
            if is_main and not is_admin:
                await self.tg.send("⛔ 仅管理员可改全局阈值。", chat_id=src)
                return
            await self._set_threshold(text, reply_chat_id=src, target_chat_id=chat_id)
            LOG.info(
                "%s 改了%s阈值 -> %s",
                "admin" if is_main else "subscriber", "全局" if is_main else "私聊", text,
            )
            return

        # 持久键盘按钮发回来的是纯文本（如「菜单」），转成对应命令处理。
        if text in KEYBOARD_ALIASES:
            text = KEYBOARD_ALIASES[text]

        if not text.startswith("/"):
            return

        LOG.info(
            "收到命令 chat=%s(type=%s%s) user=%s(id=%s) text=%r admin=%s",
            chat_id, chat_type, " main" if is_main else "",
            user_label, user_id, text[:80], is_admin,
        )

        head, _, tail = text.partition(" ")
        # 兼容 "/set_match@MyBot 1000"
        cmd = head.split("@", 1)[0].lower()
        arg = tail.strip()

        # 主频道里 /set_match / /set_summary / /test / /benchmark 需要 admin。
        # 私聊里 /set_match 是改"自己的"，不要 admin 检查。
        admin_only_cmds = {"/set_summary", "/benchmark", "/ramp"}
        main_chat_admin_cmds = {"/set_match"}
        if cmd in admin_only_cmds and not is_admin:
            await self.tg.send(
                f"⛔ 仅管理员可执行此命令。当前 user_id=<code>{user_id}</code>。",
                chat_id=src,
            )
            LOG.warning("非管理员尝试 %s: user=%s id=%s", cmd, user_label, user_id)
            return
        if cmd in main_chat_admin_cmds and is_main and not is_admin:
            await self.tg.send(
                f"⛔ 仅管理员可改全局阈值。私聊 bot 调你自己的就不需要管理员权限。",
                chat_id=src,
            )
            return

        if cmd == "/start":
            await self.tg.send(
                _menu_text(self.state, chat_id=chat_id),
                reply_markup=_persistent_keyboard(),
                chat_id=src,
            )
        elif cmd == "/menu":
            await self.tg.send(
                _menu_text(self.state, chat_id=chat_id),
                reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
                chat_id=src,
            )
        elif cmd == "/status":
            await self.tg.send(
                _menu_text(self.state, chat_id=chat_id),
                reply_markup=_persistent_keyboard(),
                chat_id=src,
            )
        elif cmd == "/whoami":
            scope = "全局（主告警频道）" if is_main else "私聊订阅"
            my_threshold = self.state.get_threshold_for(chat_id)
            await self.tg.send(
                f"你的 Telegram user_id：<code>{user_id}</code>\n"
                f"用户名：<code>{html.escape(str(user_label))}</code>\n"
                f"管理员权限：<b>{'✅ 是' if is_admin else '❌ 否'}</b>\n"
                f"当前 chat 类型：{scope}\n"
                f"chat 阈值：<b>${fmt_decimal(my_threshold, 2)} USDT</b>",
                chat_id=src,
            )
        elif cmd == "/lang":
            wanted = arg.strip().lower()
            new_lang: Optional[str] = None
            if wanted in {"zh", "cn", "chinese", "中文"}:
                new_lang = "zh"
            elif wanted in {"en", "english", "英文"}:
                new_lang = "en"
            if new_lang is None:
                cur = self.state.get_lang_for(chat_id)
                await self.tg.send(
                    f"Current language: <b>{cur}</b>\n"
                    "Usage: <code>/lang zh</code> or <code>/lang en</code>",
                    chat_id=src,
                )
            else:
                if is_main:
                    self.state.lang = new_lang
                else:
                    self.state.upsert_subscriber(chat_id, lang=new_lang)
                self.state.persist()
                # 用更新后的语言回应
                await self.tg.send(t(dataclass_replace_lang(self.state, new_lang), "lang_switched"), chat_id=src)
        elif cmd == "/set_match":
            await self._set_threshold(arg, reply_chat_id=src, target_chat_id=chat_id)
            LOG.info(
                "%s (id=%s) /set_match -> %s on chat %s",
                user_label, user_id, arg, chat_id,
            )
        elif cmd == "/unsubscribe":
            if is_main:
                await self.tg.send("主频道不可退订。", chat_id=src)
            elif self.state.remove_subscriber(chat_id):
                self.state.persist()
                await self.tg.send("✅ 已退订，不再向你私聊推送大单。", chat_id=src)
            else:
                await self.tg.send("你当前没有订阅记录。", chat_id=src)
        elif cmd == "/summary":
            await self._send_summary(reply_chat_id=src)
        elif cmd == "/set_summary":
            await self._handle_set_summary(arg, reply_chat_id=src)
        elif cmd == "/subscribers":
            # admin 才能查看订阅列表
            if not is_admin:
                await self.tg.send("⛔ 仅管理员", chat_id=src)
                return
            await self._send_subscriber_list(reply_chat_id=src)
        elif cmd == "/speed":
            await self._send_speed_status(reply_chat_id=src)
        elif cmd == "/diag":
            await self._send_diag(reply_chat_id=src, asking_chat_id=chat_id, is_admin=is_admin)
        elif cmd == "/benchmark":
            await self._run_benchmark(arg, reply_chat_id=src)
        elif cmd == "/ramp":
            await self._run_ramp(arg, reply_chat_id=src)
        elif cmd == "/help":
            await self.tg.send(
                t(self.state, "help_text"),
                reply_markup=_persistent_keyboard(),
                chat_id=src,
            )
        # /test 命令已废弃（按用户要求清理"测试功能"）。_send_test_alert 方法
        # 保留在代码里以备调试用，不再在命令路由里挂出来。

    async def _handle_callback(self, cb: Dict[str, Any]) -> None:
        cb_id = cb.get("id", "")
        chat_id = ((cb.get("message") or {}).get("chat") or {}).get("id")
        chat_type = ((cb.get("message") or {}).get("chat") or {}).get("type", "")
        from_user = cb.get("from") or {}
        user_id = from_user.get("id")
        user_label = from_user.get("username") or from_user.get("first_name") or user_id

        data = cb.get("data") or ""
        message_id = (cb.get("message") or {}).get("message_id")
        src = chat_id
        is_main = self.state.is_main_chat(chat_id)

        # 私聊里点按钮也算自动注册一次
        if not is_main and chat_type == "private":
            self.state.upsert_subscriber(chat_id)
            self.state.persist()

        # 刷新：只读，谁都能点
        if data == "refresh":
            await self.tg.answer_callback_query(cb_id, "✓")
            if message_id:
                await self.tg.edit_message(
                    message_id,
                    _menu_text(self.state, chat_id=chat_id),
                    reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
                    chat_id=src,
                )
            return

        # 摘要按钮：任何人可点；在源 chat 里直接发当前窗口摘要
        if data == "summary":
            await self.tg.answer_callback_query(cb_id, "✓")
            await self._send_summary(reply_chat_id=src)
            return

        # 切换语言：在 DM 里只翻 sub.lang；在主频道翻 state.lang
        if data == "lang:toggle":
            if is_main:
                self.state.lang = "en" if self.state.lang == "zh" else "zh"
                new_lang = self.state.lang
            else:
                cur = self.state.get_lang_for(chat_id)
                new_lang = "en" if cur == "zh" else "zh"
                self.state.upsert_subscriber(chat_id, lang=new_lang)
            self.state.persist()
            await self.tg.answer_callback_query(
                cb_id,
                t(dataclass_replace_lang(self.state, new_lang), "lang_switched"),
            )
            if message_id:
                await self.tg.edit_message(
                    message_id,
                    _menu_text(self.state, chat_id=chat_id),
                    reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
                    chat_id=src,
                )
            return

        # 测试推送：仅 admin
        if data == "test":
            if not self._is_admin(user_id):
                await self.tg.answer_callback_query(cb_id, "⛔ admin only")
                return
            await self.tg.answer_callback_query(cb_id, "✓")
            await self._send_test_alert(reply_chat_id=src)
            return

        # 其它写操作（改阈值）：主频道里要 admin；DM 里改自己的不要 admin
        if is_main and not self._is_admin(user_id):
            LOG.warning(
                "非管理员尝试改全局阈值（callback）user=%s id=%s data=%r",
                user_label, user_id, data,
            )
            await self.tg.answer_callback_query(cb_id, "⛔ 仅管理员可改全局阈值")
            return

        if data == "custom:match":
            await self.tg.answer_callback_query(cb_id, "输入成交阈值")
            await self.tg.send(
                f"💰 {CUSTOM_PROMPT_MARKER}成交阈值（USDT 数字，如 2500，最低 ${int(MIN_THRESHOLD_USDT)}）。\n"
                "回复这条消息即可，发送 /menu 取消。",
                reply_markup=_custom_prompt_markup(),
                chat_id=src,
            )
            return

        kind, _, raw_amount = data.partition(":")
        amount = _parse_amount(raw_amount)
        if kind != "match" or amount is None:
            await self.tg.answer_callback_query(cb_id, "无效操作")
            return

        # 主频道改全局；DM 改私有
        if is_main:
            self.state.threshold_usdt = amount
            self.state.persist()
            scope_label = "global"
        else:
            self.state.upsert_subscriber(chat_id, threshold_usdt=amount)
            self.state.persist()
            scope_label = "private"

        LOG.info(
            "callback %s -> %s by user=%s(id=%s) scope=%s",
            kind, amount, user_label, user_id, scope_label,
        )
        await self.tg.answer_callback_query(cb_id, f"✓ ${int(amount):,}")
        if message_id:
            await self.tg.edit_message(
                message_id,
                _menu_text(self.state, chat_id=chat_id),
                reply_markup=_menu_keyboard(self.state, chat_id=chat_id),
                chat_id=src,
            )

    async def _set_threshold(
        self,
        raw: str,
        *,
        reply_chat_id: Optional[Any] = None,
        target_chat_id: Optional[Any] = None,
    ) -> None:
        """改阈值。target_chat_id 决定改全局（主频道）还是私有（DM 订阅者）。"""
        amount = _parse_amount(raw)
        if amount is None:
            await self.tg.send(
                f"❌ 无效金额：<code>{html.escape(raw or '(空)')}</code>\n"
                f"范围 ${int(MIN_THRESHOLD_USDT)}–${int(MAX_THRESHOLD_USDT):,} USDT",
                chat_id=reply_chat_id,
            )
            return

        if target_chat_id is None or self.state.is_main_chat(target_chat_id):
            self.state.threshold_usdt = amount
            LOG.info("全局成交阈值更新为 %s USDT", amount)
        else:
            self.state.upsert_subscriber(target_chat_id, threshold_usdt=amount)
            LOG.info(
                "[subscribers] chat_id=%s 私有阈值更新为 %s USDT",
                target_chat_id, amount,
            )
        self.state.persist()

        await self.tg.send(
            _menu_text(self.state, chat_id=target_chat_id),
            reply_markup=_menu_keyboard(self.state, chat_id=target_chat_id),
            chat_id=reply_chat_id,
        )

    async def _apply_threshold(self, kind: str, amount: Decimal) -> None:
        """旧 API，保留做向后兼容。新代码请用 _set_threshold(target_chat_id=...)"""
        self.state.threshold_usdt = amount
        LOG.info("成交阈值更新为 %s USDT", amount)
        self.state.persist()

    async def _send_summary(self, *, reply_chat_id: Optional[Any] = None) -> None:
        """渲染当前窗口摘要并发到指定 chat（无指定 = 主告警频道）。"""
        # 用当前自动间隔做窗口；间隔 = 0（关闭）时仍按"过去 1 小时"快查
        window = self.state.summary_interval_sec or 3600
        summary = self.state.summarize_window(window)
        if not summary:
            label = _format_duration_label(window, self.state.lang)
            await self.tg.send(
                t(self.state, "summary_none_fmt").format(label=label),
                chat_id=reply_chat_id,
            )
            return
        text = format_summary_alert(summary, self.cfg, self.state)
        await self.tg.send(text, chat_id=reply_chat_id)

    async def _handle_set_summary(
        self, raw: str, *, reply_chat_id: Optional[Any] = None
    ) -> None:
        """改自动摘要间隔；admin 已由调用方校验。"""
        if not raw:
            await self.tg.send(t(self.state, "summary_set_help"), chat_id=reply_chat_id)
            return
        secs = _parse_duration(raw)
        if secs is None:
            await self.tg.send(
                t(self.state, "summary_set_invalid") + "\n" + t(self.state, "summary_set_help"),
                chat_id=reply_chat_id,
            )
            return
        self.state.summary_interval_sec = secs
        self.state.persist()
        if secs == 0:
            await self.tg.send(t(self.state, "summary_set_off"), chat_id=reply_chat_id)
        else:
            label = _format_duration_label(secs, self.state.lang)
            await self.tg.send(
                t(self.state, "summary_set_ok_fmt").format(label=label),
                chat_id=reply_chat_id,
            )

    async def _send_subscriber_list(self, *, reply_chat_id: Optional[Any] = None) -> None:
        """admin only：列出所有订阅者及其阈值。"""
        subs = self.state.subscribers
        if not subs:
            await self.tg.send("(无订阅者)", chat_id=reply_chat_id)
            return
        lines = [f"<b>订阅者列表（{len(subs)}）</b>"]
        for cid, info in sorted(subs.items(), key=lambda kv: kv[0]):
            try:
                amt = Decimal(str(info.get("threshold_usdt", "0")))
            except (InvalidOperation, ValueError, TypeError):
                amt = Decimal("0")
            lines.append(
                f"<code>{html.escape(cid)}</code> · "
                f"${fmt_decimal(amt, 0)} · "
                f"{info.get('lang', 'zh')} · "
                f"last_seen=<code>{html.escape(str(info.get('last_seen', '?'))[:19])}</code>"
            )
        await self.tg.send("\n".join(lines), chat_id=reply_chat_id)

    async def _send_speed_status(self, *, reply_chat_id: Optional[Any] = None) -> None:
        """读 Predict._request_log 显示当前 60s API 占用。任何人都能用。"""
        import time as _time
        now = _time.monotonic()
        recent = [t for t in self.predict._request_log if t >= now - 60]
        count = len(recent)
        pct = count * 100 // Predict.RATE_LIMIT_PER_MIN
        bar_len = min(20, pct // 5)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        msg = (
            "📊 <b>API 速度状态</b>\n\n"
            f"最近 60s 调用：<b>{count}</b> 次\n"
            f"限速预算：<b>{Predict.RATE_LIMIT_PER_MIN}/min</b>\n"
            f"占用：<b>{pct}%</b>\n"
            f"<code>{bar}</code>\n\n"
            f"当前 POLL_INTERVAL_SEC = <code>{self.cfg.poll_interval_sec}s</code>"
        )
        await self.tg.send(msg, chat_id=reply_chat_id)

    async def _send_diag(
        self,
        *,
        reply_chat_id: Optional[Any] = None,
        asking_chat_id: Optional[Any] = None,
        is_admin: bool = False,
    ) -> None:
        """全状态快照。任何人都能用，但只有 admin 能看到所有订阅者明细。"""
        import time as _time

        is_main = self.state.is_main_chat(asking_chat_id) if asking_chat_id is not None else False
        my_threshold = (
            self.state.threshold_usdt if is_main
            else self.state.get_threshold_for(asking_chat_id) if asking_chat_id is not None
            else self.state.threshold_usdt
        )

        # 持久化文件状态
        def _file_status(path: str) -> str:
            if not path:
                return "(未配置)"
            if not os.path.exists(path):
                return f"<code>{html.escape(path)}</code> ❌ 不存在（Railway volume 没挂！）"
            try:
                size = os.path.getsize(path)
                return f"<code>{html.escape(path)}</code> ✅ {size:,} bytes"
            except Exception:
                return f"<code>{html.escape(path)}</code> ⚠️ 无法读"

        # API 占用
        now = _time.monotonic()
        recent = [t for t in self.predict._request_log if t >= now - 60]
        api_count = len(recent)
        api_pct = api_count * 100 // Predict.RATE_LIMIT_PER_MIN

        # 上一轮 monitor_matches 统计
        last = self.state.last_iter_stats
        last_iter = "(尚未跑过一轮)"
        if last:
            last_iter = (
                f"fetched=<b>{last.get('fetched', '?')}</b> · "
                f"filtered=<b>{last.get('filtered', '?')}</b>"
                f"(min_th=$<b>{last.get('min_threshold', '?')}</b>) · "
                f"fresh=<b>{last.get('fresh', '?')}</b>\n"
                f"→ main=<b>{last.get('sent_main', '?')}</b> · "
                f"subs=<b>{last.get('sent_subs', '?')}</b> · "
                f"@ <code>{html.escape(str(last.get('ts', '?'))[:19])}</code>"
            )

        # 最近一笔被推送的成交（从 recent_matches 取最后一条）
        last_alert = "(本会话尚无)"
        if self.state.recent_matches:
            m = self.state.recent_matches[-1]
            ago_secs = (datetime.now(timezone.utc) - m["ts"]).total_seconds()
            if ago_secs < 60:
                ago = f"{int(ago_secs)}秒前"
            elif ago_secs < 3600:
                ago = f"{int(ago_secs / 60)}分钟前"
            else:
                ago = f"{int(ago_secs / 3600)}小时前"
            last_alert = f"${fmt_decimal(m['value'], 2)} · {normalize_text(m['title'])[:40]} · {ago}"

        # 订阅者列表
        sub_count = len(self.state.subscribers)
        if is_admin:
            if sub_count == 0:
                sub_lines = "(无)"
            else:
                rows = []
                for cid, info in sorted(self.state.subscribers.items()):
                    try:
                        amt = Decimal(str(info.get("threshold_usdt", "0")))
                    except Exception:
                        amt = Decimal("0")
                    rows.append(
                        f"  • <code>{html.escape(cid)}</code> · ${fmt_decimal(amt, 0)} · "
                        f"{info.get('lang', '?')}"
                    )
                sub_lines = "\n".join(rows[:10])
                if len(rows) > 10:
                    sub_lines += f"\n  …还有 {len(rows) - 10} 个"
        else:
            # 非 admin：只暴露数量 + 自己的 entry
            sub_lines = "(完整列表仅 admin 可见)"
            if asking_chat_id is not None:
                my_info = self.state.subscribers.get(str(asking_chat_id))
                if my_info:
                    try:
                        amt = Decimal(str(my_info.get("threshold_usdt", "0")))
                    except Exception:
                        amt = Decimal("0")
                    sub_lines += (
                        f"\n  你：${fmt_decimal(amt, 0)} · "
                        f"{my_info.get('lang', '?')}"
                    )

        scope_label = (
            "主告警频道（你看到的是全局值）" if is_main
            else "私聊订阅模式（你看到的是你自己的）"
        )

        msg = (
            "🔧 <b>当前状态诊断</b>\n\n"
            f"<b>本 chat 视角</b>：{scope_label}\n"
            f"  你的有效阈值：<b>${fmt_decimal(my_threshold, 2)} USDT</b>\n\n"
            f"<b>全局配置</b>\n"
            f"  主告警 chat_id：<code>{html.escape(self.state.main_chat_id)}</code>\n"
            f"  全局阈值：<b>${fmt_decimal(self.state.threshold_usdt, 2)} USDT</b>\n"
            f"  POLL_INTERVAL_SEC：<code>{self.cfg.poll_interval_sec}s</code>\n"
            f"  默认语言：<code>{self.state.lang}</code>\n\n"
            f"<b>持久化文件</b>\n"
            f"  runtime_state：{_file_status(self.cfg.runtime_state_path)}\n"
            f"  seen 事件：{_file_status(self.cfg.seen_state_path)}\n"
            f"  seen 市场：{_file_status(self.cfg.seen_markets_path)}\n\n"
            f"<b>订阅者：{sub_count} 个</b>\n{sub_lines}\n\n"
            f"<b>监控状态</b>\n"
            f"  最近 60s API：<b>{api_count}</b> 次（{api_pct}% 预算）\n"
            f"  最后一轮 monitor_matches：{last_iter}\n"
            f"  最后一笔已推送告警：{last_alert}\n"
            f"  内存 seen：<b>{len(self.state.recent_matches)}</b> 笔窗口缓存\n"
        )

        await self.tg.send(msg, chat_id=reply_chat_id)

    async def _benchmark_loop(
        self, interval: float, duration: float, *, label: str = "",
    ) -> Dict[str, Any]:
        """裸打 GET /v1/markets 的循环。返回 counts + RTT 分位数。供 /benchmark 和 /ramp 共用。"""
        import time as _time
        url = f"{self.cfg.predict_api_base}/v1/markets"
        headers = self.predict.headers
        params = {"first": 1}

        end = _time.monotonic() + duration
        counts = {"200": 0, "429": 0, "5xx": 0, "4xx": 0, "err": 0}
        timings: List[float] = []
        iter_n = 0
        last_log = _time.monotonic()
        prefix = f"[bench{label}] " if label else "[bench] "

        LOG.info("%s开始 interval=%.2fs duration=%.0fs", prefix, interval, duration)

        while _time.monotonic() < end:
            iter_n += 1
            t0 = _time.monotonic()
            try:
                resp = await asyncio.wait_for(
                    self.predict.client.get(
                        url, params=params, headers=headers,
                        timeout=httpx.Timeout(5),
                    ),
                    timeout=8.0,
                )
                rtt = (_time.monotonic() - t0) * 1000
                timings.append(rtt)
                if resp.status_code == 200:
                    counts["200"] += 1
                elif resp.status_code == 429:
                    counts["429"] += 1
                elif 500 <= resp.status_code < 600:
                    counts["5xx"] += 1
                else:
                    counts["4xx"] += 1
            except asyncio.TimeoutError:
                counts["err"] += 1
                LOG.warning("%siter %d 硬超时 8s", prefix, iter_n)
            except Exception as exc:
                counts["err"] += 1
                LOG.warning("%siter %d 异常: %s", prefix, iter_n, exc)

            if _time.monotonic() - last_log >= 5.0:
                last_log = _time.monotonic()
                elapsed_s = duration - (end - _time.monotonic())
                LOG.info(
                    "%s进度 %.0f/%.0fs · iter=%d · 200=%d 429=%d err=%d",
                    prefix, elapsed_s, duration, iter_n,
                    counts["200"], counts["429"], counts["err"],
                )

            elapsed = _time.monotonic() - t0
            sleep_for = max(0, interval - elapsed)
            try:
                await asyncio.sleep(sleep_for)
            except asyncio.CancelledError:
                LOG.info("%s被取消", prefix)
                break

        LOG.info(
            "%s完成 iter=%d 200=%d 429=%d 5xx=%d 4xx=%d err=%d",
            prefix, iter_n,
            counts["200"], counts["429"],
            counts["5xx"], counts["4xx"], counts["err"],
        )

        result: Dict[str, Any] = {"counts": counts, "iter": iter_n}
        if timings:
            timings.sort()
            result["rtt_min"] = timings[0]
            result["rtt_med"] = timings[len(timings) // 2]
            result["rtt_p95"] = timings[max(0, int(len(timings) * 0.95) - 1)]
            result["rtt_max"] = timings[-1]
        return result

    async def _run_benchmark(self, raw: str, *, reply_chat_id: Optional[Any] = None) -> None:
        """admin only。/benchmark [interval] [duration] — 单档速度测试。"""
        parts = (raw or "").split()
        try:
            interval = float(parts[0]) if len(parts) >= 1 and parts[0] else 0.2
            duration = float(parts[1]) if len(parts) >= 2 and parts[1] else 15.0
        except ValueError:
            await self.tg.send(
                "用法：<code>/benchmark [interval_sec] [duration_sec]</code>\n"
                "例：<code>/benchmark 0.2 15</code>（默认值）\n"
                "限制：interval ≥ 0.05，duration ≤ 60",
                chat_id=reply_chat_id,
            )
            return
        if interval < 0.05 or interval > 5 or duration < 1 or duration > 60:
            await self.tg.send(
                "❌ 参数超界。interval 0.05–5s，duration 1–60s。",
                chat_id=reply_chat_id,
            )
            return

        await self.tg.send(
            f"⏱ 速度测试运行中…\n"
            f"间隔 {interval}s · 时长 {duration}s · 预计 {int(duration / interval)} 次请求",
            chat_id=reply_chat_id,
        )

        bench_error: Optional[str] = None
        try:
            result = await self._benchmark_loop(interval, duration)
        except Exception as exc:
            LOG.exception("[benchmark] 整体异常")
            bench_error = str(exc)
            result = {"counts": {"200": 0, "429": 0, "5xx": 0, "4xx": 0, "err": 0}}

        counts = result["counts"]
        total = sum(counts.values())
        pct429 = (counts["429"] / total * 100) if total else 0
        if total and "rtt_med" in result:
            rtt_line = (
                f"min {result['rtt_min']:.0f}ms · 中位 {result['rtt_med']:.0f}ms · "
                f"p95 {result['rtt_p95']:.0f}ms · max {result['rtt_max']:.0f}ms"
            )
        else:
            rtt_line = "(无成功响应)"

        # 建议
        if total == 0:
            suggestion = "❌ 一次请求都没成功。检查 PREDICT_API_BASE / API key / 网络。"
        elif pct429 < 1:
            suggestion = (
                f"✅ 当前间隔 <b>{interval}s</b> 表现良好（429 < 1%），"
                f"可以把 <code>POLL_INTERVAL_SEC</code> 设到 <code>{interval}</code>。"
            )
        elif pct429 < 5:
            suggestion = (
                f"⚠️ 间隔 <b>{interval}s</b> 触发 {pct429:.1f}% 429。可用但偏紧；"
                f"稳健选 <code>POLL_INTERVAL_SEC={interval * 1.5:.2f}</code>。"
            )
        else:
            suggestion = (
                f"❌ 间隔 <b>{interval}s</b> 触发 {pct429:.1f}% 429，过激进。"
                f"建议 <code>POLL_INTERVAL_SEC={max(interval * 2, 0.4):.2f}</code> 起步。"
            )

        report = (
            "🏎 <b>速度测试结果</b>\n\n"
            f"间隔 <code>{interval}s</code> · 时长 <code>{duration}s</code> · 总计 <b>{total}</b> 次\n\n"
            f"✓ 200: <b>{counts['200']}</b> ({(counts['200'] / total * 100) if total else 0:.1f}%)\n"
            f"⚠️ 429: <b>{counts['429']}</b> ({pct429:.1f}%)\n"
        )
        if counts["5xx"]:
            report += f"⚠️ 5xx: {counts['5xx']}\n"
        if counts["4xx"]:
            report += f"❓ 4xx: {counts['4xx']}\n"
        if counts["err"]:
            report += f"❌ 网络/超时: {counts['err']}\n"
        report += f"\n响应：{rtt_line}\n\n{suggestion}"
        if bench_error:
            report += f"\n\n⚠️ 测试中途异常：<code>{html.escape(bench_error)}</code>"

        try:
            await self.tg.send(report, chat_id=reply_chat_id)
            LOG.info("[benchmark] 报告已发出")
        except Exception:
            LOG.exception("[benchmark] 发送报告失败")
            # 兜底：纯文本简化版
            try:
                fallback = (
                    f"benchmark done: total={total} 200={counts['200']} "
                    f"429={counts['429']} err={counts['err']}"
                )
                await self.tg.send(fallback, chat_id=reply_chat_id)
            except Exception:
                LOG.exception("[benchmark] 兜底报告也失败")

    async def _run_ramp(self, raw: str, *, reply_chat_id: Optional[Any] = None) -> None:
        """admin only。阶梯压测：逐档加速找最稳定 / 最快的 interval。

        默认序列：3.2s → 1.6s → 0.8s → 0.4s → 0.2s（每档 10s，连续两档 >5% 429 提前停）

        - <code>/ramp</code>                       默认序列
        - <code>/ramp 0.8,0.4,0.2 8</code>         自定义档位 + 每档秒数
        - <code>/ramp 3.2 5 10</code>              start / 步数 / 每步秒数（速率翻倍）
        """
        parts = (raw or "").split()
        intervals: List[float] = []
        duration_per_step = 10.0

        if len(parts) >= 1 and "," in parts[0]:
            try:
                intervals = [float(x.strip()) for x in parts[0].split(",") if x.strip()]
            except ValueError:
                intervals = []
            if len(parts) >= 2:
                try:
                    duration_per_step = float(parts[1])
                except ValueError:
                    pass
        elif len(parts) >= 1:
            try:
                start = float(parts[0])
                steps = int(parts[1]) if len(parts) >= 2 else 5
                if len(parts) >= 3:
                    duration_per_step = float(parts[2])
                intervals = [start / (2 ** i) for i in range(steps)]
            except ValueError:
                intervals = []
        else:
            intervals = [3.2 / (2 ** i) for i in range(5)]  # 默认 3.2→0.2

        intervals = [round(i, 3) for i in intervals if 0.05 <= i <= 10]
        if not intervals or len(intervals) > 8:
            await self.tg.send(
                "用法：\n"
                "<code>/ramp</code>  默认 3.2→1.6→0.8→0.4→0.2，每档 10s\n"
                "<code>/ramp 0.8,0.4,0.2 8</code>  自定义档位 + 每档秒数\n"
                "<code>/ramp 3.2 5 10</code>  start / 步数 / 每步秒数（速率翻倍）\n"
                "限制：每档间隔 0.05–10s，最多 8 档，每档 3–30s",
                chat_id=reply_chat_id,
            )
            return
        if not (3 <= duration_per_step <= 30):
            await self.tg.send("❌ 每档时长 3–30s。", chat_id=reply_chat_id)
            return

        seq_str = " → ".join(f"{i:g}s" for i in intervals)
        total_eta = int(len(intervals) * duration_per_step)
        await self.tg.send(
            f"📈 <b>速率阶梯测试</b>\n\n"
            f"序列：<code>{seq_str}</code>\n"
            f"每档：<code>{duration_per_step:.0f}s</code> · 预计总时长：~<code>{total_eta}s</code>\n"
            f"自动停：连续两档 429 &gt; 5%\n\n"
            f"开始…",
            chat_id=reply_chat_id,
        )

        rows: List[Dict[str, Any]] = []
        consecutive_bad = 0

        for i, interval in enumerate(intervals, 1):
            try:
                result = await self._benchmark_loop(
                    interval, duration_per_step, label=f"-step{i}",
                )
            except Exception:
                LOG.exception("[ramp] step %d 异常", i)
                await self.tg.send(
                    f"⚠️ 第 {i} 档（{interval:g}s）异常，跳过",
                    chat_id=reply_chat_id,
                )
                continue

            counts = result["counts"]
            total = sum(counts.values())
            pct429 = (counts["429"] / total * 100) if total else 0
            err_pct = (counts["err"] / total * 100) if total else 0
            rtt_med = result.get("rtt_med", 0)
            rtt_p95 = result.get("rtt_p95", 0)
            rtt_jitter = (rtt_p95 - rtt_med) if rtt_p95 and rtt_med else 0

            row = {
                "interval": interval,
                "total": total,
                "ok": counts["200"],
                "limited": counts["429"],
                "err": counts["err"],
                "pct429": pct429,
                "rtt_med": rtt_med,
                "rtt_p95": rtt_p95,
                "rtt_jitter": rtt_jitter,
            }
            rows.append(row)

            emoji = "✅" if pct429 < 1 and err_pct == 0 else "⚠️" if pct429 < 5 else "❌"
            await self.tg.send(
                f"{emoji} <b>第 {i}/{len(intervals)} 档</b> · 间隔 <code>{interval:g}s</code>\n"
                f"总数 {total} · 200={counts['200']} · "
                f"429={counts['429']} ({pct429:.1f}%) · err={counts['err']}\n"
                f"中位 {rtt_med:.0f}ms · p95 {rtt_p95:.0f}ms",
                chat_id=reply_chat_id,
            )

            if pct429 > 5:
                consecutive_bad += 1
            else:
                consecutive_bad = 0
            if consecutive_bad >= 2:
                await self.tg.send(
                    f"🛑 连续两档 429 &gt; 5%，提前停止（已测 {i}/{len(intervals)} 档）",
                    chat_id=reply_chat_id,
                )
                break

        if not rows:
            await self.tg.send("❌ 所有档位都失败了。", chat_id=reply_chat_id)
            return

        # 综合评分：429 < 1% 且 err = 0 的"稳定档位"
        stable = [r for r in rows if r["pct429"] < 1 and r["err"] == 0]
        # "最稳定" = 在稳定档位里 RTT 抖动最小（p95 与中位差距最小）
        most_stable = min(stable, key=lambda r: r["rtt_jitter"]) if stable else None
        # "最快稳定" = 稳定档位里 interval 最小（速率最高）
        fastest_safe = min(stable, key=lambda r: r["interval"]) if stable else None

        # 用 <pre> 等宽对齐
        summary = "📊 <b>阶梯测试总结</b>\n\n<pre>"
        summary += " 间隔 |总数|200|429| %429 |中位 |p95\n"
        summary += "------+----+---+---+------+-----+----\n"
        for r in rows:
            mark = ""
            if r is most_stable:
                mark = " ⭐"
            elif r is fastest_safe:
                mark = " 🏎"
            summary += (
                f"{r['interval']:5g}s|{r['total']:4d}|{r['ok']:3d}|{r['limited']:3d}|"
                f"{r['pct429']:5.1f}%|{r['rtt_med']:4.0f}ms|{r['rtt_p95']:4.0f}ms{mark}\n"
            )
        summary += "</pre>\n\n"

        if most_stable:
            summary += (
                f"⭐ <b>最稳定</b>：<code>{most_stable['interval']:g}s</code>"
                f"（429={most_stable['limited']}，p95-中位 = {most_stable['rtt_jitter']:.0f}ms）\n"
            )
        if fastest_safe and fastest_safe is not most_stable:
            summary += (
                f"🏎 <b>最快稳定</b>：<code>{fastest_safe['interval']:g}s</code>"
                f"（429 &lt; 1%，速率最高的安全档）\n"
            )

        if fastest_safe:
            summary += (
                f"\n<b>建议：</b><code>POLL_INTERVAL_SEC={fastest_safe['interval']:g}</code>"
            )
        elif rows:
            min_429 = min(rows, key=lambda r: r["pct429"])
            summary += (
                f"\n⚠️ 没找到 429 &lt; 1% 的档位。"
                f"最低 429 是 <b>{min_429['interval']:g}s</b>（{min_429['pct429']:.1f}%），"
                f"建议 <code>POLL_INTERVAL_SEC={min_429['interval'] * 1.5:.2f}</code>。"
            )

        try:
            await self.tg.send(summary, chat_id=reply_chat_id)
        except Exception:
            LOG.exception("[ramp] 发送总结失败")
            try:
                await self.tg.send(
                    f"ramp done: tested {len(rows)} steps, "
                    f"recommended POLL_INTERVAL_SEC="
                    f"{fastest_safe['interval'] if fastest_safe else 'N/A'}",
                    chat_id=reply_chat_id,
                )
            except Exception:
                LOG.exception("[ramp] 兜底总结也失败")

    async def _send_test_alert(self, *, reply_chat_id: Optional[Any] = None) -> None:
        """构造一笔假成交，按当前阈值/语言渲染一遍。用于验证消息样式 + 通道。
        故意填上 makers 多人、手续费、刚刚的 executedAt、cumulative=3，让 🧪 一键
        看到所有增强字段的样子。不调 bump_cumulative，避免污染真实计数器。"""
        # 用当前 match 阈值 + 0.5 价格凑整（threshold/$0.5 = 份额）
        sample_notional = self.state.threshold_usdt
        sample_price = Decimal("0.5")  # 50¢
        sample_shares = sample_notional / sample_price if sample_price > 0 else Decimal("100")

        scale_shares = Decimal(10) ** self.cfg.shares_wei_decimals
        scale_usdt = Decimal(10) ** self.cfg.usdt_wei_decimals

        # 用 takerAssetId=0 + takerAmountFilled 构造，配合 Phase 1 新解码路径
        fake_event = {
            "transactionHash": "0x" + "ab" * 32,
            "executedAt": datetime.now(timezone.utc).isoformat(),
            "amountFilled": str(int((sample_shares * scale_shares).to_integral_value(rounding=ROUND_DOWN))),
            "takerAmountFilled": str(int((sample_notional * scale_usdt).to_integral_value(rounding=ROUND_DOWN))),
            "takerAssetId": "0",
            "makerAssetId": "12345",
            "market": {
                "id": 0,
                "title": "Test Market",
                "slug": "predict-bot-test-event",
            },
            "taker": {
                "signer": "0x1234567890abcdef1234567890abcdef12345678",
                "username": "predict_bot_test",
                "outcome": {"name": "Yes"},
                "quoteType": "Bid",
                "fee": {
                    "amount": str(int(
                        (sample_notional * Decimal("0.001") * scale_usdt).to_integral_value(rounding=ROUND_DOWN)
                    )),
                },
            },
            # 演示对手方计数（>1 才会渲染）
            "makers": [
                {"signer": "0xa" * 40},
                {"signer": "0xb" * 40},
                {"signer": "0xc" * 40},
            ],
        }

        # 演示"本轮第 N 笔"行（不通过 bump_cumulative，避免污染真实计数器）
        text, markup = format_match_alert(fake_event, self.cfg, self.state, cumulative=3)
        # 加个"测试推送"前缀，让用户分清这不是真单
        text = f"{t(self.state, 'test_caption')}\n\n{text}"
        await self.tg.send(text, reply_markup=markup, chat_id=reply_chat_id)


def event_value_usdt(event: Dict[str, Any], cfg: Config) -> Decimal:
    """
    解出本笔成交的 USDT 总额。优先级：
    (1) API 直接给的 valueUsdt / notionalUsdt / valueUsdtWei 等字段
    (2) 链上 OrderFilled 同名字段：takerAmountFilled（当 takerAssetId=0）
        或 makerAmountFilled（当 makerAssetId=0）— 0 在 Predict 的 ConditionalTokens
        语义里是 collateral（USDT）
    (3) 其它直接的 USDT 金额字段：collateralAmount / usdtAmount / usdAmount
    (4) 都没有 → 返回 0 + 写 warning，让 alert 显示 "-"，**禁止再用
        amountFilled × priceExecuted 兜底**：实测 priceExecuted 不是 per-share
        价格，会算出离谱的天文数字。
    """

    # (1) 顶层显式 USDT 字段
    for key in (
        "valueUsdt", "valueUSDT", "valueUsdtWei",
        "notionalUsdt", "notionalUsdtWei",
        "totalValueUsdt", "totalValueUsdtWei",
    ):
        if event.get(key) is not None:
            return to_decimal(event.get(key), cfg.usdt_wei_decimals)

    # (1') taker 嵌套的 USDT 字段
    taker = event.get("taker") if isinstance(event.get("taker"), dict) else {}
    for key in (
        "valueUsdt", "valueUSDT", "valueUsdtWei",
        "notionalUsdt", "notionalUsdtWei",
    ):
        if taker.get(key) is not None:
            return to_decimal(taker.get(key), cfg.usdt_wei_decimals)

    # (2) 链上 OrderFilled 风格字段
    # taker_asset_id == 0 → takerAmountFilled 就是 USDT
    # 与之配对：当 makerAssetId == 0，makerAmountFilled 才是 USDT。
    def _maybe_int(v: Any) -> Optional[int]:
        try:
            return int(str(v))
        except (TypeError, ValueError):
            return None

    taker_asset_id = _maybe_int(event.get("takerAssetId"))
    maker_asset_id = _maybe_int(event.get("makerAssetId"))
    if taker_asset_id == 0 and event.get("takerAmountFilled") is not None:
        return to_decimal(event.get("takerAmountFilled"), cfg.usdt_wei_decimals)
    if maker_asset_id == 0 and event.get("makerAmountFilled") is not None:
        return to_decimal(event.get("makerAmountFilled"), cfg.usdt_wei_decimals)

    # (3) 其它直接 USDT 金额字段
    for key in ("collateralAmount", "collateralAmountFilled", "usdtAmount", "usdAmount"):
        if event.get(key) is not None:
            return to_decimal(event.get(key), cfg.usdt_wei_decimals)

    # (4) 兜底：amount × priceExecuted。曾经因为某些市场算出 6.88e16 的天文数字
    # 被禁用，但禁用后没有 valueUsdt / takerAmountFilled 的事件会被静默过滤掉，
    # 整个 bot 沉默。这里加一道"合理范围"护栏：
    #  - 单笔 < 1B USDT 才用兜底（trump-china 那种 6.88e16 会被拦下）
    #  - 用了兜底就 LOG 一条，方便排查
    amount = to_decimal(event.get("amountFilled") or taker.get("amount"), cfg.shares_wei_decimals)
    price = to_decimal(event.get("priceExecuted") or taker.get("price"), cfg.usdt_wei_decimals)
    if amount > 0 and price > 0:
        fallback_notional = amount * price
        if 0 < fallback_notional < Decimal("1000000000"):
            LOG.info(
                "[notional fallback] %s × %s = $%s (event keys=%s)",
                amount, price, fallback_notional, sorted(event.keys()),
            )
            return fallback_notional
        else:
            LOG.warning(
                "[notional rejected] amount × price = %s 超出合理范围（>1B），"
                "可能是 priceExecuted 编码异常。event keys=%s",
                fallback_notional, sorted(event.keys()),
            )

    # 真正都拿不到才返回 0；附完整字段名便于定位
    LOG.warning(
        "[notional=0] event keys=%s taker keys=%s — alert 会被过滤掉，"
        "请把这些字段名贴出来调整 event_value_usdt",
        sorted(event.keys()),
        sorted(taker.keys()) if taker else [],
    )
    return Decimal("0")


def to_usdt_strict(value: Any, decimals: int) -> Decimal:
    """
    严格模式 wei→Decimal：整数永远缩放（不依赖大小启发式）。
    适合"我已经知道这字段是 wei 编码 USDT"的场景，比如手续费（小到 1e15 wei）。
    `to_decimal` 的 wei_hint 启发式对小金额会失灵。
    """
    if value is None:
        return Decimal("0")
    s = str(value).strip().replace(",", "")
    if not s or s.lower() in {"none", "null", "nan"}:
        return Decimal("0")
    if s.startswith("$"):
        s = s[1:]
    try:
        out = Decimal(s)
    except InvalidOperation:
        return Decimal("0")
    # 没有小数点 → 整数 wei，无条件按 decimals 缩放
    if "." not in s:
        out = out / (Decimal(10) ** decimals)
    return out


def event_fee_usdt(event: Dict[str, Any], cfg: Config) -> Decimal:
    """从 taker.fee.amount / event.fee.amount 抓出手续费（USDT）。找不到返回 0。"""
    taker = event.get("taker") if isinstance(event.get("taker"), dict) else {}
    candidates = (
        (taker.get("fee") or {}).get("amount") if isinstance(taker.get("fee"), dict) else None,
        (event.get("fee") or {}).get("amount") if isinstance(event.get("fee"), dict) else None,
        taker.get("feeAmount"),
        event.get("feeAmount"),
    )
    for raw in candidates:
        if raw is not None:
            # 手续费经常是 1e15 量级（$0.001~$0.01），到不了 to_decimal 的 wei_hint
            # 阈值（1e16），所以用 strict 模式避免显示成 1377600000000000 这种数。
            return to_usdt_strict(raw, cfg.usdt_wei_decimals)
    return Decimal("0")


def format_time_ago(state: "RuntimeState", iso_ts: Optional[str]) -> str:
    """ISO 时间戳 → 'X 秒前 / X 分钟前 / X 小时前'。差超过 24h 或异常返回 ''。"""
    if not iso_ts:
        return ""
    try:
        s = str(iso_ts).strip().replace("Z", "+00:00")
        ts = datetime.fromisoformat(s)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except Exception:
        return ""
    delta = datetime.now(timezone.utc) - ts
    secs = int(delta.total_seconds())
    if secs < 0:
        # 时钟漂移，clamp 成"刚刚"
        return t(state, "time_just_now")
    if secs < 5:
        return t(state, "time_just_now")
    if secs < 60:
        return t(state, "time_ago_s_fmt").format(n=secs)
    if secs < 3600:
        return t(state, "time_ago_m_fmt").format(n=secs // 60)
    if secs < 86400:
        return t(state, "time_ago_h_fmt").format(n=secs // 3600)
    return ""  # > 24h，看着像坏的，不显示


def event_price_usdt(event: Dict[str, Any], notional: Decimal, amount: Decimal) -> Decimal:
    """
    解每股成交价：notional / amount。notional 不可用时返回 0 让 alert 显示 "-"。
    不再退回 priceExecuted —— 它在某些市场是限价单的原始 limit price 编码而不是
    per-share 价格，会算出离谱数（e.g. 200,000,000,000,000,000¢）。
    """
    if notional > 0 and amount > 0:
        return notional / amount
    return Decimal("0")


def _render_template(template: str, **kwargs: Any) -> str:
    """
    安全的字符串模板渲染：
    - 模板里出现的占位符必须有非空值，否则返回空串（避免拼出 .../event/ 这种半截链接）
    - 异常一律返回空串
    """
    if not template:
        return ""
    for key, value in kwargs.items():
        if ("{" + key + "}") in template and not str(value):
            return ""
    try:
        return template.format(**{k: str(v) for k, v in kwargs.items()})
    except Exception:
        return ""


def whale_tier(notional: Decimal, state: "RuntimeState") -> Tuple[str, str]:
    """
    根据成交价值返回 (emoji, 等级标签)。
      🟣 $100k+   超级鲸鱼单
      🔴 $20k+    大鲸鱼单
      🟠 $5k+     中型鲸鱼单
      🟡 $1k+     普通大单
      🟢 < $1k    小单（用户阈值低于 $1k 时也能用）
    """
    if notional >= Decimal("100000"):
        return "🟣", t(state, "tier_super")
    if notional >= Decimal("20000"):
        return "🔴", t(state, "tier_big")
    if notional >= Decimal("5000"):
        return "🟠", t(state, "tier_mid")
    if notional >= Decimal("1000"):
        return "🟡", t(state, "tier_normal")
    return "🟢", t(state, "tier_small")


def format_match_alert(
    event: Dict[str, Any],
    cfg: Config,
    state: "RuntimeState",
    *,
    cumulative: int = 0,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    成交大单告警。卡片式布局：
        🔴 $12,480 成交 ｜ YES @ 62.0¢
        Lakers vs Warriors

        方向：BUY YES
        数量：20,129 shares
        价格：62.0¢
        交易者：0x1234…abcd
        时间：2026-04-27 16:42:31 UTC

    标题 emoji 按金额分级。底部按钮：查看市场 / 查看钱包 / 查看交易。
    """
    market = event.get("market") or {}
    taker = event.get("taker") if isinstance(event.get("taker"), dict) else {}

    outcome_raw = taker.get("outcome") or event.get("outcome")
    if isinstance(outcome_raw, dict):
        outcome_name = str(outcome_raw.get("name") or "-")
    elif outcome_raw:
        outcome_name = str(outcome_raw)
    else:
        outcome_name = "-"

    raw_title = (
        market.get("title")
        or market.get("question")
        or market.get("name")
        or event.get("marketTitle")
        or ""
    )
    mid = market.get("id") or event.get("marketId")
    mid_str = str(mid) if mid is not None else ""
    slug = (
        market.get("slug")
        or market.get("urlSlug")
        or market.get("categorySlug")
        or ""
    )
    if not raw_title:
        raw_title = f"Market #{mid_str}" if mid_str else "Unknown market"

    # 标题逻辑：title 信息量足够 → 单行；title 太短/全是符号（如 "↓ 30,000" /
    # "$4B"）→ 加 slug 转的"父问题"作为上下文
    title_safe = normalize_text(raw_title)
    title_letters = sum(1 for c in raw_title if c.isalpha())
    informative = len(raw_title) >= 12 and title_letters >= 4
    if informative:
        market_line = f"<b>{title_safe}</b>"
    else:
        parent = slug_to_label(slug)
        norm_title = re.sub(r"[\s\-_]+", "", raw_title.lower())
        norm_parent = re.sub(r"[\s\-_]+", "", parent.lower()) if parent else ""
        if parent and norm_parent != norm_title:
            market_line = (
                f"<b>{normalize_text(parent)}</b>\n"
                f"<i>{title_safe}</i>"
            )
        else:
            market_line = f"<b>{title_safe}</b>"

    amount = to_decimal(event.get("amountFilled") or taker.get("amount"), cfg.shares_wei_decimals)
    notional = event_value_usdt(event, cfg)
    price = event_price_usdt(event, notional, amount)

    # 方向：Ask = SELL，Bid = BUY
    quote_type = str(taker.get("quoteType") or event.get("quoteType") or "").strip().lower()
    if quote_type in {"bid", "buy"}:
        action_label = t(state, "buy")
    elif quote_type in {"ask", "sell"}:
        action_label = t(state, "sell")
    else:
        action_label = t(state, "trade")

    signer = extract_signer(event) or ""
    username_raw = extract_username(event)
    short = short_addr(signer) if signer else ""
    # 用户名 + 短地址组合：有 username 就 "username · 0xabc…123"，没有就只显示短地址
    if username_raw and username_raw != short:
        trader_html = (
            f"<b>{normalize_text(username_raw)}</b> · "
            f"<code>{html.escape(short or '-')}</code>"
        )
    else:
        trader_html = f"<code>{html.escape(short or '-')}</code>"

    fee = event_fee_usdt(event, cfg)

    tx = extract_tx_hash(event)

    market_link = _render_template(cfg.market_url_template, id=mid_str, slug=slug, title=raw_title)
    user_link = (
        _render_template(cfg.user_url_template, address=signer, username=username_raw)
        if signer
        else ""
    )
    tx_link = _render_template(cfg.tx_url_template, hash=tx) if tx else ""

    # 标题：emoji + value（≥$100 无小数）+ outcome + 价格（cents）
    tier_emoji, _tier_label = whale_tier(notional, state)
    price_cents = price * Decimal("100")
    notional_str = fmt_money(notional)
    price_str = f"{fmt_decimal(price_cents, 1)}¢" if price > 0 else "-"
    outcome_safe = normalize_text(outcome_name)

    title_line = (
        f"{tier_emoji} <b>{notional_str} {t(state, 'card_traded')}</b>"
        f" ｜ {outcome_safe} @ {price_str}"
    )

    # 卡片正文
    side_line = f"{t(state, 'card_side')}：<b>{action_label} {outcome_safe}</b>"
    amount_line = (
        f"{t(state, 'card_amount')}：<code>{fmt_decimal(amount, 0)}</code>"
        f" {t(state, 'shares')}"
    )
    price_body_line = f"{t(state, 'card_price')}：<code>{price_str}</code>"
    trader_line = f"{t(state, 'card_trader')}：{trader_html}"
    fee_line = (
        f"{t(state, 'fee_label')}：<code>${fmt_decimal(fee, 4)} USDT</code>"
        if fee > 0 else None
    )
    # 时间：用 event.executedAt 的 UTC 时间戳；找不到就用 now
    ts_raw = event.get("executedAt") or event.get("createdAt") or ""
    if ts_raw:
        ts_str = str(ts_raw).replace("T", " ").replace("Z", " UTC")
        ts_str = re.sub(r"(\d{2}:\d{2}:\d{2})\.\d+", r"\1", ts_str)
    else:
        ts_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    time_line = f"{t(state, 'card_time')}：<code>{html.escape(ts_str)}</code>"

    body_lines = [side_line, amount_line, price_body_line, trader_line]
    if fee_line:
        body_lines.append(fee_line)
    body_lines.append(time_line)

    text = "\n".join([title_line, market_line, ""] + body_lines)

    # 底部 inline 按钮：查看市场 / 查看钱包 / 查看交易
    buttons: List[Dict[str, str]] = []
    if market_link:
        buttons.append({"text": t(state, "view_market"), "url": market_link})
    if user_link:
        buttons.append({"text": t(state, "view_wallet"), "url": user_link})
    if tx_link:
        buttons.append({"text": t(state, "view_tx"), "url": tx_link})

    markup: Optional[Dict[str, Any]] = (
        {"inline_keyboard": [buttons]} if buttons else None
    )

    return text, markup


def format_new_market_alert(
    market: Dict[str, Any], cfg: Config, state: "RuntimeState"
) -> Tuple[str, Optional[Dict[str, Any]]]:
    mid = market.get("id") or market.get("marketId")
    mid_str = str(mid) if mid is not None else ""
    slug = market.get("slug") or market.get("urlSlug") or market.get("categorySlug") or ""
    title = market_title_of(market)
    category = market.get("categorySlug") or market.get("category") or "-"
    end_date = (
        market.get("endDate")
        or market.get("expiresAt")
        or market.get("closesAt")
        or market.get("resolutionTime")
        or "-"
    )

    title_safe = normalize_text(title)
    link = _render_template(cfg.market_url_template, id=mid_str, slug=slug, title=title)
    title_html = (
        f'<a href="{html.escape(link, quote=True)}">{title_safe}</a>' if link else title_safe
    )

    if state.lang == "en":
        header = "🆕 <b>New Predict market</b>"
        market_label = "Market"
        id_label = "ID"
        cat_label = "Category"
        end_label = "Closes"
    else:
        header = "🆕 <b>Predict 新市场上线</b>"
        market_label = "市场"
        id_label = "Market ID"
        cat_label = "分类"
        end_label = "截止"

    text = "\n".join([
        header,
        f"{market_label}：<b>{title_html}</b>",
        f"{id_label}：<code>{html.escape(mid_str or '-')}</code> ｜ {cat_label}：<code>{html.escape(str(category))}</code>",
        f"{end_label}：<code>{html.escape(str(end_date))}</code>",
    ])

    markup: Optional[Dict[str, Any]] = None
    if link:
        markup = {"inline_keyboard": [[
            {"text": t(state, "view_market"), "url": link},
        ]]}

    return text, markup


async def watch_new_markets(
    cfg: Config,
    state: RuntimeState,
    predict: Predict,
    tg: Telegram,
    stop: asyncio.Event,
) -> None:
    """每 NEW_MARKETS_CHECK_SEC 秒拉一次开放市场，对没见过的市场发"上线"告警。"""
    seen_ids = load_seen_markets(cfg.seen_markets_path)
    bootstrap = not seen_ids
    iteration = 0
    BURST_THRESHOLD = 20  # 单轮发现 >N 新市场 → 大概率是 seen 状态丢了；只发摘要

    # 启动诊断：show seen 范围 + 最大 ID（便于人眼对照 predict.fun 上的最新市场）
    if seen_ids:
        sids = sorted(seen_ids)
        LOG.info(
            "[watch_new_markets] 启动: 已加载 %d 个 seen 市场 ID，范围 [%d..%d]，"
            "尾部 5 个 = %s",
            len(seen_ids), sids[0], sids[-1], sids[-5:],
        )
    else:
        LOG.info("[watch_new_markets] 启动: 无 seen 文件，第一轮将 seed 不告警")

    if state.lang == "en":
        seeded_msg = (
            f"Tracking <b>{len(seen_ids)}</b> known markets, max ID <code>{max(seen_ids)}</code>"
            if seen_ids
            else "First launch — seeding now, no alerts on this pass"
        )
    else:
        seeded_msg = (
            f"已记忆 <b>{len(seen_ids)}</b> 个市场，最大 ID <code>{max(seen_ids)}</code>"
            if seen_ids
            else "首次启动，第一轮 seed 不告警"
        )

    await tg.send(
        f"{t(state, 'newm_started')}\n"
        f"{t(state, 'interval')}: <code>{cfg.new_markets_check_sec}s</code>\n"
        + seeded_msg,
        silent=True,
    )

    while not stop.is_set():
        iteration += 1
        try:
            markets = await predict.fetch_open_markets()
            current_ids = set(markets.keys())

            # 关键防御：API 临时返回 0 时不要把 seen 清空、也不要"伪 seed"，
            # 否则下一轮恢复会把所有市场当成新市场刷屏。
            if not current_ids:
                LOG.warning(
                    "[watch_new_markets] iter=%d: API 返回 0 个开放市场，本轮跳过", iteration
                )
            elif bootstrap:
                LOG.info(
                    "[watch_new_markets] iter=%d: 首次 seed %d 个已开放市场（不推送）"
                    " 最大 ID=%d 最小 ID=%d",
                    iteration, len(current_ids), max(current_ids), min(current_ids),
                )
                seen_ids = current_ids
                bootstrap = False
                save_seen_markets(cfg.seen_markets_path, seen_ids)
            else:
                new_ids = sorted(current_ids - seen_ids)
                if new_ids:
                    titles = [market_title_of(markets[mid]) for mid in new_ids[:5]]
                    LOG.info(
                        "[watch_new_markets] iter=%d: 🆕 发现 %d 个新市场, IDs=%s 标题样本=%s",
                        iteration, len(new_ids), new_ids[:10], titles,
                    )

                    if len(new_ids) > BURST_THRESHOLD:
                        # 大概率 seen 文件丢了 / 被 reset；不要刷屏，只发个摘要
                        LOG.warning(
                            "[watch_new_markets] iter=%d: 异常 burst（%d 个新市场），"
                            "可能 seen 状态丢失。只发摘要 + 头 5 个，其余静默 seed。",
                            iteration, len(new_ids),
                        )
                        burst_msg = (
                            f"⚠️ 一次发现 <b>{len(new_ids)}</b> 个"
                            f"新市场（异常多，可能持久化丢失）。\n"
                            f"只推前 5 个，其余 <b>{len(new_ids) - 5}</b> 个静默 seed。"
                            if state.lang == "zh"
                            else f"⚠️ Detected <b>{len(new_ids)}</b> new markets in one "
                            f"pass (unusually many — likely a persistence loss).\n"
                            f"Showing top 5; the other <b>{len(new_ids) - 5}</b> seed silently."
                        )
                        await tg.send(burst_msg, silent=True)
                        for mid in new_ids[:5]:
                            text, markup = format_new_market_alert(markets[mid], cfg, state)
                            await tg.send(text, reply_markup=markup)
                    else:
                        for mid in new_ids:
                            text, markup = format_new_market_alert(markets[mid], cfg, state)
                            await tg.send(text, reply_markup=markup)

                    seen_ids.update(new_ids)
                    save_seen_markets(cfg.seen_markets_path, seen_ids)
                else:
                    # 每 5 轮（~150s）发心跳，便于核对监控是否还活
                    if iteration % 5 == 1:
                        LOG.info(
                            "[watch_new_markets] iter=%d: 无新市场（已知 %d，最大 ID=%d）",
                            iteration, len(seen_ids), max(seen_ids) if seen_ids else 0,
                        )

        except Exception:
            LOG.exception("[watch_new_markets] iter=%d: 失败", iteration)

        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.new_markets_check_sec)
        except asyncio.TimeoutError:
            pass


async def _dispatch_match(
    ev: Dict[str, Any],
    cfg: Config,
    state: "RuntimeState",
    tg: "Telegram",
) -> Tuple[bool, int]:
    """
    分发一笔成交告警：主频道（过全局阈值）+ 私聊订阅者（按各自门槛）。
    单笔的所有副作用都封装在这里，让 monitor_matches 干净。
    返回 (是否发到主频道, 发到几个订阅者)。
    """
    signer = extract_signer(ev) or ""
    n = state.bump_cumulative(signer) if signer else 0
    notional = event_value_usdt(ev, cfg)

    sent_main = False
    sent_subs = 0

    # 主告警频道：过全局阈值才发
    if notional >= state.threshold_usdt:
        text, markup = format_match_alert(ev, cfg, state, cumulative=n)
        await tg.send(text, reply_markup=markup)
        sent_main = True

    # 私聊订阅者：按各自门槛分发
    for sub_chat_id, info in list(state.subscribers.items()):
        try:
            sub_threshold = Decimal(str(info.get("threshold_usdt", "0")))
        except (InvalidOperation, ValueError, TypeError):
            continue
        if notional < sub_threshold:
            continue
        sub_lang = info.get("lang", state.lang)
        view = (
            state if sub_lang == state.lang
            else dataclass_replace_lang(state, sub_lang)
        )
        text, markup = format_match_alert(ev, cfg, view, cumulative=n)
        try:
            mid = await tg.send(text, reply_markup=markup, chat_id=sub_chat_id)
            if mid is not None:
                sent_subs += 1
            else:
                LOG.warning(
                    "[fanout] 发给 chat %s 失败（4xx/timeout）— bot 可能被 block 或 chat 失效",
                    sub_chat_id,
                )
        except Exception as exc:
            LOG.warning("[fanout] 发给 chat %s 异常: %s", sub_chat_id, exc)

    # 记入摘要滚动窗口（聚合用），不影响 alert 发送
    if notional > 0:
        try:
            market = ev.get("market") or {}
            mid_id = market.get("id") or ev.get("marketId") or ""
            slug = (
                market.get("slug")
                or market.get("urlSlug")
                or market.get("categorySlug")
                or ""
            )
            title = market_title_of(market) if isinstance(market, dict) else "?"
            ts_raw = ev.get("executedAt") or ev.get("createdAt") or ""
            ts: datetime = datetime.now(timezone.utc)
            if ts_raw:
                try:
                    ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                except Exception:
                    pass
            state.add_match_for_summary(
                market_title=title,
                market_slug=slug,
                market_id=str(mid_id),
                value=notional,
                signer=signer,
                timestamp=ts,
            )
        except Exception:
            LOG.exception("[summary] add_match_for_summary 异常（忽略）")

    return sent_main, sent_subs


async def monitor_matches(
    cfg: Config,
    state: RuntimeState,
    predict: Predict,
    tg: Telegram,
    stop: asyncio.Event,
) -> None:
    seen: "OrderedDict[str, None]" = load_seen(cfg.seen_state_path, cfg.max_seen_ids)
    # 从磁盘加载到了 seen，说明这是重启续跑（不是空白首次部署）。续跑场景下
    # 历史事件已经在 seen 里了，"新事件"自然就是上次保存之后才发生的，可以直接告警。
    resumed_from_disk = bool(seen)
    startup = True
    raw_logged = False  # 只对第一笔事件记一次原始字段，便于调试解码

    await tg.send(
        f"{t(state, 'match_started')}\n"
        f"{t(state, 'threshold')}: <b>${fmt_decimal(state.threshold_usdt, 2)} USDT</b>\n"
        f"Mode: <code>matches</code> ｜ Poll: <code>{cfg.poll_interval_sec}s</code>\n"
        f"{t(state, 'open_menu_hint')}",
        silent=True,
        reply_markup=_persistent_keyboard(),
    )

    while not stop.is_set():
        try:
            # 首次启动且 seen 为空时，只拉一页 seed，避免 fetch_new_matches 因为
            # 没有"已知"事件而连翻 max_pages 把全站历史都拉下来。
            max_pages = 1 if (startup and not resumed_from_disk) else cfg.matches_max_pages

            events, hit_max = await predict.fetch_new_matches(
                seen_ids=set(seen.keys()),
                page_size=cfg.matches_page_size,
                max_pages=max_pages,
            )

            if hit_max:
                LOG.warning(
                    "fetch_new_matches 翻满 %d 页仍未追上 seen — "
                    "成交速率高于轮询能力，考虑减小 POLL_INTERVAL_SEC 或增大 MATCHES_MAX_PAGES",
                    max_pages,
                )

            # 每次会话第一笔成交，把完整 JSON 写到日志（截断 4000 字符）。
            # 这样金额编码、tx 哈希字段、用户名字段任何位置改了，都能从日志一眼看出。
            if events and not raw_logged:
                try:
                    raw_dump = json.dumps(events[0], ensure_ascii=False, default=str, sort_keys=True)
                except Exception:
                    raw_dump = repr(events[0])
                LOG.info("[match raw event] %s", raw_dump[:4000])
                # 列出我们目前提取出来的关键字段，跟原始 JSON 对照
                LOG.info(
                    "[match parsed] tx=%s signer=%s amount=%s price=%s notional=%s",
                    extract_tx_hash(events[0]),
                    extract_signer(events[0]),
                    to_decimal(events[0].get("amountFilled") or (events[0].get("taker") or {}).get("amount"), cfg.shares_wei_decimals),
                    to_decimal(events[0].get("priceExecuted") or (events[0].get("taker") or {}).get("price"), cfg.usdt_wei_decimals),
                    event_value_usdt(events[0], cfg),
                )
                raw_logged = True

            # 客户端按 notional value 过滤。门槛取"全局 + 所有订阅者中最低值"，
            # 让每个订阅者都能拿到 ≥ 自己门槛的成交。后续在分发处再按各自门槛二次筛。
            fetched_n = len(events)
            min_threshold = state.threshold_usdt
            for _info in state.subscribers.values():
                try:
                    sub_th = Decimal(str(_info.get("threshold_usdt", "0")))
                    if sub_th > 0 and sub_th < min_threshold:
                        min_threshold = sub_th
                except (InvalidOperation, ValueError, TypeError):
                    continue
            events = [ev for ev in events if event_value_usdt(ev, cfg) >= min_threshold]
            filtered_n = len(events)

            fresh: List[Dict[str, Any]] = []
            for ev in events:
                eid = stable_event_id(ev)
                if eid in seen:
                    continue
                seen[eid] = None
                fresh.append(ev)

            while len(seen) > cfg.max_seen_ids:
                seen.popitem(last=False)

            # 决定本轮要不要真发告警：
            # - 不是 startup（已经跑过几轮）→ 永远发
            # - startup + 有 seen 文件（重启续跑）→ "fresh" 是上次保存后的新单 → 发
            # - startup + 无 seen 文件（首次部署 / 状态丢失）→ 默认静默 seed，
            #   除非显式 ALERT_ON_STARTUP=true 让用户主动选择补推历史
            if not startup:
                should_alert = True
            elif resumed_from_disk:
                should_alert = True
            else:
                should_alert = cfg.alert_on_startup  # 默认 False → 静默 seed

            sent_main = 0
            sent_subs = 0

            if fresh and not should_alert:
                LOG.info(
                    "首次启动（无 seen 文件），已记录 %d 条历史成交但不告警，等下一轮新单",
                    len(fresh),
                )
            elif fresh:
                # 接口通常按 executedAt DESC 排序，反转后按时间先后发送。
                for ev in reversed(fresh):
                    try:
                        sent_to_main, sent_to_subs = await _dispatch_match(
                            ev, cfg, state, tg,
                        )
                        sent_main += int(sent_to_main)
                        sent_subs += sent_to_subs
                    except Exception:
                        LOG.exception(
                            "[matches] 单笔事件分发异常 tx=%s — 继续下一笔",
                            extract_tx_hash(ev)[:16],
                        )

            # 每轮总结：写一行 INFO 日志 + 把统计写到 state.last_iter_stats 让 /diag 读
            iter_stats = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "fetched": fetched_n,
                "filtered": filtered_n,
                "min_threshold": str(min_threshold),
                "fresh": len(fresh),
                "subs_count": len(state.subscribers),
                "sent_main": sent_main,
                "sent_subs": sent_subs,
                "global_threshold": str(state.threshold_usdt),
            }
            state.last_iter_stats = iter_stats
            LOG.info(
                "[matches] fetched=%d filtered=%d(min_th=$%s) fresh=%d → main=%d subs=%d/%d",
                fetched_n, filtered_n, min_threshold, len(fresh),
                sent_main, sent_subs, len(state.subscribers),
            )

            startup = False

            if fresh:
                save_seen(cfg.seen_state_path, seen)

        except Exception as exc:
            LOG.exception("监控成交大单出错")

            await tg.send(
                f"⚠️ <b>Predict 成交监控错误</b>\n"
                f"<code>{html.escape(str(exc))}</code>",
                silent=True,
            )

            await asyncio.sleep(min(30, max(1, cfg.poll_interval_sec * 2)))

        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.poll_interval_sec)
        except asyncio.TimeoutError:
            pass


# -------------------- 每小时摘要 --------------------


def _format_duration_label(seconds: int, lang: str = "zh") -> str:
    """3600 → '1 小时' / '1h'; 1800 → '30 分钟' / '30m'; 等。"""
    if seconds <= 0:
        return "0"
    if seconds % 3600 == 0:
        n = seconds // 3600
        return f"{n} 小时" if lang == "zh" else f"{n}h"
    if seconds % 60 == 0:
        n = seconds // 60
        return f"{n} 分钟" if lang == "zh" else f"{n}m"
    return f"{seconds} 秒" if lang == "zh" else f"{seconds}s"


def _parse_duration(raw: str) -> Optional[int]:
    """
    /set_summary 接受：60（默认分钟）、30s、2h、0（关闭）。
    返回秒数；None = 解析失败；0 = 关闭。
    """
    s = (raw or "").strip().lower()
    if not s:
        return None
    # 0 = 关
    if s in {"0", "off", "disable", "none", "关"}:
        return 0
    # 单位后缀
    multiplier = 60  # 默认分钟
    if s.endswith(("s", "秒")):
        s = s.rstrip("s秒")
        multiplier = 1
    elif s.endswith(("m", "min", "分", "分钟")):
        for suffix in ("min", "分钟", "m", "分"):
            if s.endswith(suffix):
                s = s[: -len(suffix)]
                break
        multiplier = 60
    elif s.endswith(("h", "hour", "小时", "时")):
        for suffix in ("hour", "小时", "h", "时"):
            if s.endswith(suffix):
                s = s[: -len(suffix)]
                break
        multiplier = 3600
    s = s.strip()
    try:
        n = float(s)
    except ValueError:
        return None
    secs = int(n * multiplier)
    if secs == 0:
        return 0
    # 安全护栏：1 分钟 ~ 24 小时
    if secs < 60 or secs > 86400:
        return None
    return secs


def format_summary_alert(
    summary: Dict[str, Any], cfg: Config, state: "RuntimeState"
) -> str:
    """渲染摘要消息。"""
    label = _format_duration_label(int(summary["window_seconds"]), state.lang)
    lines: List[str] = [
        t(state, "summary_title"),
        "",
        t(state, "summary_period_fmt").format(label=label),
        f"{t(state, 'summary_total_count')}：<b>{summary['count']}</b>",
        f"{t(state, 'summary_total_value')}：<b>${fmt_decimal(summary['total_value'], 0)}</b>",
        f"{t(state, 'summary_max_trade')}：<b>${fmt_decimal(summary['max_value'], 0)}</b>"
        f" · {normalize_text(summary['max_title'])}",
        "",
        f"{t(state, 'summary_top_markets')}：",
    ]
    unit = t(state, "shares_unit")
    for i, mkt in enumerate(summary.get("top_markets") or [], start=1):
        title_safe = normalize_text(mkt["title"])
        link = _render_template(
            cfg.market_url_template,
            id=mkt.get("mid", ""), slug=mkt.get("slug", ""), title=mkt["title"],
        )
        if link:
            title_html = f'<a href="{html.escape(link, quote=True)}">{title_safe}</a>'
        else:
            title_html = title_safe
        lines.append(
            f"{i}. {title_html} — <b>${fmt_decimal(mkt['value'], 0)}</b>"
            f" / {mkt['count']} {unit}"
        )

    return "\n".join(lines)


async def summary_runner(
    cfg: Config,
    state: "RuntimeState",
    tg: "Telegram",
    stop: asyncio.Event,
) -> None:
    """每 state.summary_interval_sec 秒发一次摘要到主告警频道。"""
    LOG.info(
        "[summary] 启动（间隔 %ds = %s）",
        state.summary_interval_sec,
        _format_duration_label(state.summary_interval_sec),
    )
    iteration = 0
    while not stop.is_set():
        iteration += 1
        # 每次循环都重新读 summary_interval_sec —— 用户随时可能改
        interval = state.summary_interval_sec
        if interval <= 0:
            # 关闭状态：每 10 秒醒一次复查
            try:
                await asyncio.wait_for(stop.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass
            continue

        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return  # stop set
        except asyncio.TimeoutError:
            pass

        try:
            window = state.summary_interval_sec  # 用最新值（可能刚被 /set_summary 改过）
            if window <= 0:
                continue
            summary = state.summarize_window(window)
            if not summary:
                LOG.info(
                    "[summary] iter=%d 过去 %ds 无大单，跳过",
                    iteration, window,
                )
                # 同时也清掉太老的 recent_matches
                cutoff = datetime.now(timezone.utc) - timedelta(days=1)
                state.recent_matches = [m for m in state.recent_matches if m["ts"] >= cutoff]
                continue
            text = format_summary_alert(summary, cfg, state)
            await tg.send(text, silent=True)
            LOG.info(
                "[summary] iter=%d 已推送（%d 笔，总额 $%s，最大 $%s）",
                iteration, summary["count"],
                fmt_decimal(summary["total_value"], 0),
                fmt_decimal(summary["max_value"], 0),
            )
        except Exception:
            LOG.exception("[summary] iter=%d 失败", iteration)


async def main() -> None:
    cfg = Config.from_env()
    state = RuntimeState.from_config(cfg)

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    LOG.info(
        "启动配置: threshold=%s poll=%ss subscribers=%d",
        cfg.threshold_usdt, cfg.poll_interval_sec, len(state.subscribers),
    )

    # 持久化路径自检：试着原子写一个 probe 文件。Railway 没挂 volume 时会失败，
    # 提早报警避免静默丢订阅者/seen/阈值。
    persist_ok = check_persistence_writable(cfg)
    if not persist_ok:
        LOG.error(
            "⚠️⚠️ 持久化路径不可写（%s）。Railway → Settings → Volumes "
            "挂个 1GB 到 /data 才能跨重启留住订阅者/seen/阈值。",
            cfg.runtime_state_path,
        )

    stop = asyncio.Event()

    def _stop(*_: Any) -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    timeout = httpx.Timeout(cfg.request_timeout_sec)
    # 速度优化：
    # 1) HTTP/2 复用 TCP+TLS 连接 → 节省每次握手 ~50-150ms（Predict 服务端支持
    #    时自动启用，不支持时无声回退到 HTTP/1.1）
    # 2) 连接池：matches 轮询 + getUpdates 长轮询 + 潜在的 benchmark/ramp 并发，
    #    保 20 条空闲连接随时可用，避免连接重建
    transport_limits = httpx.Limits(
        max_keepalive_connections=20,
        max_connections=30,
        keepalive_expiry=60,
    )

    # http2 需要 h2 包；没装就降级到 http2=False（确保 import 不会失败）
    try:
        import h2  # noqa: F401  # 仅探测是否可用
        use_http2 = True
    except ImportError:
        use_http2 = False
        LOG.info("h2 包未安装，HTTP/2 不启用（pip install h2 可省每次 ~50-150ms 握手）")

    async with httpx.AsyncClient(
        timeout=timeout,
        http2=use_http2,
        limits=transport_limits,
        # User-Agent 让 Predict 一眼能看出是这个 bot，方便他们排查
        headers={"User-Agent": "predict-whale-bot/1.0"},
    ) as client:
        tg = Telegram(cfg, client)
        predict = Predict(cfg, client)
        bot = TelegramBot(cfg, state, tg, client, predict)

        tasks: List[asyncio.Task[Any]] = []

        # 成交大单监控（始终运行，盘口监控已移除）。
        tasks.append(
            asyncio.create_task(
                monitor_matches(cfg, state, predict, tg, stop)
            )
        )

        # 菜单/命令处理任务，与监控任务并行。
        tasks.append(asyncio.create_task(bot.run(stop)))

        # 新市场上线告警，独立任务。
        if cfg.watch_new_markets:
            tasks.append(asyncio.create_task(watch_new_markets(cfg, state, predict, tg, stop)))

        # 每 N 秒推一次摘要。state.summary_interval_sec=0 时任务空转直到被打开。
        tasks.append(asyncio.create_task(summary_runner(cfg, state, tg, stop)))

        await stop.wait()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        await tg.send(t(state, "stopped"), silent=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print(f"启动失败: {exc}", file=sys.stderr)
        sys.exit(1)
