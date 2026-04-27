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

            # 0.5s 轮询：参考 Predict API 通用限速（~10 req/s），matches 单端
            # 占 2 req/s + 新市场 ~0.03 req/s + 翻页保险 ~1 req/s 仍在容忍内。
            # 端到端检测延迟中位数 ~0.5s。被限流时 get() 会自动退避并重试。
            poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "0.5")),
            matches_page_size=int(os.getenv("MATCHES_PAGE_SIZE", "100")),
            matches_max_pages=int(os.getenv("MATCHES_MAX_PAGES", "5")),
            # 默认开：seen 持久化后，重启不会重复推送，所以 startup 告警是安全的。
            # 真正的"首次空 seen"会有特殊路径，不会刷屏。
            alert_on_startup=env_bool("ALERT_ON_STARTUP", True),
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
    # 内存里维护"本会话每个钱包大单计数"。重启清零（不持久化），FIFO 限 5000。
    cumulative_trades: "OrderedDict[str, int]" = field(default_factory=OrderedDict)
    # 滚动窗口：最近被告警过的成交，用于摘要聚合。重启清零（短窗口数据，不需持久化）。
    recent_matches: List[Dict[str, Any]] = field(default_factory=list)
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
                LOG.info(
                    "已从 %s 恢复 runtime state: threshold=%s offset=%s lang=%s summary=%ss",
                    cfg.runtime_state_path,
                    state.threshold_usdt, state.telegram_offset, state.lang,
                    state.summary_interval_sec,
                )
            except (InvalidOperation, ValueError, TypeError) as exc:
                LOG.warning("runtime state 字段格式异常 (%s): %s — 用 env 默认", saved, exc)

        return state

    def persist(self) -> None:
        """把当前阈值 + offset + lang + summary_interval 写盘。失败只 log，不抛异常。"""
        if not self._path:
            return
        save_runtime_state(self._path, {
            "threshold_usdt": str(self.threshold_usdt),
            "telegram_offset": self.telegram_offset,
            "lang": self.lang,
            "summary_interval_sec": self.summary_interval_sec,
        })


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
        "btn_test": "🧪 测试推送",
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
            "底部按钮：<b>菜单 · 状态</b>\n\n"
            "<b>菜单按钮（管理员）</b>\n"
            "💵 预设 $100/$300/$500/$1k → 一键改阈值\n"
            "✏️ → 输入任意金额\n"
            "🌐 → 切换中英文\n"
            "🧪 → 用当前阈值发测试推送\n\n"
            "<b>命令（任意聊天可用）</b>\n"
            "<code>/menu /status /summary /whoami /lang zh|en</code>\n"
            "<b>命令（管理员）</b>\n"
            "<code>/set_match 1000</code>  改成交阈值\n"
            "<code>/set_summary 60</code>  改自动摘要间隔（分钟，<code>0</code>=关）"
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
            "Bottom buttons: <b>Menu · Status</b>\n\n"
            "<b>Menu (admin)</b>\n"
            "💵 Presets $100/$300/$500/$1k → set threshold\n"
            "✏️ → enter any amount\n"
            "🌐 → toggle zh/en\n"
            "🧪 → send a test alert with current threshold\n\n"
            "<b>Commands (any chat)</b>\n"
            "<code>/menu /status /summary /whoami /lang zh|en</code>\n"
            "<b>Commands (admin)</b>\n"
            "<code>/set_match 1000</code>  threshold (USDT)\n"
            "<code>/set_summary 60</code>  auto-summary interval (min, <code>0</code>=off)"
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
    def __init__(self, cfg: Config, client: httpx.AsyncClient) -> None:
        self.cfg = cfg
        self.client = client

    @property
    def headers(self) -> Dict[str, str]:
        if self.cfg.predict_api_key:
            return {"x-api-key": self.cfg.predict_api_key}
        return {}

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
MIN_THRESHOLD_USDT = Decimal("1")
MAX_THRESHOLD_USDT = Decimal("10000000")


def _menu_text(state: RuntimeState, *, mode: str = "") -> str:
    """紧凑两行：阈值 + 元数据。所有操作放按钮里，文案不再啰嗦。"""
    lang_label = t(state, "lang_zh") if state.lang == "zh" else t(state, "lang_en")
    return (
        f"{t(state, 'menu_title')}\n\n"
        f"💵 {t(state, 'match_short')} <b>${fmt_decimal(state.threshold_usdt, 2)} USDT</b>\n"
        f"🌐 {lang_label}"
    )


def _menu_keyboard(state: RuntimeState) -> Dict[str, Any]:
    """3 行紧凑布局：每个 kind 的预设 + 自定义在一行；底部一行控制按钮。"""

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
        cells.append({"text": t(state, "btn_custom"), "callback_data": f"custom:{prefix}"})
        return cells

    return {
        "inline_keyboard": [
            preset_row("match", "💵"),
            [
                {"text": t(state, "btn_lang_switch"), "callback_data": "lang:toggle"},
                {"text": t(state, "btn_refresh"), "callback_data": "refresh"},
                {"text": t(state, "btn_test"), "callback_data": "test"},
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
    ) -> None:
        self.cfg = cfg
        self.state = state
        self.tg = tg
        self.client = client
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
                        {"command": "set_match", "description": "设置成交阈值 (USDT)"},
                        {"command": "set_summary", "description": "设置自动摘要间隔（如 60 / 2h / 0）"},
                        {"command": "whoami", "description": "查看自己的 user_id"},
                        {"command": "help", "description": "查看帮助"},
                    ]
                },
                timeout=httpx.Timeout(10),
            )
            if resp.status_code == 200 and resp.json().get("ok"):
                LOG.info("Telegram 命令列表已注册（/menu /status /summary /set_match /set_summary /whoami /help）")
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
        # 命令在哪发的就回哪。私聊 bot → 私聊回复，群里发 → 群里回复。
        # 主告警频道（cfg.tg_chat_id）不参与命令路由 —— 只用于发布全局 alert。
        src = chat_id

        # 自定义阈值的 force_reply 回填：用户的消息是对 bot 之前的"请输入自定义"提示
        # 的回复时，把后续消息当作金额处理。
        reply_to = msg.get("reply_to_message")
        if (
            isinstance(reply_to, dict)
            and (reply_to.get("from") or {}).get("is_bot")
            and CUSTOM_PROMPT_MARKER in (reply_to.get("text") or "")
            and not text.startswith("/")
            and text not in KEYBOARD_ALIASES
        ):
            if not is_admin:
                LOG.warning("非管理员尝试改阈值: user=%s id=%s", user_label, user_id)
                await self.tg.send("⛔ 仅管理员可改阈值。", chat_id=src)
                return
            await self._set_threshold("match", text, reply_chat_id=src)
            LOG.info("admin %s (id=%s) 改了成交阈值 -> %s", user_label, user_id, text)
            return

        # 持久键盘按钮发回来的是纯文本（如「菜单」），转成对应命令处理。
        if text in KEYBOARD_ALIASES:
            text = KEYBOARD_ALIASES[text]

        if not text.startswith("/"):
            return

        LOG.info(
            "收到命令 chat=%s(type=%s) user=%s(id=%s) text=%r admin=%s",
            chat_id, chat_type, user_label, user_id, text[:80], is_admin,
        )

        head, _, tail = text.partition(" ")
        # 兼容 "/set_match@MyBot 1000"
        cmd = head.split("@", 1)[0].lower()
        arg = tail.strip()

        write_cmds = {"/set_match", "/set_summary", "/test"}
        if cmd in write_cmds and not is_admin:
            await self.tg.send(
                f"⛔ 仅管理员可执行此命令。当前 user_id=<code>{user_id}</code>，"
                "如需授权请把它加进 ALLOWED_USER_IDS。",
                chat_id=src,
            )
            LOG.warning("非管理员尝试 %s: user=%s id=%s", cmd, user_label, user_id)
            return

        if cmd == "/start":
            await self.tg.send(
                _menu_text(self.state),
                reply_markup=_persistent_keyboard(),
                chat_id=src,
            )
        elif cmd == "/menu":
            await self.tg.send(
                _menu_text(self.state),
                reply_markup=_menu_keyboard(self.state),
                chat_id=src,
            )
        elif cmd == "/status":
            await self.tg.send(
                _menu_text(self.state),
                reply_markup=_persistent_keyboard(),
                chat_id=src,
            )
        elif cmd == "/whoami":
            await self.tg.send(
                f"你的 Telegram user_id：<code>{user_id}</code>\n"
                f"用户名：<code>{html.escape(str(user_label))}</code>\n"
                f"管理员权限：<b>{'✅ 是' if is_admin else '❌ 否'}</b>",
                chat_id=src,
            )
        elif cmd == "/lang":
            wanted = arg.strip().lower()
            if wanted in {"zh", "cn", "chinese", "中文"}:
                self.state.lang = "zh"
                self.state.persist()
                await self.tg.send(t(self.state, "lang_switched"), chat_id=src)
            elif wanted in {"en", "english", "英文"}:
                self.state.lang = "en"
                self.state.persist()
                await self.tg.send(t(self.state, "lang_switched"), chat_id=src)
            else:
                await self.tg.send(
                    f"Current language: <b>{self.state.lang}</b>\n"
                    "Usage: <code>/lang zh</code> 或 <code>/lang en</code>",
                    chat_id=src,
                )
        elif cmd == "/set_match":
            await self._set_threshold("match", arg, reply_chat_id=src)
            LOG.info("admin %s (id=%s) /set_match -> %s", user_label, user_id, arg)
        elif cmd == "/summary":
            # 任何用户都能查询当前摘要；回复在源聊天。
            await self._send_summary(reply_chat_id=src)
        elif cmd == "/set_summary":
            await self._handle_set_summary(arg, reply_chat_id=src)
        elif cmd == "/help":
            await self.tg.send(
                t(self.state, "help_text"),
                reply_markup=_persistent_keyboard(),
                chat_id=src,
            )
        elif cmd == "/test":
            await self._send_test_alert(reply_chat_id=src)

    async def _handle_callback(self, cb: Dict[str, Any]) -> None:
        cb_id = cb.get("id", "")
        chat_id = ((cb.get("message") or {}).get("chat") or {}).get("id")
        from_user = cb.get("from") or {}
        user_id = from_user.get("id")
        user_label = from_user.get("username") or from_user.get("first_name") or user_id

        # 不再按 chat_id 拒绝。任何 chat 都能点按钮，回复就地刷新。
        # 写操作（改阈值/测试推送）仍按 user_id 看 ALLOWED_USER_IDS。
        data = cb.get("data") or ""
        message_id = (cb.get("message") or {}).get("message_id")
        src = chat_id

        # 刷新：只读，谁都能点
        if data == "refresh":
            await self.tg.answer_callback_query(cb_id, "✓")
            if message_id:
                await self.tg.edit_message(
                    message_id,
                    _menu_text(self.state),
                    reply_markup=_menu_keyboard(self.state),
                    chat_id=src,
                )
            return

        # 切换语言：zh ↔ en
        if data == "lang:toggle":
            self.state.lang = "en" if self.state.lang == "zh" else "zh"
            self.state.persist()
            await self.tg.answer_callback_query(cb_id, t(self.state, "lang_switched"))
            if message_id:
                await self.tg.edit_message(
                    message_id,
                    _menu_text(self.state),
                    reply_markup=_menu_keyboard(self.state),
                    chat_id=src,
                )
            return

        # 测试推送：构造一笔假成交，按当前阈值/语言渲染一遍。仅管理员可触发。
        if data == "test":
            if not self._is_admin(user_id):
                await self.tg.answer_callback_query(cb_id, "⛔ admin only")
                return
            await self.tg.answer_callback_query(cb_id, "✓")
            await self._send_test_alert(reply_chat_id=src)
            return

        # 其它都是写操作（改阈值），需要管理员权限
        if not self._is_admin(user_id):
            LOG.warning(
                "非管理员尝试改阈值（callback）user=%s id=%s data=%r",
                user_label, user_id, data,
            )
            await self.tg.answer_callback_query(cb_id, "⛔ 仅管理员可改阈值")
            return

        if data == "custom:match":
            await self.tg.answer_callback_query(cb_id, "输入成交阈值")
            await self.tg.send(
                f"💰 {CUSTOM_PROMPT_MARKER}成交阈值（USDT 数字，如 2500）。\n"
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

        await self._apply_threshold(kind, amount)
        LOG.info("admin %s (id=%s) callback %s -> %s", user_label, user_id, kind, amount)
        await self.tg.answer_callback_query(cb_id, f"✓ ${int(amount):,}")
        if message_id:
            await self.tg.edit_message(
                message_id,
                _menu_text(self.state),
                reply_markup=_menu_keyboard(self.state),
                chat_id=src,
            )

    async def _set_threshold(
        self, kind: str, raw: str, *, reply_chat_id: Optional[Any] = None
    ) -> None:
        amount = _parse_amount(raw)
        if amount is None:
            await self.tg.send(
                f"❌ 无效金额：<code>{html.escape(raw or '(空)')}</code>\n"
                f"范围 {MIN_THRESHOLD_USDT}–{int(MAX_THRESHOLD_USDT):,} USDT",
                chat_id=reply_chat_id,
            )
            return

        await self._apply_threshold(kind, amount)
        await self.tg.send(
            _menu_text(self.state),
            reply_markup=_menu_keyboard(self.state),
            chat_id=reply_chat_id,
        )

    async def _apply_threshold(self, kind: str, amount: Decimal) -> None:
        # kind 历史上区分 match/book，盘口监控移除后只剩 match。
        self.state.threshold_usdt = amount
        LOG.info("成交阈值更新为 %s USDT", amount)
        # 立刻写盘，重启续跑就能恢复用户调过的阈值
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

    # (4) 兜底：放弃 amount × priceExecuted 这条死路，记 warning
    LOG.warning(
        "event_value_usdt 找不到 USDT 字段；不再用 amount×price 兜底。事件 keys=%s",
        sorted(event.keys()),
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


def format_match_alert(
    event: Dict[str, Any],
    cfg: Config,
    state: "RuntimeState",
    *,
    cumulative: int = 0,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    巨鲸提醒风格的成交告警。返回 (text, reply_markup)。

    设计目标：一眼看清"谁在哪里以多少价格做了什么"。省略 Market ID / 手续费 /
    完整时间戳 / 方向字段 / Taker 地址行 / Makers 数 — 这些细节都已经能从底部
    "查看市场 / 查看钱包 / 查看交易"按钮跳进去看到。
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
    # categorySlug 在 Predict 实际响应里通常是市场专属 slug（"polymarket-fdv-..."）
    slug = (
        market.get("slug")
        or market.get("urlSlug")
        or market.get("categorySlug")
        or ""
    )
    if not raw_title:
        raw_title = f"Market #{mid_str}" if mid_str else "Unknown market"

    # 把 slug 渲染成"父问题"标题，让单独看 "$4B" 这种短 title 时也知道是什么市场
    parent_label = slug_to_label(slug)
    # 如果父标题等价于子标题就不重复（比如 title="GC Hit Jun 2026" / slug="gc-hit-jun-2026"）
    norm_title = re.sub(r"[\s\-_]+", "", raw_title.lower())
    norm_parent = re.sub(r"[\s\-_]+", "", parent_label.lower())
    if parent_label and norm_parent != norm_title:
        market_line = (
            f"📊 <b>{normalize_text(parent_label)}</b>\n"
            f"<b>{normalize_text(raw_title)}</b> — <b>{normalize_text(outcome_name)}</b>"
        )
    else:
        market_line = (
            f"📊 <b>{normalize_text(raw_title)}</b> — <b>{normalize_text(outcome_name)}</b>"
        )

    amount = to_decimal(event.get("amountFilled") or taker.get("amount"), cfg.shares_wei_decimals)
    notional = event_value_usdt(event, cfg)
    # 价格用 notional/amount 反算最可靠；priceExecuted 在某些市场会编码出离谱数。
    price = event_price_usdt(event, notional, amount)

    # 方向：Ask = 卖出（红），Bid = 买入（绿）
    quote_type = str(taker.get("quoteType") or event.get("quoteType") or "").strip().lower()
    if quote_type in {"bid", "buy"}:
        action_label = t(state, "buy")
        action_emoji = "🟢"
    elif quote_type in {"ask", "sell"}:
        action_label = t(state, "sell")
        action_emoji = "🔴"
    else:
        action_label = t(state, "trade")
        action_emoji = "⚪"

    signer = extract_signer(event) or ""
    username_raw = extract_username(event)
    # 没有 username 就用截短地址；都没有则匿名
    if not username_raw:
        username_raw = short_addr(signer) if signer else t(state, "anon_wallet")

    tx = extract_tx_hash(event)

    market_link = _render_template(cfg.market_url_template, id=mid_str, slug=slug, title=raw_title)
    user_link = (
        _render_template(cfg.user_url_template, address=signer, username=username_raw)
        if signer
        else ""
    )
    tx_link = _render_template(cfg.tx_url_template, hash=tx) if tx else ""

    username_safe = normalize_text(username_raw)

    # 价格按"美分"展示（× 100，一位小数），符合 image 1 风格
    price_cents = price * Decimal("100")

    # notional / price 找不到合理值时显示 "-" 而不是 $0.00 / 0.0¢，避免误导
    notional_str = f"${fmt_decimal(notional, 2)}" if notional > 0 else "-"
    price_str = f"{fmt_decimal(price_cents, 1)}¢" if price > 0 else "-"

    # === 增强行 1：隐含概率 + 手续费 ===
    enrich_a_bits: List[str] = []
    if price > 0:
        # price 是 0..1 的 USDT/share，× 100 = 隐含概率 %
        enrich_a_bits.append(
            f"{t(state, 'implied_label')} {fmt_decimal(price * Decimal('100'), 1)}%"
        )
    fee = event_fee_usdt(event, cfg)
    if fee > 0:
        enrich_a_bits.append(f"{t(state, 'fee_label')} ${fmt_decimal(fee, 4)}")

    # === 增强行 2：对手方数（>1）+ 时间 + 本轮第 N 笔 ===
    enrich_b_bits: List[str] = []
    makers_count = len(event.get("makers") or [])
    if makers_count > 1:
        enrich_b_bits.append(f"{t(state, 'makers_label')} {makers_count}")
    time_ago = format_time_ago(state, event.get("executedAt") or event.get("createdAt"))
    if time_ago:
        enrich_b_bits.append(time_ago)
    if cumulative >= 2:
        enrich_b_bits.append(t(state, "cumulative_fmt").format(n=cumulative))

    lines = [
        t(state, "whale_title"),
        "",
        f"<b>{username_safe}</b> {t(state, 'made_trade')}",
        "",
        market_line,
        "",
        (
            f"{action_emoji} <b>{action_label}</b> "
            f"{notional_str} @ {price_str} · "
            f"{fmt_decimal(amount, 1)} {t(state, 'shares')}"
        ),
    ]
    if enrich_a_bits:
        lines.append("└ " + " · ".join(enrich_a_bits))
    if enrich_b_bits:
        lines.append("└ " + " · ".join(enrich_b_bits))

    text = "\n".join(lines)

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

            # 客户端按 notional value 过滤（API 不靠谱，见 fetch_new_matches 注释）。
            threshold = state.threshold_usdt
            events = [ev for ev in events if event_value_usdt(ev, cfg) >= threshold]

            fresh: List[Dict[str, Any]] = []
            for ev in events:
                eid = stable_event_id(ev)
                if eid in seen:
                    continue
                seen[eid] = None
                fresh.append(ev)

            while len(seen) > cfg.max_seen_ids:
                seen.popitem(last=False)

            should_alert = resumed_from_disk or not startup or cfg.alert_on_startup

            if fresh and not should_alert:
                LOG.info(
                    "首次启动且未启用 ALERT_ON_STARTUP，已记录 %d 条历史成交不告警",
                    len(fresh),
                )
            elif fresh:
                # 接口通常按 executedAt DESC 排序，反转后按时间先后发送。
                for ev in reversed(fresh):
                    # 只对实际告警的事件累加计数（去重 + 阈值过滤后），保证
                    # "本轮第 N 笔" 只反映被推送的大单。
                    signer = extract_signer(ev) or ""
                    n = state.bump_cumulative(signer) if signer else 0
                    text, markup = format_match_alert(ev, cfg, state, cumulative=n)
                    await tg.send(text, reply_markup=markup)

                    # 同步记入摘要滚动窗口（聚合用），不影响 alert 发送。
                    try:
                        notional = event_value_usdt(ev, cfg)
                        if notional > 0:
                            market = ev.get("market") or {}
                            mid = market.get("id") or ev.get("marketId") or ""
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
                                    ts = datetime.fromisoformat(
                                        str(ts_raw).replace("Z", "+00:00")
                                    )
                                    if ts.tzinfo is None:
                                        ts = ts.replace(tzinfo=timezone.utc)
                                except Exception:
                                    pass
                            state.add_match_for_summary(
                                market_title=title,
                                market_slug=slug,
                                market_id=str(mid),
                                value=notional,
                                signer=signer,
                                timestamp=ts,
                            )
                    except Exception:
                        LOG.exception("[summary] add_match_for_summary 异常（忽略）")

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

    LOG.info("启动配置: threshold=%s poll=%ss", cfg.threshold_usdt, cfg.poll_interval_sec)

    stop = asyncio.Event()

    def _stop(*_: Any) -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    timeout = httpx.Timeout(cfg.request_timeout_sec)

    async with httpx.AsyncClient(timeout=timeout) as client:
        tg = Telegram(cfg, client)
        predict = Predict(cfg, client)
        bot = TelegramBot(cfg, state, tg, client)

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
