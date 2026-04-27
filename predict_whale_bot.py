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
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from itertools import count
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlencode

import httpx
from dotenv import load_dotenv
import websockets


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
    predict_ws_url: str
    predict_api_key: str

    tg_bot_token: str
    tg_chat_id: str

    mode: str
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

    market_refresh_sec: int
    orderbook_sub_batch: int
    orderbook_threshold_usdt: Decimal

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

        mode = os.getenv("MODE", "matches").strip().lower()
        if mode not in {"matches", "orderbook", "both"}:
            raise RuntimeError("MODE 只能是 matches / orderbook / both")

        return cls(
            predict_api_base=os.getenv("PREDICT_API_BASE", "https://api.predict.fun").rstrip("/"),
            predict_ws_url=os.getenv("PREDICT_WS_URL", "wss://ws.predict.fun/ws").strip(),
            predict_api_key=os.getenv("PREDICT_API_KEY", "").strip(),

            tg_bot_token=token,
            tg_chat_id=chat_id,

            mode=mode,
            threshold_usdt=env_decimal("THRESHOLD_USDT", "1000"),
            usdt_wei_decimals=int(os.getenv("USDT_WEI_DECIMALS", "18")),
            # Predict 的 amountFilled / fee.amount / taker.amount 等"份额量"字段实测是 12 位
            # 小数编码（份额 × 1e12），而不是常见的 18 位 wei。如果后续接口改了再调整。
            shares_wei_decimals=int(os.getenv("SHARES_WEI_DECIMALS", "12")),

            poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "3")),
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

            market_refresh_sec=int(os.getenv("MARKET_REFRESH_SEC", "300")),
            orderbook_sub_batch=int(os.getenv("ORDERBOOK_SUB_BATCH", "80")),
            orderbook_threshold_usdt=env_decimal(
                "ORDERBOOK_THRESHOLD_USDT",
                os.getenv("THRESHOLD_USDT", "1000"),
            ),

            request_timeout_sec=float(os.getenv("REQUEST_TIMEOUT_SEC", "12")),
            api_max_retries=int(os.getenv("API_MAX_RETRIES", "5")),

            # 默认按 predict.fun 前端 + BNB Chain 浏览器拼接，覆盖默认即可换链或换路径。
            market_url_template=os.getenv(
                "MARKET_URL_TEMPLATE", "https://predict.fun/zh-cn/market/{slug}"
            ).strip(),
            tx_url_template=os.getenv(
                "TX_URL_TEMPLATE", "https://bscscan.com/tx/{hash}"
            ).strip(),

            allowed_user_ids=parse_user_id_list(os.getenv("ALLOWED_USER_IDS", "")),

            default_lang=(os.getenv("LANG_BOT") or os.getenv("LANG", "zh")).strip().lower() or "zh",
            # 用户链接：默认指向 BscScan 钱包页（一定能打开）。
            # 如果将来 predict.fun 有公开的用户主页，可以覆盖成 https://predict.fun/profile/{address}
            # 模板可用占位符：{address} / {username}
            user_url_template=os.getenv(
                "USER_URL_TEMPLATE", "https://bscscan.com/address/{address}"
            ).strip(),

            watch_new_markets=env_bool("WATCH_NEW_MARKETS", True),
            new_markets_check_sec=int(os.getenv("NEW_MARKETS_CHECK_SEC", "120")),
            seen_markets_path=os.getenv(
                "SEEN_MARKETS_PATH", "/data/predict_seen_markets.json"
            ).strip(),
        )


@dataclass
class RuntimeState:
    """
    运行期可变状态。Telegram 菜单可以改这里的阈值，监控任务每轮读取最新值。
    通过 persist() 写入 RUNTIME_STATE_PATH，重启续跑会自动加载。
    """
    threshold_usdt: Decimal
    orderbook_threshold_usdt: Decimal
    usdt_wei_decimals: int
    telegram_offset: int = 0
    lang: str = "zh"
    _path: str = ""  # 仅内部用，不参与持久化

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
            orderbook_threshold_usdt=cfg.orderbook_threshold_usdt,
            usdt_wei_decimals=cfg.usdt_wei_decimals,
            lang=lang,
            _path=cfg.runtime_state_path,
        )

        saved = load_runtime_state(cfg.runtime_state_path)
        if saved:
            try:
                if "threshold_usdt" in saved:
                    state.threshold_usdt = Decimal(str(saved["threshold_usdt"]))
                if "orderbook_threshold_usdt" in saved:
                    state.orderbook_threshold_usdt = Decimal(str(saved["orderbook_threshold_usdt"]))
                if "telegram_offset" in saved:
                    state.telegram_offset = int(saved["telegram_offset"])
                if "lang" in saved and saved["lang"] in {"zh", "en"}:
                    state.lang = saved["lang"]
                LOG.info(
                    "已从 %s 恢复 runtime state: threshold=%s orderbook=%s offset=%s lang=%s",
                    cfg.runtime_state_path,
                    state.threshold_usdt, state.orderbook_threshold_usdt, state.telegram_offset, state.lang,
                )
            except (InvalidOperation, ValueError, TypeError) as exc:
                LOG.warning("runtime state 字段格式异常 (%s): %s — 用 env 默认", saved, exc)

        return state

    def persist(self) -> None:
        """把当前阈值 + offset + lang 写盘。失败只 log，不抛异常。"""
        if not self._path:
            return
        save_runtime_state(self._path, {
            "threshold_usdt": str(self.threshold_usdt),
            "orderbook_threshold_usdt": str(self.orderbook_threshold_usdt),
            "telegram_offset": self.telegram_offset,
            "lang": self.lang,
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
        "ob_started": "✅ <b>Predict 盘口监控已启动</b>",
        "newm_started": "🆕 <b>Predict 新市场监控已启动</b>",
        "threshold": "阈值",
        "interval": "检查间隔",
        "open_menu_hint": "点底部「菜单」或发 /menu 调阈值",
        "menu_title": "🐋 <b>Predict 监控</b>",
        "match_short": "成交",
        "ob_short": "盘口",
        "lang_zh": "中文",
        "lang_en": "EN",
        "lang_switched": "已切换到中文",
        "btn_lang_switch": "🌐 EN",
        "btn_refresh": "🔄",
        "btn_test": "🧪 测试推送",
        "btn_custom": "✏️",
        "test_caption": "🧪 <b>测试推送</b>（不是真实成交）",
        "stopped": "🛑 <b>Predict 大额监控已停止</b>",
        "help_text": (
            "🐋 <b>Predict Whale Bot</b>\n\n"
            "底部按钮：<b>菜单 · 状态</b>\n\n"
            "<b>菜单按钮（管理员）</b>\n"
            "💵/📊 预设金额 → 一键改阈值\n"
            "✏️ → 输入任意金额\n"
            "🌐 → 切换中英文\n"
            "🧪 → 用当前阈值发测试推送\n\n"
            "<b>命令</b>\n"
            "<code>/menu /status</code>\n"
            "<code>/set_match 1000</code> · <code>/set_book 1000</code>\n"
            "<code>/lang zh|en</code> · <code>/whoami</code>"
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
        "ob_started": "✅ <b>Predict orderbook monitor started</b>",
        "newm_started": "🆕 <b>Predict new-market watcher started</b>",
        "threshold": "Threshold",
        "interval": "Check interval",
        "open_menu_hint": "Tap “Menu” or send /menu to change thresholds",
        "menu_title": "🐋 <b>Predict Whale Bot</b>",
        "match_short": "Match",
        "ob_short": "Book",
        "lang_zh": "中文",
        "lang_en": "EN",
        "lang_switched": "Switched to English",
        "btn_lang_switch": "🌐 中",
        "btn_refresh": "🔄",
        "btn_test": "🧪 Test",
        "btn_custom": "✏️",
        "test_caption": "🧪 <b>Test alert</b> (not a real trade)",
        "stopped": "🛑 <b>Predict whale-bot stopped</b>",
        "help_text": (
            "🐋 <b>Predict Whale Bot</b>\n\n"
            "Bottom buttons: <b>Menu · Status</b>\n\n"
            "<b>Menu (admin)</b>\n"
            "💵/📊 Preset amounts → set threshold\n"
            "✏️ → enter any amount\n"
            "🌐 → toggle zh/en\n"
            "🧪 → send a test alert with current threshold\n\n"
            "<b>Commands</b>\n"
            "<code>/menu /status</code>\n"
            "<code>/set_match 1000</code> · <code>/set_book 1000</code>\n"
            "<code>/lang zh|en</code> · <code>/whoami</code>"
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
    ) -> Optional[int]:
        chunks = chunk_html_safely(text, 3900) or [text]
        last_message_id: Optional[int] = None

        # 串行发送，避免 matches/orderbook 两个任务并发触发 Telegram 限流。
        async with self._lock:
            for idx, chunk in enumerate(chunks):
                # markup 只附在最后一片，否则按钮会被前面的分片覆盖。
                markup = reply_markup if idx == len(chunks) - 1 else None
                last_message_id = await self._send_one(
                    chunk, silent=silent, reply_markup=markup
                )
                await asyncio.sleep(0.05)

        return last_message_id

    async def _send_one(
        self,
        text: str,
        *,
        silent: bool,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> Optional[int]:
        payload: Dict[str, Any] = {
            "chat_id": self.cfg.tg_chat_id,
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
    ) -> None:
        payload: Dict[str, Any] = {
            "chat_id": self.cfg.tg_chat_id,
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


PRESET_AMOUNTS = (Decimal("500"), Decimal("1000"), Decimal("5000"), Decimal("10000"))
MIN_THRESHOLD_USDT = Decimal("1")
MAX_THRESHOLD_USDT = Decimal("10000000")


def _menu_text(state: RuntimeState, *, mode: str = "") -> str:
    """紧凑两行：阈值 + 元数据。所有操作放按钮里，文案不再啰嗦。"""
    lang_label = t(state, "lang_zh") if state.lang == "zh" else t(state, "lang_en")
    meta_bits = [f"🌐 {lang_label}"]
    if mode:
        meta_bits.append(f"⚡ <code>{html.escape(mode)}</code>")
    return (
        f"{t(state, 'menu_title')}\n\n"
        f"💵 {t(state, 'match_short')} <b>${fmt_decimal(state.threshold_usdt, 2)}</b>"
        f"  ｜  "
        f"📊 {t(state, 'ob_short')} <b>${fmt_decimal(state.orderbook_threshold_usdt, 2)}</b>\n"
        + "  ｜  ".join(meta_bits)
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
            # 第一格带 emoji，让"这一行是成交还是盘口"一眼可见
            text = f"{head_emoji} {label}" if i == 0 else label
            cells.append({"text": text, "callback_data": f"{prefix}:{int(amt)}"})
        cells.append({"text": t(state, "btn_custom"), "callback_data": f"custom:{prefix}"})
        return cells

    return {
        "inline_keyboard": [
            preset_row("match", "💵"),
            preset_row("book", "📊"),
            [
                {"text": t(state, "btn_lang_switch"), "callback_data": "lang:toggle"},
                {"text": t(state, "btn_refresh"), "callback_data": "refresh"},
                {"text": t(state, "btn_test"), "callback_data": "test"},
            ],
        ]
    }


# 自定义输入提示里的标记文字。_handle_message 通过 reply_to_message 反查到
# 这串里包含 "成交"/"盘口" 来判断要设的是哪个阈值，因此别随意改字面。
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
        # 只接受配置好的 chat_id 的命令，避免别人加机器人后乱改阈值。
        self._allowed_chat_id = str(cfg.tg_chat_id)
        # 写入类命令（改阈值）只有这些 user_id 能用。空 = 不限制（兼容老部署）。
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
        LOG.info("Telegram 命令机器人启动，allowed chat_id=%s", self._allowed_chat_id)

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
                        {"command": "set_match", "description": "设置成交阈值 (USDT)"},
                        {"command": "set_book", "description": "设置盘口阈值 (USDT)"},
                        {"command": "help", "description": "查看帮助"},
                    ]
                },
                timeout=httpx.Timeout(10),
            )
            if resp.status_code == 200 and resp.json().get("ok"):
                LOG.info("Telegram 命令列表已注册（/menu /status /set_match /set_book /help）")
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

    def _allowed(self, chat_id: Any) -> bool:
        return str(chat_id) == self._allowed_chat_id

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

        if not self._allowed(chat_id):
            if text.startswith("/"):
                LOG.warning(
                    "收到未授权 chat 的命令 chat_id=%s type=%s 期望 %s text=%r — 忽略。",
                    chat_id, chat_type, self._allowed_chat_id, text[:80],
                )
            return

        is_admin = self._is_admin(user_id)

        # 自定义阈值的 force_reply 回填：用户的消息是对 bot 之前的"请输入自定义"提示
        # 的回复时，按提示里写的"成交"/"盘口"决定改哪个阈值。
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
                await self.tg.send("⛔ 仅管理员可改阈值。")
                return
            parent_text = reply_to.get("text") or ""
            kind = "match" if "成交" in parent_text else ("book" if "盘口" in parent_text else None)
            if kind:
                await self._set_threshold(kind, text)
                LOG.info("admin %s (id=%s) 改了 %s 阈值 -> %s", user_label, user_id, kind, text)
                return

        # 持久键盘按钮发回来的是纯文本（如「菜单」），转成对应命令处理。
        if text in KEYBOARD_ALIASES:
            text = KEYBOARD_ALIASES[text]

        if not text.startswith("/"):
            return

        LOG.info(
            "收到命令 chat=%s user=%s(id=%s) text=%r admin=%s",
            chat_id, user_label, user_id, text[:80], is_admin,
        )

        head, _, tail = text.partition(" ")
        # 兼容 "/set_match@MyBot 1000"
        cmd = head.split("@", 1)[0].lower()
        arg = tail.strip()

        write_cmds = {"/set_match", "/set_book"}
        if cmd in write_cmds and not is_admin:
            await self.tg.send(
                f"⛔ 仅管理员可改阈值。当前 user_id=<code>{user_id}</code>，"
                "如需授权请把它加进 ALLOWED_USER_IDS。"
            )
            LOG.warning("非管理员尝试 %s: user=%s id=%s", cmd, user_label, user_id)
            return

        if cmd == "/start":
            # /start 用持久键盘开场，让底部「菜单」按钮立刻就位。
            await self.tg.send(
                _menu_text(self.state, mode=self.cfg.mode),
                reply_markup=_persistent_keyboard(),
            )
        elif cmd == "/menu":
            # /menu 用 inline 预设按钮，底部持久键盘不会被覆盖。
            await self.tg.send(
                _menu_text(self.state, mode=self.cfg.mode),
                reply_markup=_menu_keyboard(self.state),
            )
        elif cmd == "/status":
            await self.tg.send(
                _menu_text(self.state, mode=self.cfg.mode),
                reply_markup=_persistent_keyboard(),
            )
        elif cmd == "/whoami":
            # 帮用户查自己的 user_id，方便加进 ALLOWED_USER_IDS。
            await self.tg.send(
                f"你的 Telegram user_id：<code>{user_id}</code>\n"
                f"用户名：<code>{html.escape(str(user_label))}</code>\n"
                f"管理员权限：<b>{'✅ 是' if is_admin else '❌ 否'}</b>"
            )
        elif cmd == "/lang":
            # /lang zh / /lang en — 改语言；不带参数 = 显示当前
            wanted = arg.strip().lower()
            if wanted in {"zh", "cn", "chinese", "中文"}:
                self.state.lang = "zh"
                self.state.persist()
                await self.tg.send(t(self.state, "lang_switched"))
            elif wanted in {"en", "english", "英文"}:
                self.state.lang = "en"
                self.state.persist()
                await self.tg.send(t(self.state, "lang_switched"))
            else:
                await self.tg.send(
                    f"Current language: <b>{self.state.lang}</b>\n"
                    "Usage: <code>/lang zh</code> 或 <code>/lang en</code>"
                )
        elif cmd == "/set_match":
            await self._set_threshold("match", arg)
            LOG.info("admin %s (id=%s) /set_match -> %s", user_label, user_id, arg)
        elif cmd == "/set_book":
            await self._set_threshold("book", arg)
            LOG.info("admin %s (id=%s) /set_book -> %s", user_label, user_id, arg)
        elif cmd == "/help":
            await self.tg.send(t(self.state, "help_text"), reply_markup=_persistent_keyboard())
        elif cmd == "/test":
            if not is_admin:
                await self.tg.send("⛔ admin only")
                return
            await self._send_test_alert()

    async def _handle_callback(self, cb: Dict[str, Any]) -> None:
        cb_id = cb.get("id", "")
        chat_id = ((cb.get("message") or {}).get("chat") or {}).get("id")
        from_user = cb.get("from") or {}
        user_id = from_user.get("id")
        user_label = from_user.get("username") or from_user.get("first_name") or user_id

        if not self._allowed(chat_id):
            LOG.warning(
                "未授权回调 chat_id=%s 期望 %s data=%r",
                chat_id, self._allowed_chat_id, cb.get("data"),
            )
            await self.tg.answer_callback_query(cb_id, "无权限")
            return

        data = cb.get("data") or ""
        message_id = (cb.get("message") or {}).get("message_id")

        # 刷新是只读，谁都能点
        if data == "refresh":
            await self.tg.answer_callback_query(cb_id, "✓")
            if message_id:
                await self.tg.edit_message(
                    message_id,
                    _menu_text(self.state, mode=self.cfg.mode),
                    reply_markup=_menu_keyboard(self.state),
                )
            return

        # 切换语言：zh ↔ en，并刷新菜单消息
        if data == "lang:toggle":
            self.state.lang = "en" if self.state.lang == "zh" else "zh"
            self.state.persist()
            await self.tg.answer_callback_query(cb_id, t(self.state, "lang_switched"))
            if message_id:
                await self.tg.edit_message(
                    message_id,
                    _menu_text(self.state, mode=self.cfg.mode),
                    reply_markup=_menu_keyboard(self.state),
                )
            return

        # 测试推送：构造一笔假成交，按当前阈值/语言渲染一遍。仅管理员可触发。
        if data == "test":
            if not self._is_admin(user_id):
                await self.tg.answer_callback_query(cb_id, "⛔ admin only")
                return
            await self.tg.answer_callback_query(cb_id, "✓")
            await self._send_test_alert()
            return

        # 其它都是写操作（改阈值），需要管理员权限
        if not self._is_admin(user_id):
            LOG.warning(
                "非管理员尝试改阈值（callback）user=%s id=%s data=%r",
                user_label, user_id, data,
            )
            await self.tg.answer_callback_query(cb_id, "⛔ 仅管理员可改阈值")
            return

        if data in {"custom:match", "custom:book"}:
            kind = data.split(":", 1)[1]
            label = "成交" if kind == "match" else "盘口"
            await self.tg.answer_callback_query(cb_id, f"输入{label}阈值")
            await self.tg.send(
                # 文案里必须含 CUSTOM_PROMPT_MARKER 和「成交」或「盘口」
                # 字样，_handle_message 靠它反查。
                f"💰 {CUSTOM_PROMPT_MARKER}<b>{label}</b>阈值（USDT 数字，如 2500）。\n"
                "回复这条消息即可，发送 /menu 取消。",
                reply_markup=_custom_prompt_markup(),
            )
            return

        kind, _, raw_amount = data.partition(":")
        amount = _parse_amount(raw_amount)
        if kind not in {"match", "book"} or amount is None:
            await self.tg.answer_callback_query(cb_id, "无效操作")
            return

        await self._apply_threshold(kind, amount)
        LOG.info("admin %s (id=%s) callback %s -> %s", user_label, user_id, kind, amount)
        await self.tg.answer_callback_query(cb_id, f"✓ ${int(amount):,}")
        if message_id:
            await self.tg.edit_message(
                message_id,
                _menu_text(self.state, mode=self.cfg.mode),
                reply_markup=_menu_keyboard(self.state),
            )

    async def _set_threshold(self, kind: str, raw: str) -> None:
        amount = _parse_amount(raw)
        if amount is None:
            await self.tg.send(
                f"❌ 无效金额：<code>{html.escape(raw or '(空)')}</code>\n"
                f"范围 {MIN_THRESHOLD_USDT}–{int(MAX_THRESHOLD_USDT):,} USDT"
            )
            return

        await self._apply_threshold(kind, amount)
        await self.tg.send(
            _menu_text(self.state, mode=self.cfg.mode),
            reply_markup=_menu_keyboard(self.state),
        )

    async def _apply_threshold(self, kind: str, amount: Decimal) -> None:
        if kind == "match":
            self.state.threshold_usdt = amount
            LOG.info("成交阈值更新为 %s USDT", amount)
        else:
            self.state.orderbook_threshold_usdt = amount
            LOG.info("盘口阈值更新为 %s USDT", amount)
        # 立刻写盘，重启续跑就能恢复用户调过的阈值
        self.state.persist()

    async def _send_test_alert(self) -> None:
        """构造一笔假成交，按当前阈值/语言渲染一遍。用于验证消息样式 + 通道。"""
        # 用当前 match 阈值 + 一些方便心算的数（500 股 × 0.5 = 250 USDT）
        sample_notional = self.state.threshold_usdt
        sample_price = Decimal("0.5")  # 50¢
        sample_shares = sample_notional / sample_price if sample_price > 0 else Decimal("100")

        scale_shares = Decimal(10) ** self.cfg.shares_wei_decimals
        scale_usdt = Decimal(10) ** self.cfg.usdt_wei_decimals

        fake_event = {
            "transactionHash": "0x" + "ab" * 32,
            "executedAt": "2026-01-01T00:00:00.000Z",
            "amountFilled": str(int((sample_shares * scale_shares).to_integral_value(rounding=ROUND_DOWN))),
            "priceExecuted": str(int((sample_price * scale_usdt).to_integral_value(rounding=ROUND_DOWN))),
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
            },
            "makers": [],
        }

        text, markup = format_match_alert(fake_event, self.cfg, self.state)
        # 加个"测试推送"前缀，让用户分清这不是真单
        text = f"{t(self.state, 'test_caption')}\n\n{text}"
        await self.tg.send(text, reply_markup=markup)


def event_value_usdt(event: Dict[str, Any], cfg: Config) -> Decimal:
    # 优先使用 API 里可能直接给出的成交额字段。
    for key in (
        "valueUsdt",
        "valueUSDT",
        "valueUsdtWei",
        "notionalUsdt",
        "notionalUsdtWei",
        "totalValueUsdt",
        "totalValueUsdtWei",
    ):
        if event.get(key) is not None:
            return to_decimal(event.get(key), cfg.usdt_wei_decimals)

    taker = event.get("taker") or {}
    for key in (
        "valueUsdt",
        "valueUSDT",
        "valueUsdtWei",
        "notionalUsdt",
        "notionalUsdtWei",
    ):
        if taker.get(key) is not None:
            return to_decimal(taker.get(key), cfg.usdt_wei_decimals)

    amount = to_decimal(event.get("amountFilled") or taker.get("amount"), cfg.shares_wei_decimals)
    price = to_decimal(event.get("priceExecuted") or taker.get("price"), cfg.usdt_wei_decimals)
    return amount * price if amount and price else Decimal("0")


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
    event: Dict[str, Any], cfg: Config, state: "RuntimeState"
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
    price = to_decimal(event.get("priceExecuted") or taker.get("price"), cfg.usdt_wei_decimals)
    notional = event_value_usdt(event, cfg)

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

    lines = [
        t(state, "whale_title"),
        "",
        f"<b>{username_safe}</b> {t(state, 'made_trade')}",
        "",
        market_line,
        "",
        (
            f"{action_emoji} <b>{action_label}</b> "
            f"${fmt_decimal(notional, 2)} @ {fmt_decimal(price_cents, 1)}¢ · "
            f"{fmt_decimal(amount, 1)} {t(state, 'shares')}"
        ),
    ]

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


def format_new_market_alert(market: Dict[str, Any], cfg: Config) -> str:
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

    return "\n".join(
        [
            "🆕 <b>Predict 新市场上线</b>",
            f"市场：<b>{title_html}</b>",
            f"Market ID：<code>{html.escape(mid_str or '-')}</code> ｜ 分类：<code>{html.escape(str(category))}</code>",
            f"截止：<code>{html.escape(str(end_date))}</code>",
        ]
    )


async def watch_new_markets(
    cfg: Config,
    state: RuntimeState,
    predict: Predict,
    tg: Telegram,
    stop: asyncio.Event,
) -> None:
    """每 NEW_MARKETS_CHECK_SEC 秒拉一次开放市场，对没见过的市场发"上线"告警。"""
    seen_ids = load_seen_markets(cfg.seen_markets_path)
    # 没磁盘记录 = 全新部署。第一轮只 seed，不要把现有几百个市场全推送。
    bootstrap = not seen_ids
    iteration = 0

    if state.lang == "en":
        seeded_msg = (
            f"Tracking <b>{len(seen_ids)}</b> known markets; new listings push live"
            if seen_ids
            else "First launch — seeding now, no alerts on this pass"
        )
    else:
        seeded_msg = (
            f"已记忆 <b>{len(seen_ids)}</b> 个市场，新上线即推送"
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
                    "[watch_new_markets] iter=%d: 首次 seed %d 个已开放市场（不推送）",
                    iteration, len(current_ids),
                )
                seen_ids = current_ids
                bootstrap = False
                save_seen_markets(cfg.seen_markets_path, seen_ids)
            else:
                new_ids = sorted(current_ids - seen_ids)
                if new_ids:
                    titles = [market_title_of(markets[mid]) for mid in new_ids[:5]]
                    LOG.info(
                        "[watch_new_markets] iter=%d: 发现 %d 个新市场，IDs=%s 标题样本=%s",
                        iteration, len(new_ids), new_ids[:10], titles,
                    )
                    for mid in new_ids:
                        await tg.send(format_new_market_alert(markets[mid], cfg))
                    seen_ids.update(new_ids)
                    save_seen_markets(cfg.seen_markets_path, seen_ids)
                else:
                    # 每 10 轮（~20min）汇报一次心跳，便于排查
                    if iteration % 10 == 1:
                        LOG.info(
                            "[watch_new_markets] iter=%d: 本轮无新市场，已知 %d 个",
                            iteration, len(seen_ids),
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
                    text, markup = format_match_alert(ev, cfg, state)
                    await tg.send(text, reply_markup=markup)

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


# -------------------- 可选：盘口大挂单监控 --------------------


def parse_level(level: Any, cfg: Config) -> Optional[Tuple[Decimal, Decimal]]:
    """
    兼容 [price, size] / {price, amount|size|quantity} 的盘口层级格式。
    """
    price_raw: Any = None
    size_raw: Any = None

    if isinstance(level, dict):
        price_raw = level.get("price") or level.get("p")
        size_raw = (
            level.get("amount")
            or level.get("size")
            or level.get("quantity")
            or level.get("q")
        )
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        price_raw, size_raw = level[0], level[1]
    else:
        return None

    price = to_decimal(price_raw, cfg.usdt_wei_decimals)
    size = to_decimal(size_raw, cfg.shares_wei_decimals)

    if price <= 0 or size <= 0:
        return None

    return price, size


def normalize_side(
    levels: Iterable[Any],
    cfg: Config,
) -> Dict[str, Tuple[Decimal, Decimal]]:
    out: Dict[str, Tuple[Decimal, Decimal]] = {}

    for level in levels or []:
        parsed = parse_level(level, cfg)

        if not parsed:
            continue

        price, size = parsed
        key = str(price.normalize())
        out[key] = (price, size)

    return out


def format_orderbook_alert(
    market_id: int,
    title: str,
    side: str,
    price: Decimal,
    delta_size: Decimal,
    notional: Decimal,
    update_ts: Any,
) -> str:
    side_cn = "买盘 bids" if side == "bids" else "卖盘 asks"
    return "\n".join(
        [
            "🐋 <b>Predict 盘口大额挂单/加单</b>",
            f"市场：<b>{normalize_text(title)}</b>",
            f"Market ID：<code>{market_id}</code>",
            f"盘口：<code>{html.escape(side_cn)}</code>",
            f"新增数量：<code>{fmt_decimal(delta_size, 4)}</code> @ <code>{fmt_decimal(price, 6)}</code>",
            f"估算金额：<b>${fmt_decimal(notional, 2)} USDT</b>",
            f"更新时间：<code>{html.escape(str(update_ts or '-'))}</code>",
            "说明：盘口是价格层级增量；多人同价挂单会合并到同一层级。",
        ]
    )


async def _send_topic_op(
    ws: Any,
    method: str,
    topics: List[str],
    req_ids: count,
    batch_size: int,
) -> None:
    for i in range(0, len(topics), batch_size):
        batch = topics[i: i + batch_size]

        await ws.send(
            json.dumps(
                {
                    "method": method,
                    "requestId": next(req_ids),
                    "params": batch,
                }
            )
        )

        LOG.info("已 %s %d 个 topic", method, len(batch))
        await asyncio.sleep(0.2)


async def subscribe_topics(
    ws: Any,
    topics: List[str],
    req_ids: count,
    batch_size: int,
) -> None:
    await _send_topic_op(ws, "subscribe", topics, req_ids, batch_size)


async def unsubscribe_topics(
    ws: Any,
    topics: List[str],
    req_ids: count,
    batch_size: int,
) -> None:
    await _send_topic_op(ws, "unsubscribe", topics, req_ids, batch_size)


async def market_refresher(
    cfg: Config,
    predict: Predict,
    ws: Any,
    subscribed: Set[int],
    market_titles: Dict[int, str],
    books: Dict[int, Dict[str, Dict[str, Tuple[Decimal, Decimal]]]],
    req_ids: count,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            markets = await predict.fetch_open_markets()
            current_ids = set(markets.keys())

            new_ids = sorted(current_ids - subscribed)
            stale_ids = sorted(subscribed - current_ids)

            if new_ids:
                await subscribe_topics(
                    ws,
                    [f"predictOrderbook/{mid}" for mid in new_ids],
                    req_ids,
                    cfg.orderbook_sub_batch,
                )
                subscribed.update(new_ids)

            if stale_ids:
                await unsubscribe_topics(
                    ws,
                    [f"predictOrderbook/{mid}" for mid in stale_ids],
                    req_ids,
                    cfg.orderbook_sub_batch,
                )
                for mid in stale_ids:
                    subscribed.discard(mid)
                    books.pop(mid, None)
                    market_titles.pop(mid, None)

            # 标题可能更新（重命名），用最新值刷新
            for mid, m in markets.items():
                market_titles[mid] = market_title_of(m)

            if new_ids or stale_ids:
                LOG.info(
                    "本轮订阅变更：新增 %d，移除 %d，总订阅 %d",
                    len(new_ids),
                    len(stale_ids),
                    len(subscribed),
                )

        except Exception:
            LOG.exception("刷新/订阅市场失败")

        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg.market_refresh_sec)
        except asyncio.TimeoutError:
            pass


async def monitor_orderbook(
    cfg: Config,
    state: RuntimeState,
    predict: Predict,
    tg: Telegram,
    stop: asyncio.Event,
) -> None:
    await tg.send(
        f"{t(state, 'ob_started')}\n"
        f"{t(state, 'threshold')}: <b>${fmt_decimal(state.orderbook_threshold_usdt, 2)} USDT</b>\n"
        f"Mode: <code>orderbook</code>\n"
        f"{t(state, 'open_menu_hint')}",
        silent=True,
        reply_markup=_persistent_keyboard(),
    )

    backoff = 1

    while not stop.is_set():
        subscribed: Set[int] = set()
        market_titles: Dict[int, str] = {}

        # market_id -> side -> price_key -> (price, size)
        books: Dict[int, Dict[str, Dict[str, Tuple[Decimal, Decimal]]]] = {}

        req_ids = count(1)

        ws_url = cfg.predict_ws_url

        if cfg.predict_api_key and "apiKey=" not in ws_url:
            sep = "&" if "?" in ws_url else "?"
            ws_url = f"{ws_url}{sep}{urlencode({'apiKey': cfg.predict_api_key})}"

        refresher_task: Optional[asyncio.Task[Any]] = None

        try:
            LOG.info("连接 WebSocket: %s", cfg.predict_ws_url)

            async with websockets.connect(
                ws_url,
                # 让 websockets 自己每 20s 发一次 ping，20s 收不到 pong 就断开重连。
                # 避免链路静默掉线时 async for 无限挂起。
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
            ) as ws:
                backoff = 1

                refresher_task = asyncio.create_task(
                    market_refresher(
                        cfg,
                        predict,
                        ws,
                        subscribed,
                        market_titles,
                        books,
                        req_ids,
                        stop,
                    )
                )

                async for raw in ws:
                    msg = json.loads(raw)
                    msg_type = msg.get("type")
                    topic = msg.get("topic", "")

                    if msg_type == "R":
                        if not msg.get("success", False):
                            LOG.warning("订阅响应失败: %s", msg)
                        continue

                    if msg_type != "M":
                        continue

                    if topic == "heartbeat":
                        await ws.send(
                            json.dumps(
                                {
                                    "method": "heartbeat",
                                    "data": msg.get("data"),
                                }
                            )
                        )
                        continue

                    if not topic.startswith("predictOrderbook/"):
                        continue

                    try:
                        market_id = int(topic.split("/", 1)[1])
                    except Exception:
                        continue

                    # 已取消订阅的市场（refresher 清理过）丢弃残留帧。
                    if market_id not in subscribed:
                        continue

                    data = msg.get("data") or {}
                    title = market_titles.get(market_id, f"Market {market_id}")

                    current = {
                        "bids": normalize_side(data.get("bids") or [], cfg),
                        "asks": normalize_side(data.get("asks") or [], cfg),
                    }

                    prev = books.get(market_id)
                    books[market_id] = current

                    if prev is None:
                        # 第一帧只作为基准，不告警，避免启动时刷屏。
                        continue

                    for side in ("bids", "asks"):
                        prev_side = prev.get(side, {})
                        curr_side = current.get(side, {})

                        for key, (price, size) in curr_side.items():
                            old_size = prev_side.get(key, (price, Decimal("0")))[1]
                            delta = size - old_size

                            if delta <= 0:
                                continue

                            notional = delta * price

                            if notional >= state.orderbook_threshold_usdt:
                                await tg.send(
                                    format_orderbook_alert(
                                        market_id=market_id,
                                        title=title,
                                        side=side,
                                        price=price,
                                        delta_size=delta,
                                        notional=notional,
                                        update_ts=data.get("updateTimestampMs") or data.get("timestamp"),
                                    )
                                )

        except Exception as exc:
            LOG.exception("WebSocket 盘口监控断开/出错")

            await tg.send(
                f"⚠️ <b>Predict 盘口监控断开</b>\n"
                f"<code>{html.escape(str(exc))}</code>",
                silent=True,
            )

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

        finally:
            if refresher_task:
                refresher_task.cancel()
                await asyncio.gather(refresher_task, return_exceptions=True)


async def main() -> None:
    cfg = Config.from_env()
    state = RuntimeState.from_config(cfg)

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    LOG.info("启动配置: mode=%s threshold=%s", cfg.mode, cfg.threshold_usdt)

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

        if cfg.mode in {"matches", "both"}:
            tasks.append(
                asyncio.create_task(
                    monitor_matches(cfg, state, predict, tg, stop)
                )
            )

        if cfg.mode in {"orderbook", "both"}:
            tasks.append(
                asyncio.create_task(
                    monitor_orderbook(cfg, state, predict, tg, stop)
                )
            )

        if not tasks:
            raise RuntimeError("没有可运行的监控任务")

        # 菜单/命令处理任务，与监控任务并行。
        tasks.append(asyncio.create_task(bot.run(stop)))

        # 新市场上线告警，独立任务，跟 mode 解耦。
        if cfg.watch_new_markets:
            tasks.append(asyncio.create_task(watch_new_markets(cfg, state, predict, tg, stop)))

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
