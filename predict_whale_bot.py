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

    poll_interval_sec: float
    matches_page_size: int
    matches_max_pages: int
    alert_on_startup: bool
    max_seen_ids: int
    seen_state_path: str

    market_refresh_sec: int
    orderbook_sub_batch: int
    orderbook_threshold_usdt: Decimal

    request_timeout_sec: float

    market_url_template: str
    tx_url_template: str
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

            poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "3")),
            matches_page_size=int(os.getenv("MATCHES_PAGE_SIZE", "100")),
            matches_max_pages=int(os.getenv("MATCHES_MAX_PAGES", "5")),
            # 默认开：seen 持久化后，重启不会重复推送，所以 startup 告警是安全的。
            # 真正的"首次空 seen"会有特殊路径，不会刷屏。
            alert_on_startup=env_bool("ALERT_ON_STARTUP", True),
            max_seen_ids=int(os.getenv("MAX_SEEN_IDS", "10000")),
            # Railway 容器重启会丢 /tmp，要真正持久化请挂 volume 到这个路径。
            seen_state_path=os.getenv("SEEN_STATE_PATH", "/tmp/predict_seen.json").strip(),

            market_refresh_sec=int(os.getenv("MARKET_REFRESH_SEC", "300")),
            orderbook_sub_batch=int(os.getenv("ORDERBOOK_SUB_BATCH", "80")),
            orderbook_threshold_usdt=env_decimal(
                "ORDERBOOK_THRESHOLD_USDT",
                os.getenv("THRESHOLD_USDT", "1000"),
            ),

            request_timeout_sec=float(os.getenv("REQUEST_TIMEOUT_SEC", "12")),

            # 默认按 predict.fun 前端 + BNB Chain 浏览器拼接，覆盖默认即可换链或换路径。
            market_url_template=os.getenv(
                "MARKET_URL_TEMPLATE", "https://predict.fun/event/{slug}"
            ).strip(),
            tx_url_template=os.getenv(
                "TX_URL_TEMPLATE", "https://bscscan.com/tx/{hash}"
            ).strip(),
            # 用户链接：默认指向 BscScan 钱包页（一定能打开）。
            # 如果将来 predict.fun 有公开的用户主页，可以覆盖成 https://predict.fun/profile/{address}
            # 模板可用占位符：{address} / {username}
            user_url_template=os.getenv(
                "USER_URL_TEMPLATE", "https://bscscan.com/address/{address}"
            ).strip(),

            watch_new_markets=env_bool("WATCH_NEW_MARKETS", True),
            new_markets_check_sec=int(os.getenv("NEW_MARKETS_CHECK_SEC", "120")),
            seen_markets_path=os.getenv(
                "SEEN_MARKETS_PATH", "/tmp/predict_seen_markets.json"
            ).strip(),
        )


@dataclass
class RuntimeState:
    """运行期可变状态。Telegram 菜单可以改这里的阈值，监控任务每轮读取最新值。"""
    threshold_usdt: Decimal
    orderbook_threshold_usdt: Decimal
    usdt_wei_decimals: int

    @property
    def threshold_usdt_wei(self) -> int:
        scale = Decimal(10) ** self.usdt_wei_decimals
        return int((self.threshold_usdt * scale).to_integral_value(rounding=ROUND_DOWN))

    @classmethod
    def from_config(cls, cfg: Config) -> "RuntimeState":
        return cls(
            threshold_usdt=cfg.threshold_usdt,
            orderbook_threshold_usdt=cfg.orderbook_threshold_usdt,
            usdt_wei_decimals=cfg.usdt_wei_decimals,
        )


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
    """从 taker / event / user 嵌套字段里取 username，找不到返回空串。"""
    taker = event.get("taker") if isinstance(event.get("taker"), dict) else {}
    user = taker.get("user") if isinstance(taker.get("user"), dict) else {}
    profile = taker.get("profile") if isinstance(taker.get("profile"), dict) else {}
    candidates = (
        taker.get("username"),
        taker.get("name"),
        taker.get("displayName"),
        user.get("username"),
        user.get("name"),
        user.get("displayName"),
        profile.get("username"),
        profile.get("name"),
        event.get("takerUsername"),
    )
    for c in candidates:
        if c:
            s = str(c).strip()
            if s:
                return s
    return ""


def extract_signer(event: Dict[str, Any]) -> str:
    taker = event.get("taker") if isinstance(event.get("taker"), dict) else {}
    return str(taker.get("signer") or event.get("signer") or "").strip()


def extract_tx_hash(event: Dict[str, Any]) -> str:
    """从可能的字段里取出交易哈希，找不到返回空串。"""
    transaction = event.get("transaction") if isinstance(event.get("transaction"), dict) else None
    candidates = (
        event.get("transactionHash"),
        event.get("txHash"),
        event.get("transaction_hash"),
        event.get("hash"),
        (transaction or {}).get("hash"),
        (transaction or {}).get("transactionHash"),
    )
    for c in candidates:
        if c:
            s = str(c).strip()
            if s:
                return s
    return ""


def stable_event_id(event: Dict[str, Any]) -> str:
    tx = extract_tx_hash(event)
    executed = event.get("executedAt") or event.get("createdAt")
    market = (event.get("market") or {}).get("id") or event.get("marketId")
    amount = event.get("amountFilled") or event.get("amount")
    signer = ((event.get("taker") or {}).get("signer")) or event.get("signer")

    if tx:
        return f"{tx}:{executed}:{market}:{amount}:{signer}"

    raw = json.dumps(event, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_text(s: Any, limit: int = 180) -> str:
    text = str(s or "").strip()
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return html.escape(text)


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
        chunks = [text[i: i + 3900] for i in range(0, len(text), 3900)] or [text]
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
                # 4xx 一般是请求本身的问题（比如格式错误），重试也修不了。
                LOG.error("Telegram sendMessage 失败: %s %s", resp.status_code, resp.text[:500])
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
        url = f"{self.cfg.predict_api_base}{path}"
        resp = await self.client.get(url, params=params, headers=self.headers)
        resp.raise_for_status()

        data = resp.json()

        if isinstance(data, dict) and data.get("success") is False:
            raise RuntimeError(f"Predict API 返回失败: {data}")

        if not isinstance(data, dict):
            raise RuntimeError(f"Predict API 返回格式不是 dict: {type(data)}")

        return data

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


def _menu_text(state: RuntimeState) -> str:
    return (
        "🐋 <b>Predict 监控菜单</b>\n"
        f"成交阈值：<b>${fmt_decimal(state.threshold_usdt, 2)} USDT</b>\n"
        f"盘口阈值：<b>${fmt_decimal(state.orderbook_threshold_usdt, 2)} USDT</b>\n\n"
        "点预设按钮一键设置，或点 <b>🔧 自定义</b> 弹出输入框输入任意金额。\n"
        "也可手动发：<code>/set_match 数额</code> / <code>/set_book 数额</code>"
    )


def _menu_keyboard() -> Dict[str, Any]:
    def row(prefix: str, label: str) -> List[Dict[str, str]]:
        return [
            {
                "text": f"{label} ${int(amt):,}",
                "callback_data": f"{prefix}:{int(amt)}",
            }
            for amt in PRESET_AMOUNTS
        ]

    return {
        "inline_keyboard": [
            row("match", "成交"),
            row("book", "盘口"),
            [
                {"text": "🔧 自定义成交", "callback_data": "custom:match"},
                {"text": "🔧 自定义盘口", "callback_data": "custom:book"},
            ],
            [{"text": "🔄 刷新", "callback_data": "refresh"}],
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

    async def run(self, stop: asyncio.Event) -> None:
        LOG.info("Telegram 命令机器人启动，allowed chat_id=%s", self._allowed_chat_id)

        await self._prepare()

        offset = 0
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

            for update in payload.get("result") or []:
                offset = int(update.get("update_id", 0)) + 1
                try:
                    await self._handle_update(update)
                except Exception:
                    LOG.exception("处理 Telegram update 出错")

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

        if not self._allowed(chat_id):
            if text.startswith("/"):
                LOG.warning(
                    "收到未授权 chat 的命令 chat_id=%s type=%s 期望 %s text=%r — 忽略。"
                    "如需用此 chat 控制 bot，请把 TG_CHAT_ID 改成它，或在期望的 chat 里发送命令。",
                    chat_id, chat_type, self._allowed_chat_id, text[:80],
                )
            return

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
            parent_text = reply_to.get("text") or ""
            kind = "match" if "成交" in parent_text else ("book" if "盘口" in parent_text else None)
            if kind:
                await self._set_threshold(kind, text)
                return

        # 持久键盘按钮发回来的是纯文本（如「菜单」），转成对应命令处理。
        if text in KEYBOARD_ALIASES:
            text = KEYBOARD_ALIASES[text]

        if not text.startswith("/"):
            return

        LOG.info("收到命令 chat=%s type=%s text=%r", chat_id, chat_type, text[:80])

        head, _, tail = text.partition(" ")
        # 兼容 "/set_match@MyBot 1000"
        cmd = head.split("@", 1)[0].lower()
        arg = tail.strip()

        if cmd == "/start":
            # /start 用持久键盘开场，让底部「菜单」按钮立刻就位。
            await self.tg.send(
                "👋 <b>欢迎</b>\n点底部「菜单」按钮或发 /menu 进入菜单。\n"
                + _menu_text(self.state),
                reply_markup=_persistent_keyboard(),
            )
        elif cmd == "/menu":
            # /menu 用 inline 预设按钮，底部持久键盘不会被覆盖。
            await self.tg.send(_menu_text(self.state), reply_markup=_menu_keyboard())
        elif cmd == "/status":
            await self.tg.send(_menu_text(self.state), reply_markup=_persistent_keyboard())
        elif cmd == "/set_match":
            await self._set_threshold("match", arg)
        elif cmd == "/set_book":
            await self._set_threshold("book", arg)
        elif cmd == "/help":
            await self.tg.send(
                "命令列表：\n"
                "<code>/menu</code> 打开菜单（含快捷按钮）\n"
                "<code>/status</code> 查看当前阈值\n"
                "<code>/set_match 1000</code> 设置成交阈值（USDT）\n"
                "<code>/set_book 1000</code> 设置盘口阈值（USDT）\n"
                "底部「菜单」按钮 = /menu，「状态」按钮 = /status",
                reply_markup=_persistent_keyboard(),
            )

    async def _handle_callback(self, cb: Dict[str, Any]) -> None:
        cb_id = cb.get("id", "")
        chat_id = ((cb.get("message") or {}).get("chat") or {}).get("id")

        if not self._allowed(chat_id):
            LOG.warning(
                "未授权回调 chat_id=%s 期望 %s data=%r",
                chat_id, self._allowed_chat_id, cb.get("data"),
            )
            await self.tg.answer_callback_query(cb_id, "无权限")
            return

        data = cb.get("data") or ""
        message_id = (cb.get("message") or {}).get("message_id")

        if data == "refresh":
            await self.tg.answer_callback_query(cb_id, "已刷新")
            if message_id:
                await self.tg.edit_message(
                    message_id, _menu_text(self.state), reply_markup=_menu_keyboard()
                )
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
        await self.tg.answer_callback_query(cb_id, f"已设置 ${int(amount):,}")
        if message_id:
            await self.tg.edit_message(
                message_id, _menu_text(self.state), reply_markup=_menu_keyboard()
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
        await self.tg.send(_menu_text(self.state), reply_markup=_menu_keyboard())

    async def _apply_threshold(self, kind: str, amount: Decimal) -> None:
        if kind == "match":
            self.state.threshold_usdt = amount
            LOG.info("成交阈值更新为 %s USDT", amount)
        else:
            self.state.orderbook_threshold_usdt = amount
            LOG.info("盘口阈值更新为 %s USDT", amount)


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

    amount = to_decimal(event.get("amountFilled") or taker.get("amount"), cfg.usdt_wei_decimals)
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


def format_match_alert(event: Dict[str, Any], cfg: Config) -> str:
    market = event.get("market") or {}
    taker = event.get("taker") or {}
    outcome = taker.get("outcome") or event.get("outcome") or {}
    fee = taker.get("fee") or event.get("fee") or {}

    raw_title = (
        market.get("title")
        or market.get("question")
        or market.get("name")
        or event.get("marketTitle")
        or ""
    )
    mid = market.get("id") or event.get("marketId")
    mid_str = str(mid) if mid is not None else ""
    # categorySlug 在 Predict 的实际响应里通常就是市场专属 slug（例如
    # "bitcoin-up-or-down-april-26-2026-8pm-et"），故作为兜底。
    slug = (
        market.get("slug")
        or market.get("urlSlug")
        or market.get("categorySlug")
        or ""
    )

    # 标题缺失时不再显示 "-"，回退到 Market #<id>，确保"成交的市场"始终可见。
    if not raw_title:
        raw_title = f"Market #{mid_str}" if mid_str else "Unknown market"

    category = market.get("categorySlug") or market.get("category") or "-"

    amount = to_decimal(event.get("amountFilled") or taker.get("amount"), cfg.usdt_wei_decimals)
    price = to_decimal(event.get("priceExecuted") or taker.get("price"), cfg.usdt_wei_decimals)
    notional = event_value_usdt(event, cfg)

    fee_amount = to_decimal(fee.get("amount"), cfg.usdt_wei_decimals)
    signer = extract_signer(event) or "-"
    username = extract_username(event)
    makers = event.get("makers") or []
    tx = extract_tx_hash(event)
    executed_at = event.get("executedAt") or event.get("createdAt") or "-"

    title_safe = normalize_text(raw_title)
    market_link = _render_template(cfg.market_url_template, id=mid_str, slug=slug, title=raw_title)
    if market_link:
        title_html = f'<a href="{html.escape(market_link, quote=True)}">{title_safe}</a>'
    else:
        title_html = title_safe

    # Taker 显示：优先用户名，否则截短地址。一律链接到模板（默认 BscScan 地址页）。
    taker_label = normalize_text(username) if username else html.escape(short_addr(signer))
    user_link = _render_template(
        cfg.user_url_template, address=signer if signer != "-" else "", username=username
    )
    if user_link:
        taker_html = f'<a href="{html.escape(user_link, quote=True)}">{taker_label}</a>'
    else:
        taker_html = taker_label

    lines = [
        "🚨 <b>Predict 成交大单</b>",
        f"市场：<b>{title_html}</b>",
    ]

    # 成交价值最重要，紧跟市场显示。
    if notional > 0:
        lines.append(f"成交价值：<b>${fmt_decimal(notional, 2)} USDT</b>")
    else:
        lines.append("成交价值：<b>-</b>")

    lines.extend(
        [
            f"Market ID：<code>{html.escape(mid_str or '-')}</code> ｜ 分类：<code>{html.escape(str(category))}</code>",
            f"Taker：{taker_html} <code>{html.escape(short_addr(signer))}</code> ｜ Makers：<code>{len(makers)}</code>",
            f"方向字段：<code>{html.escape(str(taker.get('quoteType', event.get('quoteType', '-'))))}</code> ｜ Outcome：<b>{normalize_text(outcome.get('name') if isinstance(outcome, dict) else outcome)}</b>",
            f"份额数量：<code>{fmt_decimal(amount, 4)}</code> ｜ 价格：<code>{fmt_decimal(price, 6)}</code>",
            f"手续费：<code>{fmt_decimal(fee_amount, 6)}</code> <code>{html.escape(str(fee.get('type') or ''))}</code>",
            f"时间：<code>{html.escape(str(executed_at))}</code>",
        ]
    )

    if tx:
        tx_url = _render_template(cfg.tx_url_template, hash=tx)
        if tx_url:
            lines.append(
                f'Tx：<a href="{html.escape(tx_url, quote=True)}"><code>{html.escape(tx)}</code></a>'
            )
        else:
            lines.append(f"Tx：<code>{html.escape(tx)}</code>")
    else:
        lines.append("Tx：<code>-</code>")

    return "\n".join(lines)


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
    predict: Predict,
    tg: Telegram,
    stop: asyncio.Event,
) -> None:
    """每 NEW_MARKETS_CHECK_SEC 秒拉一次开放市场，对没见过的市场发"上线"告警。"""
    seen_ids = load_seen_markets(cfg.seen_markets_path)
    # 没磁盘记录 = 全新部署。第一轮只 seed，不要把现有几百个市场全推送。
    bootstrap = not seen_ids
    iteration = 0

    await tg.send(
        "🆕 <b>Predict 新市场监控已启动</b>\n"
        f"检查间隔：<code>{cfg.new_markets_check_sec}s</code>\n"
        + (
            f"已记忆 <b>{len(seen_ids)}</b> 个市场，新上线即推送"
            if seen_ids
            else "首次启动，第一轮 seed 不告警"
        ),
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

    await tg.send(
        f"✅ <b>Predict 成交大单监控已启动</b>\n"
        f"阈值：<b>${fmt_decimal(state.threshold_usdt, 2)} USDT</b>\n"
        f"模式：<code>matches</code> ｜ 轮询：<code>{cfg.poll_interval_sec}s</code>\n"
        f"点底部「菜单」按钮或发 /menu 调整阈值",
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
                    await tg.send(format_match_alert(ev, cfg))

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
    size = to_decimal(size_raw, cfg.usdt_wei_decimals)

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
        f"✅ <b>Predict 盘口监控已启动</b>\n"
        f"阈值：<b>${fmt_decimal(state.orderbook_threshold_usdt, 2)} USDT</b>\n"
        f"模式：<code>orderbook</code>\n"
        f"点底部「菜单」按钮或发 /menu 调整阈值",
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
            tasks.append(asyncio.create_task(watch_new_markets(cfg, predict, tg, stop)))

        await stop.wait()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        await tg.send("🛑 <b>Predict 大额监控已停止</b>", silent=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print(f"启动失败: {exc}", file=sys.stderr)
        sys.exit(1)
