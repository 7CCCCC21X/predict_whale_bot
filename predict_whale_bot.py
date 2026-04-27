#!/usr/bin/env python3
"""
Predict.fun 大额订单 / 大额成交 Telegram 监控机器人

默认监控成交大单：
  REST /v1/orders/matches?minValueUsdtWei=...

可选监控盘口大挂单：
  WebSocket predictOrderbook/{marketId}

运行：
  cp .env.example .env
  pip install -r requirements.txt
  python predict_whale_bot.py
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

    mode: str  # matches / orderbook / both
    threshold_usdt: Decimal
    usdt_wei_decimals: int

    poll_interval_sec: float
    matches_page_size: int
    alert_on_startup: bool
    max_seen_ids: int

    market_refresh_sec: int
    orderbook_sub_batch: int
    orderbook_threshold_usdt: Decimal

    request_timeout_sec: float

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
            raise RuntimeError("缺少 TG_BOT_TOKEN。请先在 .env 里填写 Telegram Bot Token。")
        if not chat_id:
            raise RuntimeError("缺少 TG_CHAT_ID。请先在 .env 里填写目标群/频道/个人 chat_id。")

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

            poll_interval_sec=float(os.getenv("POLL_INTERVAL_SEC", "8")),
            matches_page_size=int(os.getenv("MATCHES_PAGE_SIZE", "30")),
            alert_on_startup=env_bool("ALERT_ON_STARTUP", False),
            max_seen_ids=int(os.getenv("MAX_SEEN_IDS", "10000")),

            market_refresh_sec=int(os.getenv("MARKET_REFRESH_SEC", "300")),
            orderbook_sub_batch=int(os.getenv("ORDERBOOK_SUB_BATCH", "80")),
            orderbook_threshold_usdt=env_decimal(
                "ORDERBOOK_THRESHOLD_USDT",
                os.getenv("THRESHOLD_USDT", "1000"),
            ),

            request_timeout_sec=float(os.getenv("REQUEST_TIMEOUT_SEC", "12")),
        )


def d(value: Any, decimals: int = 18, *, wei_hint: bool = True) -> Decimal:
    """
    把 API 里的字符串数字转成 Decimal。

    - 123.45 直接作为 Decimal
    - 很大的整数字符串按 wei / 1e18 处理
    - 空值返回 0
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

    # Predict 的金额参数叫 minValueUsdtWei，通常用 1e18 精度。
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


def stable_event_id(event: Dict[str, Any]) -> str:
    tx = event.get("transactionHash")
    executed = event.get("executedAt")
    market = (event.get("market") or {}).get("id")
    amount = event.get("amountFilled")
    signer = ((event.get("taker") or {}).get("signer"))

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

    async def send(self, text: str, *, silent: bool = False) -> None:
        # Telegram 文本限制 4096 字符；这里留一点余量。
        chunks = [text[i: i + 3900] for i in range(0, len(text), 3900)] or [text]

        for chunk in chunks:
            resp = await self.client.post(
                f"{self.base}/sendMessage",
                json={
                    "chat_id": self.cfg.tg_chat_id,
                    "text": chunk,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                    "disable_notification": silent,
                },
            )

            if resp.status_code >= 400:
                LOG.error("Telegram sendMessage 失败: %s %s", resp.status_code, resp.text[:500])

            await asyncio.sleep(0.25)


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

        return data

    async def fetch_matches(self) -> List[Dict[str, Any]]:
        data = await self.get(
            "/v1/orders/matches",
            params={
                "first": self.cfg.matches_page_size,
                "minValueUsdtWei": str(self.cfg.threshold_usdt_wei),
            },
        )
        return list(data.get("data") or [])

    async def fetch_open_markets(self) -> Dict[int, str]:
        """
        分页拉取可见 OPEN 市场。
        返回 market_id -> title/question。
        """
        markets: Dict[int, str] = {}
        after: Optional[str] = None
        pages = 0

        while True:
            pages += 1
            params: Dict[str, Any] = {"first": 100}

            if after:
                params["after"] = after

            data = await self.get("/v1/markets", params=params)

            for m in data.get("data") or []:
                try:
                    mid = int(m.get("id"))
                except Exception:
                    continue

                visible = m.get("isVisible", True)
                trading_status = str(m.get("tradingStatus", "")).upper()

                if visible and trading_status == "OPEN":
                    markets[mid] = str(
                        m.get("title") or m.get("question") or f"Market {mid}"
                    )

            after = data.get("cursor")

            if not after or pages >= 100:
                break

        return markets


def format_match_alert(event: Dict[str, Any], cfg: Config) -> str:
    market = event.get("market") or {}
    taker = event.get("taker") or {}
    outcome = taker.get("outcome") or {}
    fee = taker.get("fee") or {}

    title = market.get("title") or market.get("question") or "-"
    mid = market.get("id", "-")
    category = market.get("categorySlug") or "-"

    amount = d(event.get("amountFilled") or taker.get("amount"), cfg.usdt_wei_decimals)
    price = d(event.get("priceExecuted") or taker.get("price"), cfg.usdt_wei_decimals)
    notional = amount * price if amount and price else Decimal("0")

    fee_amount = d(fee.get("amount"), cfg.usdt_wei_decimals)
    signer = taker.get("signer") or "-"
    makers = event.get("makers") or []
    tx = event.get("transactionHash") or "-"
    executed_at = event.get("executedAt") or "-"

    # 不假定 quoteType 就是买/卖，只展示 API 原字段，避免方向误判。
    lines = [
        "🚨 <b>Predict 成交大单</b>",
        f"市场：<b>{normalize_text(title)}</b>",
        f"Market ID：<code>{html.escape(str(mid))}</code> ｜ 分类：<code>{html.escape(str(category))}</code>",
        f"Taker：<code>{html.escape(short_addr(signer))}</code> ｜ Makers：<code>{len(makers)}</code>",
        f"方向字段：<code>{html.escape(str(taker.get('quoteType', '-')))}</code> ｜ Outcome：<b>{normalize_text(outcome.get('name') or '-')}</b>",
    ]

    if notional > 0:
        lines.append(f"估算成交额：<b>${fmt_decimal(notional, 2)} USDT</b>")

    lines.extend(
        [
            f"数量：<code>{fmt_decimal(amount, 4)}</code> ｜ 价格：<code>{fmt_decimal(price, 6)}</code>",
            f"手续费：<code>{fmt_decimal(fee_amount, 6)}</code> <code>{html.escape(str(fee.get('type') or ''))}</code>",
            f"时间：<code>{html.escape(str(executed_at))}</code>",
            f"Tx：<code>{html.escape(short_addr(tx))}</code>",
        ]
    )

    return "\n".join(lines)


async def monitor_matches(
    cfg: Config,
    predict: Predict,
    tg: Telegram,
    stop: asyncio.Event,
) -> None:
    seen: Set[str] = set()
    startup = True

    await tg.send(
        f"✅ <b>Predict 成交大单监控已启动</b>\n"
        f"阈值：<b>${fmt_decimal(cfg.threshold_usdt, 2)} USDT</b>\n"
        f"模式：<code>matches</code> ｜ 轮询：<code>{cfg.poll_interval_sec}s</code>",
        silent=True,
    )

    while not stop.is_set():
        try:
            events = await predict.fetch_matches()
            fresh: List[Dict[str, Any]] = []

            for ev in events:
                eid = stable_event_id(ev)

                if eid in seen:
                    continue

                seen.add(eid)
                fresh.append(ev)

            if startup and not cfg.alert_on_startup:
                LOG.info("启动时已种子化 %d 条历史成交，不发送历史告警", len(seen))
                startup = False
            else:
                startup = False

                # 接口按 executedAt DESC 排序，反转后按时间先后发送。
                for ev in reversed(fresh):
                    await tg.send(format_match_alert(ev, cfg))

            if len(seen) > cfg.max_seen_ids:
                # set 无序，简单裁剪；生产上可换成 OrderedDict/LRU。
                seen = set(list(seen)[-cfg.max_seen_ids // 2:])

        except Exception as exc:
            LOG.exception("监控成交大单出错")

            await tg.send(
                f"⚠️ <b>Predict 成交监控错误</b>\n"
                f"<code>{html.escape(str(exc))}</code>",
                silent=True,
            )

            await asyncio.sleep(min(30, cfg.poll_interval_sec * 2))

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

    price = d(price_raw, cfg.usdt_wei_decimals)
    size = d(size_raw, cfg.usdt_wei_decimals)

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
    return "\n".join(
        [
            "🐋 <b>Predict 盘口大额挂单/加单</b>",
            f"市场：<b>{normalize_text(title)}</b>",
            f"Market ID：<code>{market_id}</code>",
            f"盘口：<code>{html.escape(side)}</code>",
            f"新增数量：<code>{fmt_decimal(delta_size, 4)}</code> @ <code>{fmt_decimal(price, 6)}</code>",
            f"估算金额：<b>${fmt_decimal(notional, 2)} USDT</b>",
            f"更新时间：<code>{html.escape(str(update_ts or '-'))}</code>",
            "说明：这是盘口层级增量监控；如果多人同价挂单，会显示为同一价格层级的合并增量。",
        ]
    )


async def subscribe_topics(
    ws: Any,
    topics: List[str],
    req_ids: count,
    batch_size: int,
) -> None:
    for i in range(0, len(topics), batch_size):
        batch = topics[i: i + batch_size]

        await ws.send(
            json.dumps(
                {
                    "method": "subscribe",
                    "requestId": next(req_ids),
                    "params": batch,
                }
            )
        )

        LOG.info("已订阅 %d 个 topic", len(batch))
        await asyncio.sleep(0.2)


async def market_refresher(
    cfg: Config,
    predict: Predict,
    ws: Any,
    subscribed: Set[int],
    market_titles: Dict[int, str],
    req_ids: count,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            markets = await predict.fetch_open_markets()
            market_titles.update(markets)

            new_ids = [mid for mid in markets if mid not in subscribed]

            if new_ids:
                topics = [f"predictOrderbook/{mid}" for mid in new_ids]

                await subscribe_topics(
                    ws,
                    topics,
                    req_ids,
                    cfg.orderbook_sub_batch,
                )

                subscribed.update(new_ids)

                LOG.info(
                    "本轮新增订阅市场: %d，总订阅: %d",
                    len(new_ids),
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
    predict: Predict,
    tg: Telegram,
    stop: asyncio.Event,
) -> None:
    await tg.send(
        f"✅ <b>Predict 盘口监控已启动</b>\n"
        f"阈值：<b>${fmt_decimal(cfg.orderbook_threshold_usdt, 2)} USDT</b>\n"
        f"模式：<code>orderbook</code>",
        silent=True,
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
                ping_interval=None,
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

                            if notional >= cfg.orderbook_threshold_usdt:
                                await tg.send(
                                    format_orderbook_alert(
                                        market_id=market_id,
                                        title=title,
                                        side=side,
                                        price=price,
                                        delta_size=delta,
                                        notional=notional,
                                        update_ts=data.get("updateTimestampMs"),
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


async def main() -> None:
    cfg = Config.from_env()

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

        tasks: List[asyncio.Task[Any]] = []

        if cfg.mode in {"matches", "both"}:
            tasks.append(
                asyncio.create_task(
                    monitor_matches(cfg, predict, tg, stop)
                )
            )

        if cfg.mode in {"orderbook", "both"}:
            tasks.append(
                asyncio.create_task(
                    monitor_orderbook(cfg, predict, tg, stop)
                )
            )

        if not tasks:
            raise RuntimeError("没有可运行的监控任务")

        await stop.wait()

        for t in tasks:
            t.cancel()

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
