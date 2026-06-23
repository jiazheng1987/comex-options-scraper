#!/usr/bin/env python3
"""
COMEX 黄金/白银期权 Volume & Open Interest 自动抓取脚本。

每天美国东部时间 17:00（收盘后）从 CME Group 官网公开接口拉取
行权价、成交量、未平仓量，去重后追加写入 comex_options_data.csv。

用法:
  python fetch_comex_options.py          # 启动定时任务（每天 17:00 ET）
  python fetch_comex_options.py --once   # 立即执行一次
  python fetch_comex_options.py --once --date 2026-05-22
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

ET = ZoneInfo("America/New_York")
OUTPUT_CSV = Path(__file__).resolve().parent / "comex_options_data.csv"

UNDERLYINGS = (
    {"symbol": "GC", "name": "Gold", "referer": "https://www.cmegroup.com/markets/metals/precious/gold-futures.html"},
    {"symbol": "SI", "name": "Silver", "referer": "https://www.cmegroup.com/markets/metals/precious/silver-futures.html"},
)

CME_BASE = "https://www.cmegroup.com"
PRODUCT_SLATE_URL = (
    CME_BASE
    + "/CmeWS/mvc/ProductSlate/V2/List?pageNumber=1&sortAsc=false&sortField=rank&searchString={symbol}&pageSize=10"
)
TRADE_DATES_URL = CME_BASE + "/CmeWS/mvc/Settlements/Options/TradeDateAndExpirations/{product_id}"
OPTION_CATEGORIES_URL = CME_BASE + "/CmeWS/mvc/Options/Categories/List/{option_id}/G?optionTypeFilter="
OPTION_QUOTES_URL = (
    CME_BASE
    + "/CmeWS/mvc/Quotes/Option/{option_id}/G/{expiration_code}/ALL"
    + "?optionProductId={option_id}&strikeRange=ALL&_={ts}"
)
DAILY_SETTLEMENT_URL = (
    CME_BASE
    + "/CmeWS/mvc/xsltTransformer.do?xlstDoc=/XSLT/da/DailySettlement.xsl"
    + "&url=/da/DailySettlement/V1/DSReport/ProductCode/{product_code}/FOI/OOF"
    + "/EXCHANGE/XCEC/Underlying/{underlying}/ProductId/{product_id}"
    + "?tradeDate={trade_date}&monthYear=null&optionTypeName={option_type_name}&optionType={option_type}"
)

CSV_COLUMNS = [
    "trade_date",
    "underlying",
    "product_name",
    "option_type",
    "expiration",
    "expiration_code",
    "strike_price",
    "option_right",
    "volume",
    "open_interest",
    "source",
    "fetched_at",
]

DEDUP_KEYS = [
    "trade_date",
    "underlying",
    "product_name",
    "option_type",
    "expiration",
    "strike_price",
    "option_right",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("comex_options")


# ---------------------------------------------------------------------------
# HTTP 客户端（带重试与请求头轮换）
# ---------------------------------------------------------------------------


class CMEClient:
    def __init__(self, max_retries: int = 5, base_delay: float = 2.0) -> None:
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.session = requests.Session()
        self._ua_index = 0

    def _build_headers(self, referer: str | None = None) -> dict[str, str]:
        ua = USER_AGENTS[self._ua_index % len(USER_AGENTS)]
        headers = {
            "User-Agent": ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": referer or f"{CME_BASE}/",
            "Origin": CME_BASE,
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
        return headers

    def get_json(self, url: str, referer: str | None = None) -> Any:
        return self.get(url, referer=referer).json()

    def get_text(self, url: str, referer: str | None = None) -> str:
        return self.get(url, referer=referer).text

    def get(self, url: str, referer: str | None = None) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            headers = self._build_headers(referer)
            try:
                resp = self.session.get(url, headers=headers, timeout=45)
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (403, 429, 503):
                    self._ua_index += 1
                    delay = self.base_delay * (2 ** (attempt - 1)) + random.uniform(0.5, 1.5)
                    log.warning(
                        "HTTP %s for %s (attempt %s/%s), retry in %.1fs",
                        resp.status_code,
                        url,
                        attempt,
                        self.max_retries,
                        delay,
                    )
                    if resp.status_code == 403:
                        log.warning("CME 访问限制，已轮换 User-Agent 与请求头后重试")
                    time.sleep(delay)
                    last_error = requests.HTTPError(f"{resp.status_code} {resp.text[:200]}")
                    continue
                resp.raise_for_status()
            except requests.RequestException as exc:
                last_error = exc
                delay = self.base_delay * (2 ** (attempt - 1)) + random.uniform(0.5, 1.5)
                log.warning("请求失败 %s (attempt %s/%s): %s", url, attempt, self.max_retries, exc)
                time.sleep(delay)
        raise RuntimeError(f"请求失败，已重试 {self.max_retries} 次: {url}") from last_error


# ---------------------------------------------------------------------------
# 数据解析工具
# ---------------------------------------------------------------------------


def _clean_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "—", "N/A", "null"}:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _clean_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text or text in {"-", "—", "N/A", "null"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _pick(d: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in d and d[key] not in (None, "", "-"):
            return d[key]
    return None


def _option_type_label(entry: dict[str, Any]) -> str:
    if entry.get("daily"):
        return "Daily"
    if entry.get("weekly"):
        return "Weekly"
    if entry.get("sto"):
        return "Short-Term"
    code = entry.get("optionType") or entry.get("optionTypeName") or "Unknown"
    return str(code)


def _right_label(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"c", "call", "calls"}:
        return "Call"
    if text in {"p", "put", "puts"}:
        return "Put"
    return str(value)


# ---------------------------------------------------------------------------
# CME 数据抓取
# ---------------------------------------------------------------------------


def get_future_product_id(client: CMEClient, symbol: str, referer: str) -> int:
    url = PRODUCT_SLATE_URL.format(symbol=symbol)
    payload = client.get_json(url, referer=referer)
    products = payload.get("products") or payload.get("Products") or []
    matches = [
        p
        for p in products
        if (p.get("globex") or p.get("Globex") or "").upper() == symbol.upper()
        and (p.get("cleared") or p.get("Cleared") or "").lower() == "futures"
        and (p.get("globexTraded") or p.get("GlobexTraded") is not False)
    ]
    if not matches:
        raise RuntimeError(f"未在 ProductSlate 中找到 {symbol} 期货产品 ID")
    product_id = matches[0].get("id") or matches[0].get("Id")
    log.info("%s 期货 Product ID: %s", symbol, product_id)
    return int(product_id)


def get_option_products(client: CMEClient, future_product_id: int, referer: str) -> list[dict[str, Any]]:
    url = TRADE_DATES_URL.format(product_id=future_product_id)
    payload = client.get_json(url, referer=referer)
    if not isinstance(payload, list):
        raise RuntimeError(f"TradeDateAndExpirations 返回异常: {type(payload)}")
    return payload


def get_expiration_codes(client: CMEClient, option_product_id: int, referer: str) -> list[tuple[str, str | None]]:
    url = OPTION_CATEGORIES_URL.format(option_id=option_product_id)
    payload = client.get_json(url, referer=referer)
    expirations: list[tuple[str, str | None]] = []
    for item in payload if isinstance(payload, list) else []:
        code = item.get("expirationCode") or item.get("code") or item.get("id")
        exp_date = item.get("expirationDate") or item.get("expiry")
        if code:
            expirations.append((str(code), str(exp_date) if exp_date else None))
    return expirations


def parse_option_quotes_payload(
    payload: dict[str, Any],
    *,
    underlying: str,
    product_name: str,
    option_type: str,
    expiration: str | None,
    expiration_code: str,
    trade_date: str,
    fetched_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    quotes = payload.get("optionContractQuotes") or payload.get("quotes") or []
    for entry in quotes:
        strike = _clean_float(_pick(entry, "strikePrice", "options-strikePrice", "strike"))
        if strike is None:
            continue

        # 部分响应在 entry 内嵌 call/put 对象
        nested_pairs = [
            ("Call", entry.get("call") or entry.get("Call")),
            ("Put", entry.get("put") or entry.get("Put")),
        ]
        handled = False
        for right, side in nested_pairs:
            if not isinstance(side, dict):
                continue
            handled = True
            rows.append(
                {
                    "trade_date": trade_date,
                    "underlying": underlying,
                    "product_name": product_name,
                    "option_type": option_type,
                    "expiration": expiration or _pick(side, "expirationDate", "options-expirationDate") or "",
                    "expiration_code": expiration_code,
                    "strike_price": strike,
                    "option_right": right,
                    "volume": _clean_int(_pick(side, "volume", "options-volume", "estimatedVolume")),
                    "open_interest": _clean_int(
                        _pick(side, "openInterest", "priorDayOpenInterest", "options-openInterest", "open_interest")
                    ),
                    "source": "quotes_api",
                    "fetched_at": fetched_at,
                }
            )

        if handled:
            continue

        # 扁平字段（CME3.py 风格）
        right = _right_label(_pick(entry, "options-optiontype", "optionType", "type"))
        if right:
            rows.append(
                {
                    "trade_date": trade_date,
                    "underlying": underlying,
                    "product_name": product_name,
                    "option_type": option_type,
                    "expiration": expiration
                    or _pick(entry, "futures-expirationDate", "expirationDate", "options-expirationDate")
                    or "",
                    "expiration_code": expiration_code,
                    "strike_price": strike,
                    "option_right": right,
                    "volume": _clean_int(_pick(entry, "options-volume", "volume", "estimatedVolume")),
                    "open_interest": _clean_int(
                        _pick(entry, "options-openInterest", "openInterest", "priorDayOpenInterest")
                    ),
                    "source": "quotes_api",
                    "fetched_at": fetched_at,
                }
            )
    return rows


def fetch_from_quotes_api(
    client: CMEClient,
    underlying_cfg: dict[str, str],
    trade_date: date | None = None,
) -> list[dict[str, Any]]:
    symbol = underlying_cfg["symbol"]
    referer = underlying_cfg["referer"]
    fetched_at = datetime.now(tz=ET).isoformat()
    trade_date_str = (trade_date or date.today()).isoformat()

    future_product_id = get_future_product_id(client, symbol, referer)
    option_products = get_option_products(client, future_product_id, referer)

    rows: list[dict[str, Any]] = []
    for opt_product in option_products:
        option_product_id = opt_product.get("productId") or opt_product.get("ProductId")
        if not option_product_id:
            continue
        option_type = _option_type_label(opt_product)
        product_name = opt_product.get("productName") or opt_product.get("name") or f"{underlying_cfg['name']} Option"

        expirations = opt_product.get("expirations") or opt_product.get("Expirations") or []
        expiration_items: list[tuple[str, str | None]] = []
        for exp in expirations:
            code = exp.get("code") or exp.get("expirationCode")
            exp_date = exp.get("expiration") or exp.get("expirationDate")
            if code:
                expiration_items.append((str(code), str(exp_date) if exp_date else None))

        if not expiration_items:
            expiration_items = get_expiration_codes(client, int(option_product_id), referer)

        for expiration_code, expiration_date in expiration_items:
            ts = int(time.time() * 1000)
            url = OPTION_QUOTES_URL.format(
                option_id=option_product_id,
                expiration_code=expiration_code,
                ts=ts,
            )
            try:
                payload = client.get_json(url, referer=referer)
            except RuntimeError as exc:
                log.warning("跳过 %s %s: %s", product_name, expiration_code, exc)
                continue

            parsed = parse_option_quotes_payload(
                payload,
                underlying=symbol,
                product_name=product_name,
                option_type=option_type,
                expiration=expiration_date,
                expiration_code=expiration_code,
                trade_date=trade_date_str,
                fetched_at=fetched_at,
            )
            if parsed:
                log.info(
                    "Quotes API: %s %s %s -> %s 条",
                    symbol,
                    option_type,
                    expiration_code,
                    len(parsed),
                )
                rows.extend(parsed)
            time.sleep(0.35)

    return rows


def fetch_from_daily_settlement(
    client: CMEClient,
    underlying_cfg: dict[str, str],
    trade_date: date | None = None,
) -> list[dict[str, Any]]:
    """HTML 日报结算表备用数据源（含 Strike / Volume / Open Interest）。"""
    symbol = underlying_cfg["symbol"]
    referer = underlying_cfg["referer"]
    fetched_at = datetime.now(tz=ET).isoformat()
    td = trade_date or date.today()
    trade_date_fmt = td.strftime("%m/%d/%Y")
    trade_date_str = td.isoformat()

    future_product_id = get_future_product_id(client, symbol, referer)
    option_products = get_option_products(client, future_product_id, referer)

    rows: list[dict[str, Any]] = []
    for opt_product in option_products:
        option_product_id = opt_product.get("productId") or opt_product.get("ProductId")
        option_type_code = opt_product.get("optionType") or "AME"
        option_type_name = opt_product.get("optionTypeName") or _option_type_label(opt_product)
        product_name = opt_product.get("productName") or f"{underlying_cfg['name']} Option"
        product_code = opt_product.get("productCode") or opt_product.get("globex") or symbol

        url = DAILY_SETTLEMENT_URL.format(
            product_code=product_code,
            underlying=symbol,
            product_id=option_product_id,
            trade_date=trade_date_fmt,
            option_type_name=option_type_name,
            option_type=option_type_code,
        )
        try:
            html = client.get_text(url, referer=referer)
        except RuntimeError as exc:
            log.warning("Daily Settlement 备用源失败 %s: %s", product_name, exc)
            continue

        table_rows = _parse_daily_settlement_html(
            html,
            underlying=symbol,
            product_name=product_name,
            option_type=option_type_name,
            trade_date=trade_date_str,
            fetched_at=fetched_at,
        )
        if table_rows:
            log.info("Daily Settlement: %s -> %s 条", product_name, len(table_rows))
            rows.extend(table_rows)
        time.sleep(0.35)

    return rows


def _parse_daily_settlement_html(
    html: str,
    *,
    underlying: str,
    product_name: str,
    option_type: str,
    trade_date: str,
    fetched_at: str,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    header_cells = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
    if not header_cells:
        first_row = table.find("tr")
        if first_row:
            header_cells = [td.get_text(" ", strip=True).lower() for td in first_row.find_all("td")]

    def col_idx(*names: str) -> int | None:
        for i, header in enumerate(header_cells):
            if any(name in header for name in names):
                return i
        return None

    idx_strike = col_idx("strike")
    idx_type = col_idx("type")
    idx_volume = col_idx("volume", "estimated")
    idx_oi = col_idx("open interest", "openinterest", "prior day")

    rows: list[dict[str, Any]] = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cells) < 3:
            continue

        strike_raw = cells[idx_strike] if idx_strike is not None and idx_strike < len(cells) else cells[0]
        strike = _clean_float(strike_raw)
        if strike is None:
            continue

        right_raw = cells[idx_type] if idx_type is not None and idx_type < len(cells) else None
        right = _right_label(right_raw)
        if not right:
            continue

        volume = _clean_int(cells[idx_volume]) if idx_volume is not None and idx_volume < len(cells) else None
        oi = _clean_int(cells[idx_oi]) if idx_oi is not None and idx_oi < len(cells) else None

        rows.append(
            {
                "trade_date": trade_date,
                "underlying": underlying,
                "product_name": product_name,
                "option_type": option_type,
                "expiration": "",
                "expiration_code": "",
                "strike_price": strike,
                "option_right": right,
                "volume": volume,
                "open_interest": oi,
                "source": "daily_settlement_html",
                "fetched_at": fetched_at,
            }
        )
    return rows


# ---------------------------------------------------------------------------
# CSV 持久化
# ---------------------------------------------------------------------------


def append_deduped_rows(rows: Iterable[dict[str, Any]], csv_path: Path = OUTPUT_CSV) -> int:
    new_df = pd.DataFrame(list(rows), columns=CSV_COLUMNS)
    if new_df.empty:
        log.warning("没有可写入的数据")
        return 0

    existing_len = 0
    if csv_path.exists():
        try:
            existing = pd.read_csv(csv_path, dtype=str)
            existing_len = len(existing)
        except Exception:
            existing = pd.DataFrame(columns=CSV_COLUMNS)
        combined = pd.concat([existing, new_df.astype(str)], ignore_index=True)
    else:
        combined = new_df.astype(str)

    combined = combined.drop_duplicates(subset=DEDUP_KEYS, keep="last")
    combined.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    added = len(combined) - existing_len
    log.info("已写入 %s（合并后共 %s 行，本次新增约 %s 行）", csv_path, len(combined), max(added, 0))
    return len(new_df)


# ---------------------------------------------------------------------------
# 主流程与定时
# ---------------------------------------------------------------------------


def run_fetch(trade_date: date | None = None) -> None:
    client = CMEClient()
    all_rows: list[dict[str, Any]] = []

    for cfg in UNDERLYINGS:
        log.info("开始抓取 %s (%s)...", cfg["name"], cfg["symbol"])
        try:
            rows = fetch_from_quotes_api(client, cfg, trade_date=trade_date)
            if not rows:
                log.info("%s Quotes API 无数据，尝试 Daily Settlement 备用源...", cfg["symbol"])
                rows = fetch_from_daily_settlement(client, cfg, trade_date=trade_date)
            all_rows.extend(rows)
        except Exception as exc:
            log.error("抓取 %s 失败: %s", cfg["symbol"], exc, exc_info=True)

    append_deduped_rows(all_rows)


def seconds_until_next_run(target_hour: int = 17, tz: ZoneInfo = ET) -> float:
    now = datetime.now(tz)
    target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def run_scheduler() -> None:
    try:
        import schedule
    except ImportError:
        log.error("缺少 schedule 库，请运行: pip install -r requirements.txt")
        sys.exit(1)

    def job() -> None:
        log.info("定时任务触发（美东 17:00）")
        run_fetch()

    schedule.every().day.at("17:00", "America/New_York").do(job)
    log.info("定时任务已启动：每天美东时间 17:00 执行，输出文件 %s", OUTPUT_CSV)
    log.info("按 Ctrl+C 退出")

    while True:
        schedule.run_pending()
        time.sleep(30)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="COMEX 黄金/白银期权 Volume & OI 自动抓取")
    parser.add_argument("--once", action="store_true", help="立即执行一次，不启动定时任务")
    parser.add_argument("--date", type=str, default=None, help="指定交易日 YYYY-MM-DD（仅 --once 时有效）")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    trade_date = None
    if args.date:
        trade_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    if args.once:
        run_fetch(trade_date=trade_date)
        return

    # 启动时若已过 17:00 ET 且今日尚未抓取，可先补跑（可选：这里直接等待下次定时）
    wait_sec = seconds_until_next_run()
    log.info("距离下次执行还有 %.0f 分钟", wait_sec / 60)
    run_scheduler()


if __name__ == "__main__":
    main()
