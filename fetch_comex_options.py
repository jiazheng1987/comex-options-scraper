#!/usr/bin/env python3
"""
COMEX 黄金/白银期权 Volume & Open Interest 抓取脚本（经 ScraperAPI 代理）。

所有对 CME 官网的请求均通过 ScraperAPI 转发，避免 GitHub Actions 等机房 IP 被封锁。

环境变量（必填）:
  SCRAPERAPI_KEY  ScraperAPI 账户 API Key（勿写入代码，使用 GitHub Secrets 注入）

可选:
  SCRAPERAPI_PREMIUM=true   使用住宅/高级代理池（CME 较难访问时建议开启）

用法:
  python fetch_comex_options.py --once
  python fetch_comex_options.py --once --date 2026-05-22
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
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

SCRAPERAPI_ENDPOINT = "https://api.scraperapi.com"

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

UNDERLYINGS = (
    {
        "symbol": "GC",
        "option_globex": "OG",
        "primary_name": "gold option",
        "name": "Gold",
        "referer": "https://www.cmegroup.com/markets/metals/precious/gold-futures.html",
    },
    {
        "symbol": "SI",
        "option_globex": "SO",
        "primary_name": "silver option",
        "name": "Silver",
        "referer": "https://www.cmegroup.com/markets/metals/precious/silver-futures.html",
    },
)

# CME 月份代码: F G H J K M N Q U V X Z
CME_MONTH_CODES = "FGHJKMNQUVXZ"

SETTLEMENT_PARAM_VARIANTS = (
    ("American", "AME"),
    ("Monthly", "AME"),
    ("Options", "AME"),
)

# CME 结算/成交量/持仓数据通常晚于收盘后才稳定发布。
# 默认美东 20:30 前视为“当天数据未发布”，优先抓上一工作日。
DEFAULT_DATA_RELEASE_HOUR_ET = 20
DEFAULT_DATA_RELEASE_MINUTE_ET = 30
FALLBACK_BUSINESS_DAYS = 5

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
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("comex_options")


# ---------------------------------------------------------------------------
# ScraperAPI 客户端
# ---------------------------------------------------------------------------


def get_scraperapi_key() -> str:
    key = os.environ.get("SCRAPERAPI_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "缺少环境变量 SCRAPERAPI_KEY。\n"
            "请在 GitHub 仓库 Settings → Secrets → Actions 中添加，"
            "或在本地终端 export SCRAPERAPI_KEY=你的密钥"
        )
    return key


def _use_premium_default() -> bool:
    return os.environ.get("SCRAPERAPI_PREMIUM", "").strip().lower() in {"1", "true", "yes"}


class ScraperAPIClient:
    """将目标 URL 经 ScraperAPI 转发，避免源站 IP 封禁。"""

    def __init__(
        self,
        api_key: str,
        max_retries: int = 5,
        base_delay: float = 2.0,
        premium: bool | None = None,
    ) -> None:
        self.api_key = api_key
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.premium = _use_premium_default() if premium is None else premium
        self.session = requests.Session()
        self._ua_index = 0

    def _build_target_headers(self, referer: str | None = None) -> dict[str, str]:
        ua = USER_AGENTS[self._ua_index % len(USER_AGENTS)]
        return {
            "User-Agent": ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer or f"{CME_BASE}/",
            "Origin": CME_BASE,
        }

    def _build_scraperapi_params(self, target_url: str, *, render: bool = False) -> dict[str, str]:
        params: dict[str, str] = {
            "api_key": self.api_key,
            "url": target_url,
            "keep_headers": "true",
            "country_code": "us",
        }
        if self.premium:
            params["premium"] = "true"
        if render:
            params["render"] = "true"
        return params

    def get(self, target_url: str, referer: str | None = None, *, render: bool = False) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            headers = self._build_target_headers(referer)
            params = self._build_scraperapi_params(target_url, render=render)
            try:
                log.debug("ScraperAPI → %s", target_url)
                resp = self.session.get(
                    SCRAPERAPI_ENDPOINT,
                    params=params,
                    headers=headers,
                    timeout=120,
                )
                if resp.status_code == 200:
                    # ScraperAPI 成功时返回目标站内容；若 body 含 CME 403 JSON 则视为失败
                    if self._is_cme_block(resp.text):
                        self._ua_index += 1
                        if not self.premium and attempt == 2:
                            log.warning("检测到 CME 封禁，启用 ScraperAPI premium 代理重试")
                            self.premium = True
                        delay = self.base_delay * (2 ** (attempt - 1)) + random.uniform(0.5, 1.5)
                        log.warning("CME 403 in body (attempt %s/%s), retry in %.1fs", attempt, self.max_retries, delay)
                        time.sleep(delay)
                        continue
                    return resp

                if resp.status_code == 404:
                    raise RuntimeError(
                        f"目标 URL 返回 404（可能已下线）: {target_url}"
                    ) from requests.HTTPError(f"404 {resp.text[:200]}")

                if resp.status_code in (403, 429, 500, 503):
                    self._ua_index += 1
                    if not self.premium and resp.status_code in (403, 429):
                        self.premium = True
                        log.warning("ScraperAPI HTTP %s，启用 premium 重试", resp.status_code)
                    delay = self.base_delay * (2 ** (attempt - 1)) + random.uniform(0.5, 1.5)
                    log.warning(
                        "ScraperAPI HTTP %s (attempt %s/%s), retry in %.1fs",
                        resp.status_code,
                        attempt,
                        self.max_retries,
                        delay,
                    )
                    time.sleep(delay)
                    last_error = requests.HTTPError(f"{resp.status_code} {resp.text[:200]}")
                    continue
                resp.raise_for_status()
            except requests.RequestException as exc:
                last_error = exc
                delay = self.base_delay * (2 ** (attempt - 1))
                log.warning("ScraperAPI 请求异常 (attempt %s/%s): %s", attempt, self.max_retries, exc)
                time.sleep(delay)
        raise RuntimeError(f"经 ScraperAPI 访问失败，已重试 {self.max_retries} 次: {target_url}") from last_error

    @staticmethod
    def _is_cme_block(text: str) -> bool:
        lower = text.lower()
        return "blocked" in lower and "scraping" in lower

    def get_json(self, target_url: str, referer: str | None = None) -> Any:
        return self.get(target_url, referer=referer).json()

    def get_text(self, target_url: str, referer: str | None = None, *, render: bool = False) -> str:
        return self.get(target_url, referer=referer, render=render).text


# ---------------------------------------------------------------------------
# 解析工具
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
    return str(entry.get("optionType") or entry.get("optionTypeName") or "Unknown")


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
# CME 数据抓取（经 ScraperAPI）
# ---------------------------------------------------------------------------


def get_future_product_id(client: ScraperAPIClient, symbol: str, referer: str) -> int:
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


def _normalize_expiration_code(code: Any, exp_date: Any = None) -> str | None:
    text = str(code or "").strip().upper()
    if len(text) == 2 and text[0] in CME_MONTH_CODES and text[1].isdigit():
        return text
    parsed = pd.to_datetime(exp_date, errors="coerce")
    if pd.notna(parsed):
        return CME_MONTH_CODES[parsed.month - 1] + str(parsed.year)[-1]
    return None


def _is_excluded_option_name(name: str) -> bool:
    lowered = name.lower()
    return any(k in lowered for k in ("weekly", "daily", "micro", "e-mini", "emini"))


def _previous_business_day(day: date) -> date:
    candidate = day - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def _previous_business_days(day: date, count: int) -> list[date]:
    days: list[date] = []
    candidate = day
    while len(days) < count:
        candidate = _previous_business_day(candidate)
        days.append(candidate)
    return days


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        log.warning("环境变量 %s=%r 不是整数，使用默认值 %s", name, raw, default)
        return default


def _data_release_cutoff(now_et: datetime) -> datetime:
    hour = _env_int("CME_DATA_RELEASE_HOUR_ET", DEFAULT_DATA_RELEASE_HOUR_ET)
    minute = _env_int("CME_DATA_RELEASE_MINUTE_ET", DEFAULT_DATA_RELEASE_MINUTE_ET)
    hour = min(max(hour, 0), 23)
    minute = min(max(minute, 0), 59)
    return now_et.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _candidate_trade_dates(trade_date: date | None) -> list[date]:
    if trade_date:
        return [trade_date]

    now_et = datetime.now(tz=ET)
    today = now_et.date()
    prior_business_days = _previous_business_days(today, FALLBACK_BUSINESS_DAYS)
    if now_et >= _data_release_cutoff(now_et):
        dates = [today, *prior_business_days]
        log.info("已过 CME 数据发布时间，先尝试当天交易日，再回退: %s", dates)
    else:
        dates = prior_business_days
        log.info("未到 CME 数据发布时间，跳过当天交易日，改查最近工作日: %s", dates)

    unique: list[date] = []
    for d in dates:
        if d not in unique:
            unique.append(d)
    return unique


def _truthy_flag(entry: dict[str, Any], *keys: str) -> bool:
    for key in keys:
        value = entry.get(key)
        if value is True:
            return True
        if isinstance(value, str) and value.strip().lower() in {"1", "true", "yes"}:
            return True
    return False


def _product_field(entry: dict[str, Any], *keys: str) -> Any:
    return _pick(entry, *keys)


def _product_id(entry: dict[str, Any]) -> int | None:
    raw = _product_field(entry, "productId", "ProductId", "product_id", "id", "Id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _product_display_name(entry: dict[str, Any], fallback: str) -> str:
    return str(
        _product_field(entry, "productName", "ProductName", "name", "Name", "label", "Label") or fallback
    )


def _is_monthly_ame_option(entry: dict[str, Any]) -> bool:
    """排除 daily/weekly/sto，只保留美式月度期权。"""
    if _truthy_flag(entry, "daily", "Daily"):
        return False
    if _truthy_flag(entry, "weekly", "Weekly"):
        return False
    if _truthy_flag(entry, "sto", "Sto"):
        return False

    name = _product_display_name(entry, "").strip().lower()
    if _is_excluded_option_name(name):
        return False

    option_type = str(_product_field(entry, "optionType", "OptionType") or "").upper()
    return option_type == "AME"


def _is_primary_option_product(entry: dict[str, Any], underlying_cfg: dict[str, str]) -> bool:
    if not _is_monthly_ame_option(entry):
        return False

    target = underlying_cfg["primary_name"]
    option_globex = underlying_cfg["option_globex"].upper()
    name = _product_display_name(entry, "").strip().lower()
    globex = str(_product_field(entry, "globex", "Globex", "productCode", "ProductCode") or "").upper()
    return name == target or globex == option_globex or target in name


def _select_primary_option_products(
    products: list[dict[str, Any]],
    underlying_cfg: dict[str, str],
) -> list[dict[str, Any]]:
    """只选 Gold Option / Silver Option 主合约，绝不回退到 Weekly。"""
    names_preview = [
        f"{_product_display_name(p, '?')} (id={_product_id(p)}, weekly={_truthy_flag(p, 'weekly', 'Weekly')})"
        for p in products[:20]
    ]
    log.info("CME 期权产品列表(%s): %s", underlying_cfg["symbol"], "; ".join(names_preview))

    matched = [p for p in products if _is_primary_option_product(p, underlying_cfg)]
    if not matched:
        matched = [p for p in products if _is_monthly_ame_option(p)]
        if matched:
            log.warning(
                "未精确匹配 %s 主期权名称，改抓非 Weekly 的 AME 月度合约: %s",
                underlying_cfg["symbol"],
                [_product_display_name(m, "?") for m in matched],
            )
    if matched:
        log.info("选中月度期权 %s: %s", underlying_cfg["symbol"], [_product_display_name(m, "?") for m in matched])
    else:
        log.error(
            "未找到 %s 月度期权（需 daily/weekly/sto=false 且 optionType=AME），已跳过 Weekly 产品",
            underlying_cfg["symbol"],
        )
    return matched


def get_option_products_raw(client: ScraperAPIClient, future_product_id: int, referer: str) -> list[dict[str, Any]]:
    url = TRADE_DATES_URL.format(product_id=future_product_id)
    payload = client.get_json(url, referer=referer)
    if not isinstance(payload, list):
        raise RuntimeError(f"TradeDateAndExpirations 返回异常: {type(payload)}")
    return payload


def _extract_expiration_items(opt_product: dict[str, Any]) -> list[tuple[str, str | None]]:
    """从 TradeDateAndExpirations 条目解析到期月份代码。"""
    items: list[tuple[str, str | None]] = []
    expirations = (
        opt_product.get("expirations")
        or opt_product.get("Expirations")
        or opt_product.get("expirationList")
        or []
    )
    for exp in expirations:
        if not isinstance(exp, dict):
            continue
        code = _pick(exp, "code", "Code", "expirationCode", "ExpirationCode")
        exp_date = _pick(exp, "expirationDate", "ExpirationDate", "expiry", "Expiry")

        nested = exp.get("expiration") or exp.get("Expiration")
        if isinstance(nested, dict):
            code = code or _pick(nested, "code", "Code", "expirationCode", "ExpirationCode")
            exp_date = exp_date or _pick(
                nested,
                "expiration",
                "expirationDate",
                "ExpirationDate",
                "expiry",
                "Expiry",
            )
            if not exp_date:
                year = _pick(nested, "year", "Year")
                month = _pick(nested, "month", "Month")
                if year and month:
                    try:
                        exp_date = date(int(year), int(month), 1).isoformat()
                    except (TypeError, ValueError):
                        pass
        elif nested and not exp_date:
            exp_date = nested

        if isinstance(exp_date, dict):
            exp_date = _pick(exp_date, "expiration", "expirationDate", "ExpirationDate")

        norm_code = _normalize_expiration_code(code, exp_date)
        if norm_code:
            items.append((norm_code, str(exp_date) if exp_date else None))
    return items


def get_expiration_codes(client: ScraperAPIClient, option_product_id: int, referer: str) -> list[tuple[str, str | None]]:
    """备用接口（CME 可能已下线）；失败时返回空列表，不中断主流程。"""
    url = OPTION_CATEGORIES_URL.format(option_id=option_product_id)
    try:
        payload = client.get_json(url, referer=referer)
    except RuntimeError as exc:
        log.warning(
            "Options/Categories 备用接口不可用 (product %s)，已跳过: %s",
            option_product_id,
            exc,
        )
        return []
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
    quotes = (
        payload.get("optionContractQuotes")
        or payload.get("OptionContractQuotes")
        or payload.get("quotes")
        or payload.get("Quotes")
        or []
    )
    for entry in quotes:
        strike = _clean_float(_pick(entry, "strikePrice", "options-strikePrice", "strike"))
        if strike is None:
            continue

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
                        _pick(side, "openInterest", "priorDayOpenInterest", "options-openInterest")
                    ),
                    "source": "cme_quotes_api",
                    "fetched_at": fetched_at,
                }
            )
        if handled:
            continue

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
                    "source": "cme_quotes_api",
                    "fetched_at": fetched_at,
                }
            )
    return rows


def fetch_from_quotes_api(
    client: ScraperAPIClient,
    underlying_cfg: dict[str, str],
    trade_date: date | None = None,
) -> list[dict[str, Any]]:
    symbol = underlying_cfg["symbol"]
    referer = underlying_cfg["referer"]
    fetched_at = datetime.now(tz=ET).isoformat()
    trade_date_str = (trade_date or date.today()).isoformat()

    future_product_id = get_future_product_id(client, symbol, referer)
    all_products = get_option_products_raw(client, future_product_id, referer)
    option_products = _select_primary_option_products(all_products, underlying_cfg)

    rows: list[dict[str, Any]] = []
    for opt_product in option_products:
        option_product_id = _product_id(opt_product)
        if not option_product_id:
            log.warning("跳过无 productId 的期权条目: %s", _product_display_name(opt_product, "?"))
            continue
        option_type = _option_type_label(opt_product)
        product_name = _product_display_name(opt_product, f"{underlying_cfg['name']} Option")

        expiration_items = _extract_expiration_items(opt_product)
        if not expiration_items:
            expiration_items = get_expiration_codes(client, option_product_id, referer)

        if not expiration_items:
            log.warning("跳过 %s (product %s): 未解析到期权到期月份", product_name, option_product_id)
            continue

        log.info("%s 共 %s 个到期月份待抓取", product_name, len(expiration_items))
        for expiration_code, expiration_date in expiration_items[:12]:
            ts = int(time.time() * 1000)
            url = OPTION_QUOTES_URL.format(
                option_id=option_product_id,
                expiration_code=expiration_code,
                ts=ts,
            )
            try:
                payload = client.get_json(url, referer=referer)
            except RuntimeError as exc:
                if "404" in str(exc):
                    log.info("Quotes 404，跳过 %s %s", product_name, expiration_code)
                else:
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
                log.info("Quotes API: %s %s %s -> %s 条", symbol, option_type, expiration_code, len(parsed))
                rows.extend(parsed)
            time.sleep(1.0)

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
    tables = soup.find_all("table")
    if not tables:
        return []

    all_rows: list[dict[str, Any]] = []
    for table in tables:
        rows = _parse_settlement_table(
            table,
            underlying=underlying,
            product_name=product_name,
            option_type=option_type,
            trade_date=trade_date,
            fetched_at=fetched_at,
        )
        if rows:
            all_rows.extend(rows)
    return all_rows


def _parse_settlement_table(
    table: Any,
    *,
    underlying: str,
    product_name: str,
    option_type: str,
    trade_date: str,
    fetched_at: str,
) -> list[dict[str, Any]]:
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
        strike = _clean_float(cells[idx_strike] if idx_strike is not None and idx_strike < len(cells) else cells[0])
        if strike is None:
            continue
        right = _right_label(cells[idx_type] if idx_type is not None and idx_type < len(cells) else None)
        if not right:
            continue
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
                "volume": _clean_int(cells[idx_volume]) if idx_volume is not None and idx_volume < len(cells) else None,
                "open_interest": _clean_int(cells[idx_oi]) if idx_oi is not None and idx_oi < len(cells) else None,
                "source": "cme_daily_settlement",
                "fetched_at": fetched_at,
            }
        )
    return rows


def fetch_from_daily_settlement(
    client: ScraperAPIClient,
    underlying_cfg: dict[str, str],
    trade_date: date | None = None,
) -> list[dict[str, Any]]:
    symbol = underlying_cfg["symbol"]
    referer = underlying_cfg["referer"]
    fetched_at = datetime.now(tz=ET).isoformat()
    td = trade_date or date.today()
    trade_date_fmt = td.strftime("%m/%d/%Y")
    trade_date_str = td.isoformat()

    future_product_id = get_future_product_id(client, symbol, referer)
    all_products = get_option_products_raw(client, future_product_id, referer)
    primary_products = _select_primary_option_products(all_products, underlying_cfg)

    rows: list[dict[str, Any]] = []
    option_code = underlying_cfg["option_globex"]

    for opt_product in primary_products:
        option_product_id = _product_id(opt_product)
        if not option_product_id:
            continue
        product_name = _product_display_name(opt_product, f"{underlying_cfg['name']} Option")

        for type_name, type_code in SETTLEMENT_PARAM_VARIANTS:
            url = DAILY_SETTLEMENT_URL.format(
                product_code=option_code,
                underlying=symbol,
                product_id=option_product_id,
                trade_date=trade_date_fmt,
                option_type_name=type_name,
                option_type=type_code,
            )
            try:
                html = client.get_text(url, referer=referer, render=True)
            except RuntimeError as exc:
                log.info("Daily Settlement 尝试失败 %s (%s/%s): %s", product_name, type_name, type_code, exc)
                continue

            parsed = _parse_daily_settlement_html(
                html,
                underlying=symbol,
                product_name=product_name,
                option_type=type_name,
                trade_date=trade_date_str,
                fetched_at=fetched_at,
            )
            if parsed:
                log.info("Daily Settlement 成功: %s (%s/%s) -> %s 条", product_name, type_name, type_code, len(parsed))
                rows.extend(parsed)
                break
            snippet = " ".join(html.split())[:240]
            log.info(
                "Daily Settlement 无表格: %s (%s/%s)，HTML 长度=%s，片段=%s",
                product_name,
                type_name,
                type_code,
                len(html),
                snippet,
            )
            time.sleep(0.5)
        time.sleep(0.5)
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
# 主流程
# ---------------------------------------------------------------------------


def fetch_underlying_rows(
    client: ScraperAPIClient,
    cfg: dict[str, str],
    trade_date: date,
) -> list[dict[str, Any]]:
    rows = fetch_from_quotes_api(client, cfg, trade_date=trade_date)
    if rows:
        return rows
    log.info("%s Quotes API 无数据 (trade_date=%s)，尝试 Daily Settlement...", cfg["symbol"], trade_date)
    return fetch_from_daily_settlement(client, cfg, trade_date=trade_date)


def run_fetch(trade_date: date | None = None) -> None:
    log.info("Scraper 版本: 2026-06-23-monthly-ame-v4（按发布时间选择交易日，未发布则回退前一工作日）")
    api_key = get_scraperapi_key()
    client = ScraperAPIClient(api_key)
    all_rows: list[dict[str, Any]] = []

    for cfg in UNDERLYINGS:
        log.info("开始抓取 %s (%s)...", cfg["name"], cfg["symbol"])
        try:
            rows: list[dict[str, Any]] = []
            for candidate_date in _candidate_trade_dates(trade_date):
                rows = fetch_underlying_rows(client, cfg, candidate_date)
                if rows:
                    log.info("%s 使用交易日 %s 获得 %s 条", cfg["symbol"], candidate_date, len(rows))
                    break
                log.info("%s 交易日 %s 无数据，继续尝试其他日期", cfg["symbol"], candidate_date)
            all_rows.extend(rows)
        except Exception as exc:
            log.error("抓取 %s 失败: %s", cfg["symbol"], exc, exc_info=True)

    count = append_deduped_rows(all_rows)
    if not all_rows or not OUTPUT_CSV.is_file():
        log.error(
            "抓取未产出 CSV：解析行数=%s，文件存在=%s，路径=%s",
            len(all_rows),
            OUTPUT_CSV.is_file(),
            OUTPUT_CSV,
        )
        sys.exit(1)
    log.info("抓取完成，本次写入 %s 行 → %s", count, OUTPUT_CSV)


def run_scheduler() -> None:
    try:
        import schedule
    except ImportError:
        log.error("缺少 schedule 库，请运行: pip install -r requirements.txt")
        sys.exit(1)

    schedule.every().day.at("17:00", "America/New_York").do(lambda: run_fetch())
    log.info("定时任务已启动：每天美东 17:00，输出 %s", OUTPUT_CSV)
    while True:
        schedule.run_pending()
        time.sleep(30)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="COMEX 期权抓取（ScraperAPI 代理）")
    parser.add_argument("--once", action="store_true", help="立即执行一次")
    parser.add_argument("--date", type=str, default=None, help="交易日 YYYY-MM-DD")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    trade_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
    if args.once:
        run_fetch(trade_date=trade_date)
        return
    run_scheduler()


if __name__ == "__main__":
    main()
