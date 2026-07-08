#!/usr/bin/env python3
"""
Use Databento SDK to fetch COMEX gold options OI and compute max pain.

Environment variables:
  DATABENTO_API_KEY   Databento API key (required unless --api-key is passed)

Examples:
  python fetch_databento_max_pain.py
  python fetch_databento_max_pain.py --parent OG.OPT --lookback-days 21
"""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import databento as db
import pandas as pd


NANO_SCALE = 1_000_000_000
OPEN_INTEREST_STAT_TYPE = 9


def _resolve_api_key(cli_key: str | None) -> str:
    key = (cli_key or os.getenv("DATABENTO_API_KEY", "")).strip()
    if not key:
        raise RuntimeError("Missing Databento API key. Set DATABENTO_API_KEY or pass --api-key.")
    return key


def _date_window(lookback_days: int) -> tuple[str, str]:
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=lookback_days)
    return start_dt.date().isoformat(), end_dt.date().isoformat()


def _date_window_from_end(lookback_days: int, end: str | None) -> tuple[str, str]:
    if end:
        end_ts = pd.Timestamp(end)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize("UTC")
        else:
            end_ts = end_ts.tz_convert("UTC")
        start_ts = end_ts - pd.Timedelta(days=lookback_days)
        return start_ts.isoformat(), end_ts.isoformat()
    return _date_window(lookback_days)


def _extract_entitlement_cutoff(error_text: str) -> str | None:
    # Example from Databento:
    # "Try again with an end time before 2026-07-07T23:29:34.923663000Z"
    match = re.search(r"before\s+([0-9T:\.\-]+Z)", error_text)
    if not match:
        return None
    return match.group(1)


def _get_range_with_entitlement_retry(
    client: db.Historical,
    *,
    dataset: str,
    schema: str,
    symbols: list[str],
    stype_in: str,
    start: str,
    end: str,
) -> tuple[pd.DataFrame, str]:
    try:
        df = client.timeseries.get_range(
            dataset=dataset,
            schema=schema,
            symbols=symbols,
            stype_in=stype_in,
            start=start,
            end=end,
        ).to_df()
        return df, end
    except Exception as exc:
        text = str(exc)
        cutoff = _extract_entitlement_cutoff(text)
        if not cutoff:
            raise
        print(f"[WARN] Entitlement window exceeded for schema={schema}. Retrying with end={cutoff}")
        df = client.timeseries.get_range(
            dataset=dataset,
            schema=schema,
            symbols=symbols,
            stype_in=stype_in,
            start=start,
            end=cutoff,
        ).to_df()
        return df, cutoff


def _normalize_strike(df: pd.DataFrame) -> pd.Series:
    if "strike_price" not in df.columns:
        raise RuntimeError("Definition response missing strike_price column.")
    return pd.to_numeric(df["strike_price"], errors="coerce") / NANO_SCALE


def _get_expiration_col(df: pd.DataFrame) -> str | None:
    for col in ("expiration", "expiration_date", "maturity_date", "maturity"):
        if col in df.columns:
            return col
    return None


def _prepare_definition_latest(df_def: pd.DataFrame) -> pd.DataFrame:
    if df_def.empty:
        raise RuntimeError("No instrument definitions returned.")
    required = {"instrument_id", "instrument_class"}
    missing = required - set(df_def.columns)
    if missing:
        raise RuntimeError(f"Definition response missing columns: {sorted(missing)}")

    df = df_def.copy()
    df = df[df["instrument_class"].isin(["C", "P"])]
    df["strike"] = _normalize_strike(df)
    df = df[df["strike"].notna()]
    if df.empty:
        raise RuntimeError("No call/put option definitions with valid strikes.")

    if "ts_recv" in df.columns:
        df = df.sort_values(["instrument_id", "ts_recv"]).drop_duplicates("instrument_id", keep="last")
    else:
        df = df.drop_duplicates("instrument_id", keep="last")

    exp_col = _get_expiration_col(df)
    if exp_col:
        df["expiration"] = pd.to_datetime(df[exp_col], errors="coerce").dt.date
    else:
        df["expiration"] = pd.NaT

    if "raw_symbol" not in df.columns:
        df["raw_symbol"] = df["instrument_id"].astype(str)

    return df[["instrument_id", "instrument_class", "strike", "expiration", "raw_symbol"]]


def _prepare_latest_oi(df_stats: pd.DataFrame) -> tuple[pd.DataFrame, Any]:
    if df_stats.empty:
        raise RuntimeError("No statistics returned.")
    required = {"instrument_id", "stat_type"}
    missing = required - set(df_stats.columns)
    if missing:
        raise RuntimeError(f"Statistics response missing columns: {sorted(missing)}")
    if "quantity" not in df_stats.columns:
        raise RuntimeError("Statistics response missing quantity column for OI.")

    df = df_stats.copy()
    df = df[df["stat_type"] == OPEN_INTEREST_STAT_TYPE]
    df = df[df["quantity"].notna()]
    if df.empty:
        raise RuntimeError("No open interest records found (stat_type=9).")

    ts_ref_col = "ts_ref" if "ts_ref" in df.columns else "ts_event"
    if ts_ref_col not in df.columns:
        raise RuntimeError("Statistics response missing ts_ref/ts_event for trade-date selection.")

    latest_ref = df[ts_ref_col].max()
    df = df[df[ts_ref_col] == latest_ref]
    if "ts_recv" in df.columns:
        df = df.sort_values(["instrument_id", "ts_recv"]).drop_duplicates("instrument_id", keep="last")
    else:
        df = df.drop_duplicates("instrument_id", keep="last")

    df["open_interest"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = df[df["open_interest"].notna() & (df["open_interest"] > 0)]
    if df.empty:
        raise RuntimeError("Latest OI snapshot exists but has no positive OI.")

    return df[["instrument_id", "open_interest"]], latest_ref


def _max_pain_strike(df: pd.DataFrame) -> tuple[float, float]:
    strikes = sorted(df["strike"].dropna().unique())
    if not strikes:
        raise RuntimeError("No strikes available for max pain calculation.")

    calls = df[df["instrument_class"] == "C"][["strike", "open_interest"]]
    puts = df[df["instrument_class"] == "P"][["strike", "open_interest"]]

    best_strike = strikes[0]
    best_payout = float("inf")
    for settle in strikes:
        call_pay = ((settle - calls["strike"]).clip(lower=0) * calls["open_interest"]).sum()
        put_pay = ((puts["strike"] - settle).clip(lower=0) * puts["open_interest"]).sum()
        total = float(call_pay + put_pay)
        if total < best_payout:
            best_payout = total
            best_strike = float(settle)
    return best_strike, best_payout


def compute_max_pain_by_expiration(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for expiration, sub in df.groupby("expiration", dropna=False):
        strike, payout = _max_pain_strike(sub)
        rows.append(
            {
                "expiration": expiration,
                "contracts": int(sub["open_interest"].sum()),
                "num_strikes": int(sub["strike"].nunique()),
                "max_pain_strike": strike,
                "holder_payout_proxy": payout,
            }
        )
    out = pd.DataFrame(rows).sort_values("expiration", na_position="last").reset_index(drop=True)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch latest COMEX gold options OI from Databento and compute max pain.")
    parser.add_argument("--api-key", type=str, default=None, help="Databento API key. Optional if DATABENTO_API_KEY is set.")
    parser.add_argument("--dataset", type=str, default="GLBX.MDP3", help="Databento dataset. Default: GLBX.MDP3")
    parser.add_argument("--parent", type=str, default="OG.OPT", help="Parent symbol for options product. Default: OG.OPT")
    parser.add_argument("--lookback-days", type=int, default=14, help="Historical lookback window to find latest OI.")
    parser.add_argument("--end", type=str, default=None, help="Optional end timestamp/date (UTC), e.g. 2026-07-07T23:29:34Z")
    parser.add_argument("--output", type=str, default="databento_og_max_pain.csv", help="CSV output path for max pain by expiration.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key = _resolve_api_key(args.api_key)
    start, end = _date_window_from_end(args.lookback_days, args.end)
    print(f"[INFO] Querying {args.dataset} {args.parent} from {start} to {end}")

    client = db.Historical(api_key)

    df_def_raw, used_end = _get_range_with_entitlement_retry(
        client,
        dataset=args.dataset,
        schema="definition",
        symbols=[args.parent],
        stype_in="parent",
        start=start,
        end=end,
    )
    df_def = _prepare_definition_latest(df_def_raw)
    print(f"[INFO] Definitions: {len(df_def)} option instruments")

    df_stats_raw, used_end_stats = _get_range_with_entitlement_retry(
        client,
        dataset=args.dataset,
        schema="statistics",
        symbols=[args.parent],
        stype_in="parent",
        start=start,
        end=used_end,
    )
    if used_end_stats != used_end:
        print(f"[WARN] Statistics used a stricter entitlement end: {used_end_stats}")
    df_oi, latest_ref = _prepare_latest_oi(df_stats_raw)
    print(f"[INFO] Latest OI reference timestamp: {latest_ref}")
    print(f"[INFO] OI instruments in latest snapshot: {len(df_oi)}")

    df = df_def.merge(df_oi, on="instrument_id", how="inner")
    if df.empty:
        raise RuntimeError("No overlap between definition and OI instruments.")

    max_pain_by_exp = compute_max_pain_by_expiration(df)
    max_pain_by_exp.to_csv(args.output, index=False)

    nearest = max_pain_by_exp.dropna(subset=["expiration"]).sort_values("expiration").head(1)
    if nearest.empty:
        headline = max_pain_by_exp.head(1)
    else:
        headline = nearest

    row = headline.iloc[0]
    print("\n=== Max Pain (Nearest Expiration) ===")
    print(f"expiration: {row['expiration']}")
    print(f"max_pain_strike: {row['max_pain_strike']}")
    print(f"contracts_sum_oi: {int(row['contracts'])}")
    print(f"payout_proxy: {row['holder_payout_proxy']:.2f}")
    print(f"\n[INFO] Full results written to: {args.output}")


if __name__ == "__main__":
    main()

