#!/usr/bin/env python3
"""
Incremental Bronze extractor for UK National Grid (NESO) Carbon Intensity
and Open-Meteo Northwich weather telemetry.

Writes idempotent daily NDJSON partitions to:
  {DATA_DIR}/bronze/<stream>/<YYYY-MM-DD>.jsonl
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

NESO_BASE = "https://api.carbonintensity.org.uk"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Northwich, Cheshire (North West England — DNO Region 3)
NORTHWICH_LAT = 53.2587
NORTHWICH_LON = -2.5180

REQUEST_TIMEOUT = 25
MAX_RETRIES = 3


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")


def get_json(session: requests.Session, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(
                url,
                params=params,
                headers={"Accept": "application/json", "User-Agent": "seanstock-data-platform/1.0"},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_err = exc
            if attempt < MAX_RETRIES:
                sleep_s = 2 ** attempt
                print(f"[extract] retry {attempt}/{MAX_RETRIES} for {url} after error: {exc}", file=sys.stderr)
                time.sleep(sleep_s)
    raise RuntimeError(f"Failed GET {url} after {MAX_RETRIES} attempts: {last_err}") from last_err


def iter_windows(start: datetime, end: datetime, step_hours: int = 24) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    cursor = start
    step = timedelta(hours=step_hours)
    while cursor < end:
        nxt = min(cursor + step, end)
        windows.append((cursor, nxt))
        cursor = nxt
    return windows


def write_partitioned_jsonl(bronze_root: Path, stream: str, records: list[dict[str, Any]], ts_field: str) -> int:
    if not records:
        return 0
    stream_dir = bronze_root / stream
    stream_dir.mkdir(parents=True, exist_ok=True)

    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        day = str(rec[ts_field])[:10]
        by_day[day].append(rec)

    written = 0
    for day, day_records in by_day.items():
        target = stream_dir / f"{day}.jsonl"
        # Append records so historical partitions keep prior runs; DuckDB deduplicates in Silver/Gold
        with target.open("a", encoding="utf-8") as fh:
            for rec in day_records:
                fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
                written += 1
    return written


def extract_national_intensity(session: requests.Session, start: datetime, end: datetime, extracted_at: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for win_start, win_end in iter_windows(start, end, step_hours=48):
        url = f"{NESO_BASE}/intensity/{iso_z(win_start)}/{iso_z(win_end)}"
        payload = get_json(session, url)
        for item in payload.get("data", []):
            intensity = item.get("intensity") or {}
            rows.append(
                {
                    "interval_start": item["from"],
                    "interval_end": item["to"],
                    "forecast_gco2": intensity.get("forecast"),
                    "actual_gco2": intensity.get("actual"),
                    "intensity_index": intensity.get("index"),
                    "extracted_at": extracted_at,
                }
            )
    return rows


def extract_national_generation(session: requests.Session, start: datetime, end: datetime, extracted_at: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    # Generation mix only exists up to current interval
    gen_end = min(end, utc_now())
    for win_start, win_end in iter_windows(start, gen_end, step_hours=48):
        url = f"{NESO_BASE}/generation/{iso_z(win_start)}/{iso_z(win_end)}"
        payload = get_json(session, url)
        for item in payload.get("data", []):
            for fuel in item.get("generationmix", []):
                rows.append(
                    {
                        "interval_start": item["from"],
                        "interval_end": item["to"],
                        "fuel": fuel.get("fuel"),
                        "perc": fuel.get("perc"),
                        "extracted_at": extracted_at,
                    }
                )
    return rows


def extract_regional_intensity(session: requests.Session, start: datetime, end: datetime, extracted_at: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for win_start, win_end in iter_windows(start, end, step_hours=24):
        url = f"{NESO_BASE}/regional/intensity/{iso_z(win_start)}/{iso_z(win_end)}"
        payload = get_json(session, url)
        for slot in payload.get("data", []):
            slot_from = slot["from"]
            slot_to = slot["to"]
            for reg in slot.get("regions", []):
                intensity = reg.get("intensity") or {}
                mix_map = {m["fuel"]: float(m["perc"]) for m in reg.get("generationmix", []) if m.get("fuel")}
                rows.append(
                    {
                        "interval_start": slot_from,
                        "interval_end": slot_to,
                        "region_id": int(reg["regionid"]),
                        "dno_region": reg.get("dnoregion"),
                        "region_name": reg.get("shortname"),
                        "forecast_gco2": intensity.get("forecast"),
                        "intensity_index": intensity.get("index"),
                        "wind_pct": mix_map.get("wind", 0.0),
                        "solar_pct": mix_map.get("solar", 0.0),
                        "hydro_pct": mix_map.get("hydro", 0.0),
                        "biomass_pct": mix_map.get("biomass", 0.0),
                        "nuclear_pct": mix_map.get("nuclear", 0.0),
                        "gas_pct": mix_map.get("gas", 0.0),
                        "coal_pct": mix_map.get("coal", 0.0),
                        "imports_pct": mix_map.get("imports", 0.0),
                        "other_pct": mix_map.get("other", 0.0),
                        "extracted_at": extracted_at,
                    }
                )
    return rows


def extract_northwich_weather(session: requests.Session, past_days: int, extracted_at: str) -> list[dict[str, Any]]:
    params = {
        "latitude": NORTHWICH_LAT,
        "longitude": NORTHWICH_LON,
        "hourly": "temperature_2m,wind_speed_10m,wind_speed_100m,wind_gusts_10m,shortwave_radiation",
        "past_days": min(max(past_days, 2), 14),
        "forecast_days": 2,
        "timezone": "UTC",
    }
    payload = get_json(session, OPEN_METEO_URL, params=params)
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    rows: list[dict[str, Any]] = []
    for idx, ts in enumerate(times):
        rows.append(
            {
                "hour_start": f"{ts}Z" if not str(ts).endswith("Z") else ts,
                "temperature_c": (hourly.get("temperature_2m") or [None])[idx],
                "wind_speed_10m_kmh": (hourly.get("wind_speed_10m") or [None])[idx],
                "wind_speed_100m_kmh": (hourly.get("wind_speed_100m") or [None])[idx],
                "wind_gusts_10m_kmh": (hourly.get("wind_gusts_10m") or [None])[idx],
                "solar_radiation_wm2": (hourly.get("shortwave_radiation") or [None])[idx],
                "extracted_at": extracted_at,
            }
        )
    return rows


def main() -> int:
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    lookback_hours = int(os.environ.get("LOOKBACK_HOURS", "48"))
    forecast_hours = int(os.environ.get("FORECAST_HOURS", "24"))

    bronze_root = data_dir / "bronze"
    now = utc_now().replace(minute=(utc_now().minute // 30) * 30, second=0, microsecond=0)
    start = now - timedelta(hours=lookback_hours)
    end = now + timedelta(hours=forecast_hours)
    extracted_at = utc_now().isoformat()

    print(f"[extract] window={iso_z(start)}..{iso_z(end)} data_dir={data_dir}")

    with requests.Session() as session:
        nat_rows = extract_national_intensity(session, start, end, extracted_at)
        gen_rows = extract_national_generation(session, start, end, extracted_at)
        reg_rows = extract_regional_intensity(session, start, end, extracted_at)
        wx_rows = extract_northwich_weather(session, past_days=max(2, lookback_hours // 24 + 1), extracted_at=extracted_at)

    c_nat = write_partitioned_jsonl(bronze_root, "national_intensity", nat_rows, "interval_start")
    c_gen = write_partitioned_jsonl(bronze_root, "national_generation", gen_rows, "interval_start")
    c_reg = write_partitioned_jsonl(bronze_root, "regional_intensity", reg_rows, "interval_start")
    c_wx = write_partitioned_jsonl(bronze_root, "northwich_weather", wx_rows, "hour_start")

    print(
        f"[extract] done: national_intensity={c_nat}, national_generation={c_gen}, "
        f"regional_intensity={c_reg}, northwich_weather={c_wx}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
