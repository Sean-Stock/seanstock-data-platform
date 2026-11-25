#!/usr/bin/env python3
"""
DuckDB Silver/Gold transformation, Data Quality checks, and atomic Parquet swap.

Reads Bronze NDJSON from {DATA_DIR}/bronze/*/*.jsonl, builds deduplicated Gold
Parquet marts in a staging directory, validates DQ assertions, atomically swaps
into {DATA_DIR}/gold/, rebuilds read-only views in {DATA_DIR}/views.duckdb, and
emits {DATA_DIR}/gold/pipeline-meta.json for portfolio telemetry.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def run_dq_checks(con: duckdb.DuckDBPyConnection, staging: Path) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []

    def assert_check(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})
        status = "PASS" if passed else "FAIL"
        print(f"[dq] {status} | {name}: {detail}")
        if not passed:
            raise AssertionError(f"Data quality check failed [{name}]: {detail}")

    nat_cnt = con.execute(f"SELECT COUNT(*) FROM read_parquet('{staging / 'gold_national_intensity.parquet'}')").fetchone()[0]
    reg_cnt = con.execute(f"SELECT COUNT(*) FROM read_parquet('{staging / 'gold_regional_mix.parquet'}')").fetchone()[0]
    nw_cnt = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{staging / 'gold_regional_mix.parquet'}') WHERE is_north_west = TRUE"
    ).fetchone()[0]
    wx_cnt = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{staging / 'gold_northwest_weather_energy.parquet'}')"
    ).fetchone()[0]

    assert_check("national_intensity_non_empty", nat_cnt > 0, f"rows={nat_cnt}")
    assert_check("regional_mix_non_empty", reg_cnt > 0, f"rows={reg_cnt}")
    assert_check("north_west_region_present", nw_cnt > 0, f"region_id=3 rows={nw_cnt}")
    assert_check("weather_energy_join_non_empty", wx_cnt > 0, f"joined_hours={wx_cnt}")

    bad_fuel_sum = con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{staging / 'gold_regional_mix.parquet'}')
        WHERE (wind_pct + solar_pct + hydro_pct + biomass_pct + nuclear_pct + gas_pct + coal_pct + imports_pct + other_pct)
              NOT BETWEEN 95.0 AND 105.0
        """
    ).fetchone()[0]
    assert_check("regional_fuel_mix_sums_to_100", bad_fuel_sum == 0, f"out_of_bounds_rows={bad_fuel_sum}")

    latest_ts = con.execute(
        f"SELECT MAX(interval_start) FROM read_parquet('{staging / 'gold_regional_mix.parquet'}') WHERE interval_start <= CURRENT_TIMESTAMP"
    ).fetchone()[0]
    assert_check("freshness_not_null", latest_ts is not None, f"latest_interval={latest_ts}")
    if latest_ts is not None:
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
        lag_minutes = (utc_now() - latest_ts).total_seconds() / 60.0
        assert_check("freshness_under_4h", lag_minutes < 240, f"lag_minutes={lag_minutes:.1f}")

    return checks


def rebuild_superset_views(data_dir: Path, superset_data_dir: str) -> None:
    """Create a tiny read-only DuckDB catalog whose views resolve Gold Parquet files."""
    tmp_db = data_dir / ".views.next.duckdb"
    if tmp_db.exists():
        tmp_db.unlink()

    gold_prefix = f"{superset_data_dir.rstrip('/')}/gold"
    vcon = duckdb.connect(str(tmp_db))
    try:
        views = {
            "gold_national_intensity": f"{gold_prefix}/gold_national_intensity.parquet",
            "gold_regional_mix": f"{gold_prefix}/gold_regional_mix.parquet",
            "gold_national_generation": f"{gold_prefix}/gold_national_generation.parquet",
            "gold_northwest_weather_energy": f"{gold_prefix}/gold_northwest_weather_energy.parquet",
            "gold_pipeline_runs": f"{gold_prefix}/gold_pipeline_runs.parquet",
            "gold_dag_dispatch_runs": f"{gold_prefix}/gold_dag_dispatch_runs.parquet",
        }
        for view_name, parquet_path in views.items():
            vcon.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_path}');")
    finally:
        vcon.close()

    os.replace(tmp_db, data_dir / "views.duckdb")


def main() -> int:
    started = time.perf_counter()
    run_ts = utc_now()
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    superset_data_dir = os.environ.get("SUPERSET_DATA_DIR", str(data_dir))
    bronze_dir = data_dir / "bronze"
    gold_dir = data_dir / "gold"
    staging = gold_dir / ".staging"

    gold_dir.mkdir(parents=True, exist_ok=True)
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone = 'UTC';")

    # Helper SQL expression normalizing ISO-8601 strings missing seconds (e.g. "2026-11-22T21:00Z")
    # so DuckDB parses them identically across all DuckDB 1.x releases.
    def ts_expr(col: str) -> str:
        return (
            f"CAST(regexp_replace(CAST({col} AS VARCHAR), "
            f"'^([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}})Z$', '\\1:00Z') AS TIMESTAMPTZ)"
        )

    # 1. Gold National Intensity (deduplicated + forecast accuracy metrics)
    con.execute(
        f"""
        COPY (
            WITH raw AS (
                SELECT
                    {ts_expr('interval_start')} AS interval_start,
                    {ts_expr('interval_end')} AS interval_end,
                    CAST(forecast_gco2 AS DOUBLE) AS forecast_gco2,
                    CAST(actual_gco2 AS DOUBLE) AS actual_gco2,
                    CAST(intensity_index AS VARCHAR) AS intensity_index,
                    {ts_expr('extracted_at')} AS extracted_at
                FROM read_json_auto('{bronze_dir / 'national_intensity' / '*.jsonl'}')
            ),
            dedup AS (
                SELECT *
                FROM raw
                QUALIFY ROW_NUMBER() OVER (PARTITION BY interval_start ORDER BY extracted_at DESC) = 1
            )
            SELECT
                interval_start,
                interval_end,
                forecast_gco2,
                actual_gco2,
                CASE WHEN actual_gco2 IS NOT NULL THEN actual_gco2 - forecast_gco2 END AS forecast_error,
                CASE WHEN actual_gco2 IS NOT NULL THEN ABS(actual_gco2 - forecast_gco2) END AS abs_forecast_error,
                intensity_index,
                (actual_gco2 IS NULL) AS is_forecast_only,
                extracted_at
            FROM dedup
            ORDER BY interval_start
        ) TO '{staging / 'gold_national_intensity.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    # 2. Gold Regional Mix (all 17 GB DNO regions, North West = region_id 3)
    con.execute(
        f"""
        COPY (
            WITH raw AS (
                SELECT
                    {ts_expr('interval_start')} AS interval_start,
                    {ts_expr('interval_end')} AS interval_end,
                    CAST(region_id AS INTEGER) AS region_id,
                    CAST(dno_region AS VARCHAR) AS dno_region,
                    CAST(region_name AS VARCHAR) AS region_name,
                    CAST(forecast_gco2 AS DOUBLE) AS forecast_gco2,
                    CAST(intensity_index AS VARCHAR) AS intensity_index,
                    CAST(wind_pct AS DOUBLE) AS wind_pct,
                    CAST(solar_pct AS DOUBLE) AS solar_pct,
                    CAST(hydro_pct AS DOUBLE) AS hydro_pct,
                    CAST(biomass_pct AS DOUBLE) AS biomass_pct,
                    CAST(nuclear_pct AS DOUBLE) AS nuclear_pct,
                    CAST(gas_pct AS DOUBLE) AS gas_pct,
                    CAST(coal_pct AS DOUBLE) AS coal_pct,
                    CAST(imports_pct AS DOUBLE) AS imports_pct,
                    CAST(other_pct AS DOUBLE) AS other_pct,
                    {ts_expr('extracted_at')} AS extracted_at
                FROM read_json_auto('{bronze_dir / 'regional_intensity' / '*.jsonl'}')
            ),
            dedup AS (
                SELECT *
                FROM raw
                QUALIFY ROW_NUMBER() OVER (PARTITION BY interval_start, region_id ORDER BY extracted_at DESC) = 1
            )
            SELECT
                interval_start,
                interval_end,
                region_id,
                dno_region,
                region_name,
                (region_id = 3) AS is_north_west,
                forecast_gco2,
                intensity_index,
                wind_pct,
                solar_pct,
                hydro_pct,
                biomass_pct,
                nuclear_pct,
                gas_pct,
                coal_pct,
                imports_pct,
                other_pct,
                ROUND(wind_pct + solar_pct + hydro_pct + biomass_pct, 2) AS renewable_pct,
                ROUND(wind_pct + solar_pct + hydro_pct + biomass_pct + nuclear_pct, 2) AS low_carbon_pct,
                extracted_at
            FROM dedup
            ORDER BY interval_start, region_id
        ) TO '{staging / 'gold_regional_mix.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    # 3. Gold National Generation Mix (long format for stacked area charts)
    con.execute(
        f"""
        COPY (
            WITH raw AS (
                SELECT
                    {ts_expr('interval_start')} AS interval_start,
                    {ts_expr('interval_end')} AS interval_end,
                    CAST(fuel AS VARCHAR) AS fuel,
                    CAST(perc AS DOUBLE) AS perc,
                    {ts_expr('extracted_at')} AS extracted_at
                FROM read_json_auto('{bronze_dir / 'national_generation' / '*.jsonl'}')
            )
            SELECT interval_start, interval_end, fuel, perc, extracted_at
            FROM raw
            QUALIFY ROW_NUMBER() OVER (PARTITION BY interval_start, fuel ORDER BY extracted_at DESC) = 1
            ORDER BY interval_start, fuel
        ) TO '{staging / 'gold_national_generation.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    # 4. Gold North West Weather x Renewables Correlation (hourly grain)
    con.execute(
        f"""
        COPY (
            WITH wx_raw AS (
                SELECT
                    {ts_expr('hour_start')} AS hour_start,
                    CAST(temperature_c AS DOUBLE) AS temperature_c,
                    CAST(wind_speed_10m_kmh AS DOUBLE) AS wind_speed_10m_kmh,
                    CAST(wind_speed_100m_kmh AS DOUBLE) AS wind_speed_100m_kmh,
                    CAST(wind_gusts_10m_kmh AS DOUBLE) AS wind_gusts_10m_kmh,
                    CAST(solar_radiation_wm2 AS DOUBLE) AS solar_radiation_wm2,
                    {ts_expr('extracted_at')} AS extracted_at
                FROM read_json_auto('{bronze_dir / 'northwich_weather' / '*.jsonl'}')
            ),
            wx AS (
                SELECT *
                FROM wx_raw
                QUALIFY ROW_NUMBER() OVER (PARTITION BY hour_start ORDER BY extracted_at DESC) = 1
            ),
            nw_hourly AS (
                SELECT
                    date_trunc('hour', interval_start) AS hour_start,
                    ROUND(AVG(forecast_gco2), 1) AS nw_intensity_gco2,
                    ROUND(AVG(wind_pct), 2) AS nw_wind_pct,
                    ROUND(AVG(solar_pct), 2) AS nw_solar_pct,
                    ROUND(AVG(renewable_pct), 2) AS nw_renewable_pct,
                    ROUND(AVG(low_carbon_pct), 2) AS nw_low_carbon_pct,
                    ROUND(AVG(gas_pct), 2) AS nw_gas_pct
                FROM read_parquet('{staging / 'gold_regional_mix.parquet'}')
                WHERE is_north_west = TRUE
                GROUP BY 1
            )
            SELECT
                n.hour_start,
                n.nw_intensity_gco2,
                n.nw_wind_pct,
                n.nw_solar_pct,
                n.nw_renewable_pct,
                n.nw_low_carbon_pct,
                n.nw_gas_pct,
                w.temperature_c,
                w.wind_speed_10m_kmh,
                w.wind_speed_100m_kmh,
                w.wind_gusts_10m_kmh,
                w.solar_radiation_wm2
            FROM nw_hourly n
            INNER JOIN wx w ON n.hour_start = w.hour_start
            ORDER BY n.hour_start
        ) TO '{staging / 'gold_northwest_weather_energy.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
    )

    # 4b. Gold DAG Dispatch Game Telemetry (deduplicated by run_id)
    dag_bronze_files = list((bronze_dir / "dag_dispatch").glob("*.jsonl")) if (bronze_dir / "dag_dispatch").exists() else []
    if dag_bronze_files:
        con.execute(
            f"""
            COPY (
                WITH raw AS (
                    SELECT
                        CAST(run_id AS VARCHAR) AS run_id,
                        {ts_expr('submitted_at_utc')} AS submitted_at_utc,
                        CAST(handle AS VARCHAR) AS handle,
                        CAST(is_named_submission AS BOOLEAN) AS is_named_submission,
                        CAST(score AS INTEGER) AS score,
                        CAST(sprint AS INTEGER) AS sprint,
                        CAST(grade AS VARCHAR) AS grade,
                        CAST(efficiency_mult AS DOUBLE) AS efficiency_mult,
                        CAST(outage_cause AS VARCHAR) AS outage_cause,
                        CAST(duration_sec AS DOUBLE) AS duration_sec,
                        CAST(gold_delivered AS INTEGER) AS gold_delivered,
                        CAST(raw_delivered AS INTEGER) AS raw_delivered,
                        CAST(dirty_blocked AS INTEGER) AS dirty_blocked,
                        CAST(corrupt_leaked AS INTEGER) AS corrupt_leaked,
                        CAST(dupes_leaked AS INTEGER) AS dupes_leaked,
                        CAST(dropped_packets AS INTEGER) AS dropped_packets,
                        CAST(cache_hits AS INTEGER) AS cache_hits,
                        CAST(avg_latency_sec AS DOUBLE) AS avg_latency_sec,
                        CAST(nodes_deployed AS INTEGER) AS nodes_deployed,
                        CAST(total_revenue AS DOUBLE) AS total_revenue,
                        CAST(total_spend AS DOUBLE) AS total_spend,
                        CAST(country AS VARCHAR) AS country,
                        {ts_expr('extracted_at')} AS extracted_at
                    FROM read_json_auto('{bronze_dir / 'dag_dispatch' / '*.jsonl'}')
                ),
                dedup AS (
                    SELECT *
                    FROM raw
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY run_id
                        ORDER BY is_named_submission DESC, extracted_at DESC
                    ) = 1
                )
                SELECT
                    run_id,
                    submitted_at_utc,
                    CAST(date_trunc('day', submitted_at_utc) AS DATE) AS run_date,
                    handle,
                    is_named_submission,
                    score,
                    sprint,
                    grade,
                    efficiency_mult,
                    outage_cause,
                    duration_sec,
                    gold_delivered,
                    raw_delivered,
                    dirty_blocked,
                    corrupt_leaked,
                    dupes_leaked,
                    dropped_packets,
                    cache_hits,
                    avg_latency_sec,
                    nodes_deployed,
                    total_revenue,
                    total_spend,
                    ROUND(total_revenue - total_spend, 1) AS net_margin_usd,
                    ROUND(
                        100.0 * (gold_delivered + raw_delivered) /
                        NULLIF(gold_delivered + raw_delivered + corrupt_leaked + dupes_leaked, 0),
                        1
                    ) AS clean_rate_pct,
                    country,
                    extracted_at
                FROM dedup
                ORDER BY submitted_at_utc DESC
            ) TO '{staging / 'gold_dag_dispatch_runs.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
    else:
        con.execute(
            f"""
            COPY (
                SELECT
                    CAST(NULL AS VARCHAR) AS run_id,
                    CAST(NULL AS TIMESTAMPTZ) AS submitted_at_utc,
                    CAST(NULL AS DATE) AS run_date,
                    CAST(NULL AS VARCHAR) AS handle,
                    CAST(NULL AS BOOLEAN) AS is_named_submission,
                    CAST(NULL AS INTEGER) AS score,
                    CAST(NULL AS INTEGER) AS sprint,
                    CAST(NULL AS VARCHAR) AS grade,
                    CAST(NULL AS DOUBLE) AS efficiency_mult,
                    CAST(NULL AS VARCHAR) AS outage_cause,
                    CAST(NULL AS DOUBLE) AS duration_sec,
                    CAST(NULL AS INTEGER) AS gold_delivered,
                    CAST(NULL AS INTEGER) AS raw_delivered,
                    CAST(NULL AS INTEGER) AS dirty_blocked,
                    CAST(NULL AS INTEGER) AS corrupt_leaked,
                    CAST(NULL AS INTEGER) AS dupes_leaked,
                    CAST(NULL AS INTEGER) AS dropped_packets,
                    CAST(NULL AS INTEGER) AS cache_hits,
                    CAST(NULL AS DOUBLE) AS avg_latency_sec,
                    CAST(NULL AS INTEGER) AS nodes_deployed,
                    CAST(NULL AS DOUBLE) AS total_revenue,
                    CAST(NULL AS DOUBLE) AS total_spend,
                    CAST(NULL AS DOUBLE) AS net_margin_usd,
                    CAST(NULL AS DOUBLE) AS clean_rate_pct,
                    CAST(NULL AS VARCHAR) AS country,
                    CAST(NULL AS TIMESTAMPTZ) AS extracted_at
                WHERE FALSE
            ) TO '{staging / 'gold_dag_dispatch_runs.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )

    # 5. Run Data Quality assertions before publishing
    dq_checks = run_dq_checks(con, staging)

    # Gather live telemetry snapshot for pipeline-meta.json & run log
    nw_latest = con.execute(
        f"""
        SELECT interval_start, forecast_gco2, intensity_index, renewable_pct, low_carbon_pct, wind_pct
        FROM read_parquet('{staging / 'gold_regional_mix.parquet'}')
        WHERE is_north_west = TRUE AND interval_start <= CURRENT_TIMESTAMP
        ORDER BY interval_start DESC
        LIMIT 1
        """
    ).fetchone()

    gb_latest = con.execute(
        f"""
        SELECT interval_start, COALESCE(actual_gco2, forecast_gco2), intensity_index
        FROM read_parquet('{staging / 'gold_national_intensity.parquet'}')
        WHERE interval_start <= CURRENT_TIMESTAMP
        ORDER BY interval_start DESC
        LIMIT 1
        """
    ).fetchone()

    wx_latest = con.execute(
        f"""
        SELECT wind_speed_100m_kmh, temperature_c
        FROM read_parquet('{staging / 'gold_northwest_weather_energy.parquet'}')
        WHERE hour_start <= CURRENT_TIMESTAMP
        ORDER BY hour_start DESC
        LIMIT 1
        """
    ).fetchone()

    total_rows = int(
        con.execute(
            f"""
            SELECT
                (SELECT COUNT(*) FROM read_parquet('{staging / 'gold_national_intensity.parquet'}')) +
                (SELECT COUNT(*) FROM read_parquet('{staging / 'gold_regional_mix.parquet'}')) +
                (SELECT COUNT(*) FROM read_parquet('{staging / 'gold_national_generation.parquet'}')) +
                (SELECT COUNT(*) FROM read_parquet('{staging / 'gold_northwest_weather_energy.parquet'}')) +
                (SELECT COUNT(*) FROM read_parquet('{staging / 'gold_dag_dispatch_runs.parquet'}'))
            """
        ).fetchone()[0]
    )

    dag_summary = con.execute(
        f"""
        SELECT
            COUNT(*) AS total_shifts,
            MAX(score) AS top_score,
            ROUND(AVG(efficiency_mult), 2) AS avg_efficiency,
            ROUND(AVG(clean_rate_pct), 1) AS avg_clean_pct
        FROM read_parquet('{staging / 'gold_dag_dispatch_runs.parquet'}')
        """
    ).fetchone()
    dag_top_cause = con.execute(
        f"""
        SELECT outage_cause
        FROM read_parquet('{staging / 'gold_dag_dispatch_runs.parquet'}')
        GROUP BY 1
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """
    ).fetchone()

    duration_s = round(time.perf_counter() - started, 2)

    # Append to gold_pipeline_runs.parquet history
    existing_runs = gold_dir / "gold_pipeline_runs.parquet"
    run_record_sql = f"""
        SELECT
            CAST('{run_ts.isoformat()}' AS TIMESTAMPTZ) AS run_at,
            'healthy' AS status,
            {duration_s}::DOUBLE AS duration_seconds,
            {total_rows}::BIGINT AS total_gold_rows,
            {len(dq_checks)}::INTEGER AS dq_checks_passed
    """
    if existing_runs.exists():
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_parquet('{existing_runs}')
                UNION ALL
                {run_record_sql}
                ORDER BY run_at DESC
                LIMIT 2000
            ) TO '{staging / 'gold_pipeline_runs.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """
        )
    else:
        con.execute(
            f"COPY ({run_record_sql}) TO '{staging / 'gold_pipeline_runs.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD);"
        )

    con.close()

    # 6. Atomic swap of all staged Parquet files into /data/gold/
    for pq in staging.glob("*.parquet"):
        os.replace(pq, gold_dir / pq.name)
    shutil.rmtree(staging, ignore_errors=True)

    # 7. Rebuild Superset read-only DuckDB views catalog
    rebuild_superset_views(data_dir, superset_data_dir)

    # 8. Emit pipeline-meta.json atomically for seanstock.dev telemetry strip
    meta = {
        "status": "healthy",
        "pipeline": "meltano-uk-grid-elt",
        "engine": "Meltano + DuckDB + Apache Superset on K8s",
        "last_sync_utc": run_ts.isoformat(),
        "duration_seconds": duration_s,
        "total_gold_rows": total_rows,
        "dq_checks_passed": len(dq_checks),
        "dq_checks_total": len(dq_checks),
        "northwest": {
            "region_id": 3,
            "region_name": "North West England",
            "interval_start": nw_latest[0].isoformat() if nw_latest and nw_latest[0] else None,
            "intensity_gco2": round(float(nw_latest[1]), 1) if nw_latest and nw_latest[1] is not None else None,
            "intensity_index": nw_latest[2] if nw_latest else None,
            "renewable_pct": round(float(nw_latest[3]), 1) if nw_latest and nw_latest[3] is not None else None,
            "low_carbon_pct": round(float(nw_latest[4]), 1) if nw_latest and nw_latest[4] is not None else None,
            "wind_pct": round(float(nw_latest[5]), 1) if nw_latest and nw_latest[5] is not None else None,
            "wind_speed_100m_kmh": round(float(wx_latest[0]), 1) if wx_latest and wx_latest[0] is not None else None,
            "temperature_c": round(float(wx_latest[1]), 1) if wx_latest and wx_latest[1] is not None else None,
        },
        "gb_national": {
            "interval_start": gb_latest[0].isoformat() if gb_latest and gb_latest[0] else None,
            "intensity_gco2": round(float(gb_latest[1]), 1) if gb_latest and gb_latest[1] is not None else None,
            "intensity_index": gb_latest[2] if gb_latest else None,
        },
        "dag_dispatch": {
            "total_shifts": int(dag_summary[0]) if dag_summary else 0,
            "top_score": int(dag_summary[1]) if dag_summary and dag_summary[1] is not None else 0,
            "avg_efficiency": float(dag_summary[2]) if dag_summary and dag_summary[2] is not None else 1.0,
            "avg_clean_pct": float(dag_summary[3]) if dag_summary and dag_summary[3] is not None else 100.0,
            "top_outage_cause": dag_top_cause[0] if dag_top_cause else None,
        },
    }

    tmp_meta = gold_dir / ".pipeline-meta.json.tmp"
    tmp_meta.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp_meta, gold_dir / "pipeline-meta.json")

    print(
        f"[transform] published {total_rows} gold rows in {duration_s}s | "
        f"NW England: {meta['northwest']['intensity_gco2']} gCO2/kWh ({meta['northwest']['renewable_pct']}% renewable)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
