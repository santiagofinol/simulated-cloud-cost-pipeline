"""
transform.py
Reads all unprocessed raw JSON files from data/raw/, cleans them,
and upserts records into the PostgreSQL star schema on Supabase.

Tracks processed files via a local manifest to avoid double-loading.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

RAW_DIR = Path(os.getenv("RAW_DATA_DIR", "data/raw"))
MANIFEST = RAW_DIR / ".processed_manifest.txt"


def load_manifest() -> set:
    if MANIFEST.exists():
        return set(MANIFEST.read_text().splitlines())
    return set()


def save_manifest(processed: set) -> None:
    MANIFEST.write_text("\n".join(sorted(processed)))


def time_of_day(hour: int) -> str:
    if 6 <= hour < 12:
        return "morning"
    if 12 <= hour < 18:
        return "afternoon"
    if 18 <= hour < 22:
        return "evening"
    return "night"


def get_or_create_service(conn, name: str) -> int:
    row = conn.execute(
        text("SELECT service_key FROM dim_service WHERE service_name = :n"),
        {"n": name},
    ).fetchone()
    if row:
        return row[0]
    result = conn.execute(
        text(
            "INSERT INTO dim_service (service_name) VALUES (:n) "
            "ON CONFLICT (service_name) DO UPDATE SET service_name=EXCLUDED.service_name "
            "RETURNING service_key"
        ),
        {"n": name},
    )
    return result.fetchone()[0]


def get_or_create_region(conn, code: str) -> int:
    geo_map = {
        "us-east-1": "US",
        "us-west-2": "US",
        "eu-west-1": "EU",
        "eu-central-1": "EU",
        "ap-southeast-1": "APAC",
        "ap-northeast-1": "APAC",
    }
    row = conn.execute(
        text("SELECT region_key FROM dim_region WHERE region_code = :c"),
        {"c": code},
    ).fetchone()
    if row:
        return row[0]
    geo = geo_map.get(code, "UNKNOWN")
    result = conn.execute(
        text(
            "INSERT INTO dim_region (region_code, geography) VALUES (:c, :g) "
            "ON CONFLICT (region_code) DO UPDATE SET region_code=EXCLUDED.region_code "
            "RETURNING region_key"
        ),
        {"c": code, "g": geo},
    )
    return result.fetchone()[0]


def ensure_dim_date(conn, ts: datetime) -> int:
    date_key = int(ts.strftime("%Y%m%d"))
    conn.execute(
        text(
            """
            INSERT INTO dim_date
                (date_key, full_date, year, month, day, day_of_week, week_of_year, quarter)
            VALUES (:dk, :fd, :y, :m, :d, :dow, :woy, :q)
            ON CONFLICT (date_key) DO NOTHING
            """
        ),
        dict(
            dk=date_key,
            fd=ts.date(),
            y=ts.year,
            m=ts.month,
            d=ts.day,
            dow=ts.weekday(),
            woy=ts.isocalendar().week,
            q=(ts.month - 1) // 3 + 1,
        ),
    )
    return date_key


def ensure_dim_time(conn, ts: datetime) -> int:
    # Round to nearest 30-min slot
    minute = 30 if ts.minute >= 30 else 0
    time_key = ts.hour * 100 + minute
    conn.execute(
        text(
            """
            INSERT INTO dim_time (time_key, hour, minute, time_of_day)
            VALUES (:tk, :h, :mi, :tod)
            ON CONFLICT (time_key) DO NOTHING
            """
        ),
        dict(tk=time_key, h=ts.hour, mi=minute, tod=time_of_day(ts.hour)),
    )
    return time_key


def process_file(conn, filepath: Path) -> int:
    records = json.loads(filepath.read_text())
    loaded = 0
    for rec in records:
        ts = pd.Timestamp(rec["timestamp"]).tz_convert("UTC")
        date_key = ensure_dim_date(conn, ts)
        time_key = ensure_dim_time(conn, ts)
        svc_key = get_or_create_service(conn, rec["service"])
        reg_key = get_or_create_region(conn, rec["region"])

        conn.execute(
            text(
                """
                INSERT INTO fact_cost
                    (date_key, time_key, service_key, region_key, cost_usd, resource_count)
                VALUES (:dk, :tk, :sk, :rk, :cost, :rc)
                ON CONFLICT (date_key, time_key, service_key, region_key) DO NOTHING
                """
            ),
            dict(
                dk=date_key,
                tk=time_key,
                sk=svc_key,
                rk=reg_key,
                cost=float(rec["cost_usd"]),
                rc=int(rec["resource_count"]),
            ),
        )
        loaded += 1
    return loaded

def run() -> None:
    engine = create_engine(os.environ["DB_URL"], pool_pre_ping=True, pool_size=5, max_overflow=10)
    processed = load_manifest()
    files = sorted(RAW_DIR.glob("*.json"))
    new_files = [f for f in files if f.name not in processed]

    if not new_files:
        print("[transform] No new files to process.")
        return

    print(f"[transform] Processing {len(new_files)} file(s)…")
    total = 0
    BATCH_SIZE = 50

    for i in range(0, len(new_files), BATCH_SIZE):
        batch = new_files[i:i + BATCH_SIZE]
        with engine.begin() as conn:
            for fpath in batch:
                n = process_file(conn, fpath)
                total += n
                processed.add(fpath.name)
                print(f"  ✓ {fpath.name} → {n} rows")
        save_manifest(processed)
        print(f"[transform] Batch {i // BATCH_SIZE + 1} committed. Total so far: {total}")

    print(f"[transform] Done. {total} rows upserted into fact_cost.")


if __name__ == "__main__":
    run()
