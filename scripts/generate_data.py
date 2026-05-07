"""
generate_data.py
Simulates a cloud billing API by producing realistic synthetic cost records
with time-of-day seasonality, noise, and occasional cost spikes.

Saves raw JSON to data/raw/<timestamp>.json for downstream ingestion.
"""

import json
import math
import os
import random
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path(os.getenv("RAW_DATA_DIR", "data/raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

REGIONS = ["us-east-1", "eu-west-1", "ap-southeast-1"]
SERVICES = ["EC2", "RDS", "S3", "Lambda", "EKS"]

BASE_COST = {
    "EC2": 4.50,
    "RDS": 2.10,
    "S3": 0.30,
    "Lambda": 0.05,
    "EKS": 6.00,
}

REGION_MULTIPLIER = {
    "us-east-1": 1.00,
    "eu-west-1": 1.12,
    "ap-southeast-1": 1.08,
}

SPIKE_PROBABILITY = 0.02  # ~2% chance any record is a spike
SPIKE_MULTIPLIER_RANGE = (4.0, 9.0)


def generate_cost(ts: datetime, service: str, region: str) -> float:
    """
    Generate a realistic cost value for a service/region at a given timestamp.

    Uses:
    - Sinusoidal daily seasonality (peaks around midday)
    - Gaussian noise proportional to base cost
    - Rare multiplicative spikes to simulate incidents
    - Region-specific multiplier
    """
    hour = ts.hour
    base = BASE_COST[service]
    region_mult = REGION_MULTIPLIER[region]

    # Daily seasonality: peaks around 14:00 UTC
    seasonal = 1 + 0.35 * math.sin(2 * math.pi * (hour - 6) / 24)

    # Gaussian noise (
    noise = random.gauss(0, base * 0.05)

    # Occasional spikes
    spike = 0.0
    if random.random() < SPIKE_PROBABILITY:
        spike = base * random.uniform(*SPIKE_MULTIPLIER_RANGE)

    cost = (base * seasonal + noise + spike) * region_mult
    return round(max(0.0, cost), 4)


def generate_resource_count(service: str) -> int:
    """Simulate number of running resources (instances, functions, buckets)."""
    ranges = {
        "EC2": (2, 25),
        "RDS": (1, 8),
        "S3": (5, 50),
        "Lambda": (10, 200),
        "EKS": (1, 10),
    }
    lo, hi = ranges[service]
    return random.randint(lo, hi)


def fetch_and_save(ts: datetime | None = None) -> Path:
    """Generate one batch of records and persist to disk as JSON."""
    if ts is None:
        ts = datetime.now(timezone.utc)

    records = [
        {
            "timestamp": ts.isoformat(),
            "region": region,
            "service": service,
            "cost_usd": generate_cost(ts, service, region),
            "resource_count": generate_resource_count(service),
        }
        for region in REGIONS
        for service in SERVICES
    ]

    fname = RAW_DIR / f"{ts.strftime('%Y%m%d_%H%M%S')}.json"
    fname.write_text(json.dumps(records, indent=2))
    print(f"[generate] Saved {len(records)} records → {fname}")
    return fname


def backfill(days: int = 60) -> None:
    """
    Generate historical data going back `days` days at 30-min intervals.
    Used once on project setup to give the ML models enough training data.
    """
    from datetime import timedelta

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    total_slots = days * 24 * 2  # 48 slots per day
    print(f"[backfill] Generating {total_slots} slots ({days} days)…")

    for i in range(total_slots, 0, -1):
        slot_ts = now - timedelta(minutes=30 * i)
        fetch_and_save(ts=slot_ts)

    print("[backfill] Done.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 60
        backfill(days=days)
    else:
        fetch_and_save()
