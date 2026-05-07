"""
Unit tests for scripts/generate_data.py
"""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from generate_data import (
    generate_cost,
    generate_resource_count,
    fetch_and_save,
    BASE_COST,
    REGION_MULTIPLIER,
    REGIONS,
    SERVICES,
)


def test_generate_cost_returns_positive():
    ts = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
    cost = generate_cost(ts, "EC2", "us-east-1")
    assert cost >= 0.0


def test_generate_cost_respects_base():
    ts = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
    cost = generate_cost(ts, "Lambda", "us-east-1")
    assert cost < BASE_COST["EC2"] * 2


def test_generate_cost_region_multiplier():
    ts = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
    us_cost = generate_cost(ts, "EC2", "us-east-1")
    eu_cost = generate_cost(ts, "EC2", "eu-west-1")
    assert eu_cost != us_cost


def test_generate_resource_count_in_range():
    count = generate_resource_count("EC2")
    assert 2 <= count <= 25


def test_fetch_and_save_creates_json(tmp_path):
    import os
    os.environ["RAW_DATA_DIR"] = str(tmp_path)
    
    ts = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    fpath = fetch_and_save(ts)
    
    assert fpath.exists()
    data = json.loads(fpath.read_text())
    assert len(data) == len(REGIONS) * len(SERVICES)
    assert all("timestamp" in rec for rec in data)
    assert all("cost_usd" in rec for rec in data)


def test_fetch_and_save_includes_all_combinations(tmp_path):
    import os
    os.environ["RAW_DATA_DIR"] = str(tmp_path)
    
    ts = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    fpath = fetch_and_save(ts)
    data = json.loads(fpath.read_text())
    
    combos = {(r["region"], r["service"]) for r in data}
    expected = {(reg, svc) for reg in REGIONS for svc in SERVICES}
    assert combos == expected


def test_fetch_and_save_validates_schema(tmp_path):
    import os
    os.environ["RAW_DATA_DIR"] = str(tmp_path)
    
    ts = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    fpath = fetch_and_save(ts)
    data = json.loads(fpath.read_text())
    
    required = {"timestamp", "region", "service", "cost_usd", "resource_count"}
    for rec in data:
        assert set(rec.keys()) == required
        assert isinstance(rec["cost_usd"], (int, float))
        assert isinstance(rec["resource_count"], int)
