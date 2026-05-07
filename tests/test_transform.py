"""
Unit tests for scripts/transform.py
"""

from datetime import datetime
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from transform import time_of_day


def test_time_of_day_morning():
    assert time_of_day(6) == "morning"
    assert time_of_day(11) == "morning"


def test_time_of_day_afternoon():
    assert time_of_day(12) == "afternoon"
    assert time_of_day(17) == "afternoon"


def test_time_of_day_evening():
    assert time_of_day(18) == "evening"
    assert time_of_day(21) == "evening"


def test_time_of_day_night():
    assert time_of_day(22) == "night"
    assert time_of_day(23) == "night"
    assert time_of_day(0) == "night"
    assert time_of_day(5) == "night"


def test_time_of_day_boundaries():
    assert time_of_day(5) == "night"
    assert time_of_day(6) == "morning"
    assert time_of_day(11) == "morning"
    assert time_of_day(12) == "afternoon"
    assert time_of_day(17) == "afternoon"
    assert time_of_day(18) == "evening"
    assert time_of_day(21) == "evening"
    assert time_of_day(22) == "night"
