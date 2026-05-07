"""
Unit tests for scripts/score.py
"""

from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from score import severity_label


def test_severity_label_not_anomaly():
    assert severity_label(-0.5, False) is None
    assert severity_label(0.1, False) is None


def test_severity_label_high():
    assert severity_label(-0.20, True) == "high"
    assert severity_label(-0.16, True) == "high"


def test_severity_label_medium():
    assert severity_label(-0.14, True) == "medium"
    assert severity_label(-0.10, True) == "medium"
    assert severity_label(-0.08, True) == "medium"


def test_severity_label_low():
    assert severity_label(-0.07, True) == "low"
    assert severity_label(-0.05, True) == "low"
    assert severity_label(-0.01, True) == "low"


def test_severity_label_boundary():
    assert severity_label(-0.15, True) == "high"
    assert severity_label(-0.149, True) == "medium"
    assert severity_label(-0.08, True) == "medium"
    assert severity_label(-0.079, True) == "low"
