"""
train.py
Trains one Prophet forecasting model per (service, region) combination
on historical fact_cost data pulled from Supabase.

Run manually (or weekly via GitHub Actions) after you have >= 48 hours
of data. Saves models to models/prophet_models.pkl.

Usage:
    python scripts/train.py
"""

import os
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from prophet import Prophet
from sqlalchemy import create_engine

load_dotenv()

DB_URL = os.environ["DB_URL"]
MODELS_DIR = Path("models")
MODELS_DIR.mkdir(exist_ok=True)
MODEL_PATH = MODELS_DIR / "prophet_models.pkl"

MIN_ROWS = 48  # Minimum data points required to train a model


def load_training_data(engine) -> pd.DataFrame:
    query = """
        SELECT
            (dd.full_date + (dt.hour || ' hours')::INTERVAL
                          + (dt.minute || ' minutes')::INTERVAL)
                AT TIME ZONE 'UTC' AS ds,
            fc.cost_usd             AS y,
            ds.service_name,
            dr.region_code
        FROM fact_cost fc
        JOIN dim_date    dd ON dd.date_key   = fc.date_key
        JOIN dim_time    dt ON dt.time_key   = fc.time_key
        JOIN dim_service ds ON ds.service_key = fc.service_key
        JOIN dim_region  dr ON dr.region_key  = fc.region_key
        ORDER BY ds
    """
    df = pd.read_sql(query, engine)
    df["ds"] = pd.to_datetime(df["ds"], utc=True)
    return df


def train_models(df: pd.DataFrame) -> dict:
    models = {}
    groups = df.groupby(["service_name", "region_code"])
    print(f"[train] Found {len(groups)} (service, region) groups.")

    for (svc, reg), grp in groups:
        grp = grp[["ds", "y"]].dropna().sort_values("ds").reset_index(drop=True)

        if len(grp) < MIN_ROWS:
            print(f"  ⚠ Skipping ({svc}, {reg}) — only {len(grp)} rows (need {MIN_ROWS})")
            continue

        print(f"  → Training ({svc}, {reg}) on {len(grp)} rows…", end=" ")

        m = Prophet(
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=False,    # not enough data for yearly
            changepoint_prior_scale=0.1, # moderate flexibility
            interval_width=0.90,         # 90% confidence bands
            uncertainty_samples=200,
        )
        # Timezone-naive required by Prophet
        grp["ds"] = grp["ds"].dt.tz_localize(None)
        m.fit(grp)
        models[(svc, reg)] = m
        print("✓")

    return models


def run() -> None:
    engine = create_engine(DB_URL, pool_pre_ping=True)
    print("[train] Loading training data from Supabase…")
    df = load_training_data(engine)

    if df.empty:
        print("[train] No data found. Run generate_data.py and transform.py first.")
        return

    print(f"[train] Total rows: {len(df)}, date range: {df['ds'].min()} → {df['ds'].max()}")
    models = train_models(df)

    if not models:
        print("[train] No models trained — not enough data.")
        return

    joblib.dump(models, MODEL_PATH)
    print(f"[train] Saved {len(models)} models → {MODEL_PATH}")


if __name__ == "__main__":
    run()
