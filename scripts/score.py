"""
score.py
Batch scoring script — runs every 30 minutes after ingestion.

Steps:
  1. Load saved Prophet models, generate 4-hour forecasts, write to fact_forecast.
  2. Pull recent 7-day window from fact_cost, run Isolation Forest,
     write anomaly flags + severity to fact_anomaly.

Usage:
    python scripts/score.py
"""

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from sqlalchemy import create_engine, text

load_dotenv()

MODEL_PATH = Path("models/prophet_models.pkl")
FORECAST_HORIZON_HOURS = 4
ANOMALY_WINDOW_DAYS = 7
CONTAMINATION = 0.02


def get_dim_key(conn, table: str, col: str, val: str) -> int | None:
    pk_col = col.replace("_name", "_key").replace("_code", "_key")
    row = conn.execute(
        text(f"SELECT {pk_col} FROM {table} WHERE {col} = :v"),
        {"v": val},
    ).fetchone()
    return row[0] if row else None


def write_forecasts(conn, models: dict) -> None:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    future_ts = [now + timedelta(hours=h) for h in range(1, FORECAST_HORIZON_HOURS + 1)]

    # Clear stale forecasts 
    conn.execute(
        text("DELETE FROM fact_forecast WHERE forecast_ts < NOW() - INTERVAL '6 hours'")
    )

    inserted = 0
    for (svc, reg), model in models.items():
        svc_key = get_dim_key(conn, "dim_service", "service_name", svc)
        reg_key = get_dim_key(conn, "dim_region", "region_code", reg)
        if svc_key is None or reg_key is None:
            continue

        future_df = pd.DataFrame({"ds": future_ts})
        future_df["ds"] = pd.to_datetime(future_df["ds"]).dt.tz_localize(None)
        fc = model.predict(future_df)

        for _, row in fc.iterrows():
            ts_utc = pd.Timestamp(row["ds"]).tz_localize("UTC")
            conn.execute(
                text(
                    """
                    INSERT INTO fact_forecast
                        (forecast_ts, service_key, region_key, yhat, yhat_lower, yhat_upper)
                    VALUES (:ts, :sk, :rk, :y, :yl, :yu)
                    """
                ),
                dict(
                    ts=ts_utc,
                    sk=svc_key,
                    rk=reg_key,
                    y=max(0.0, float(row["yhat"])),
                    yl=max(0.0, float(row["yhat_lower"])),
                    yu=max(0.0, float(row["yhat_upper"])),
                ),
            )
            inserted += 1

    print(f"[score] Forecasts written: {inserted}")


def severity_label(score: float, is_anomaly: bool) -> str | None:
    if not is_anomaly:
        return None
    # Isolation Forest decision_function returns negative scores for anomalies
    
    if score < -0.15:
        return "high"
    if score < -0.08:
        return "medium"
    return "low"


def write_anomalies(conn, engine) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=ANOMALY_WINDOW_DAYS)
    df = pd.read_sql(
        text(
            """
            SELECT
                fc.cost_id,
                fc.cost_usd,
                fc.resource_count,
                dt.hour,
                dd.day_of_week,
                ds.service_name,
                dr.region_code
            FROM fact_cost fc
            JOIN dim_date    dd ON dd.date_key    = fc.date_key
            JOIN dim_time    dt ON dt.time_key    = fc.time_key
            JOIN dim_service ds ON ds.service_key = fc.service_key
            JOIN dim_region  dr ON dr.region_key  = fc.region_key
            WHERE fc.ingested_at >= :cutoff
            ORDER BY fc.cost_id
            """
        ),
        engine,
        params={"cutoff": cutoff},
    )

    if len(df) < 20:
        print(f"[score] Only {len(df)} rows — skipping anomaly detection (need >= 20).")
        return

    features = df[["cost_usd", "resource_count", "hour", "day_of_week"]].values
    iso = IsolationForest(
        contamination=CONTAMINATION,
        n_estimators=200,
        random_state=42,
        n_jobs=-1,
    )
    iso.fit(features)
    predictions = iso.predict(features)       # 1=normal, -1=anomaly
    scores = iso.decision_function(features)  # lower = more anomalu

    df["is_anomaly"] = predictions == -1
    df["anomaly_score"] = scores
    df["severity"] = [
        severity_label(s, a) for s, a in zip(df["anomaly_score"], df["is_anomaly"])
    ]

    anomaly_count = df["is_anomaly"].sum()
    print(f"[score] Anomalies detected: {anomaly_count} / {len(df)}")

    # Upsert anomaly results
    conn.execute(
        text("DELETE FROM fact_anomaly WHERE detected_at < NOW() - INTERVAL '8 hours'")
    )

    for _, row in df.iterrows():
        conn.execute(
            text(
                """
                INSERT INTO fact_anomaly (cost_id, is_anomaly, anomaly_score, severity)
                VALUES (:cid, :ia, :sc, :sev)
                ON CONFLICT (cost_id) DO UPDATE
                    SET is_anomaly    = EXCLUDED.is_anomaly,
                        anomaly_score = EXCLUDED.anomaly_score,
                        severity      = EXCLUDED.severity,
                        detected_at   = NOW()
                """
            ),
            dict(
                cid=int(row["cost_id"]),
                ia=bool(row["is_anomaly"]),
                sc=float(row["anomaly_score"]),
                sev=row["severity"],
            ),
        )


def run() -> None:
    if not MODEL_PATH.exists():
        print(f"[score] Model file not found at {MODEL_PATH}. Run train.py first.")
        return

    print("[score] Loading models…")
    models = joblib.load(MODEL_PATH)
    print(f"[score] Loaded {len(models)} Prophet models.")

    engine = create_engine(os.environ["DB_URL"], pool_pre_ping=True)

    with engine.begin() as conn:
        print("[score] Writing forecasts…")
        write_forecasts(conn, models)

        print("[score] Running anomaly detection…")
        write_anomalies(conn, engine)

    print("[score] Scoring complete.")


if __name__ == "__main__":
    run()
