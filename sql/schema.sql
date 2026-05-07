-- Cloud Cost Pipeline Star Schema
-- Run once against your Supabase project via the SQL Editor

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INT PRIMARY KEY,
    full_date    DATE NOT NULL,
    year         INT NOT NULL,
    month        INT NOT NULL,
    day          INT NOT NULL,
    day_of_week  INT NOT NULL,  -- 0=Monday
    week_of_year INT NOT NULL,
    quarter      INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_key    INT PRIMARY KEY,  -- HHMM integer e.g. 1430
    hour        INT NOT NULL,
    minute      INT NOT NULL,
    time_of_day VARCHAR(10) NOT NULL  -- morning/afternoon/evening/night
);

CREATE TABLE IF NOT EXISTS dim_service (
    service_key  SERIAL PRIMARY KEY,
    service_name VARCHAR(50) UNIQUE NOT NULL,
    service_type VARCHAR(30)  -- compute/storage/database/serverless/container
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_key   SERIAL PRIMARY KEY,
    region_code  VARCHAR(30) UNIQUE NOT NULL,
    geography    VARCHAR(20) NOT NULL  -- US/EU/APAC
);

CREATE TABLE IF NOT EXISTS fact_cost (
    cost_id        BIGSERIAL PRIMARY KEY,
    date_key       INT NOT NULL REFERENCES dim_date(date_key),
    time_key       INT NOT NULL REFERENCES dim_time(time_key),
    service_key    INT NOT NULL REFERENCES dim_service(service_key),
    region_key     INT NOT NULL REFERENCES dim_region(region_key),
    cost_usd       NUMERIC(12,4) NOT NULL,
    resource_count INT NOT NULL DEFAULT 1,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (date_key, time_key, service_key, region_key)
);

CREATE TABLE IF NOT EXISTS fact_forecast (
    forecast_id  BIGSERIAL PRIMARY KEY,
    forecast_ts  TIMESTAMPTZ NOT NULL,
    service_key  INT NOT NULL REFERENCES dim_service(service_key),
    region_key   INT NOT NULL REFERENCES dim_region(region_key),
    yhat         NUMERIC(12,4) NOT NULL,
    yhat_lower   NUMERIC(12,4) NOT NULL,
    yhat_upper   NUMERIC(12,4) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_anomaly (
    anomaly_id    BIGSERIAL PRIMARY KEY,
    cost_id       BIGINT NOT NULL REFERENCES fact_cost(cost_id),
    is_anomaly    BOOLEAN NOT NULL,
    anomaly_score FLOAT NOT NULL,
    severity      VARCHAR(10),  -- low/medium/high
    detected_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cost_id)
);

-- Indexes for Power BI query performance
CREATE INDEX IF NOT EXISTS idx_fact_cost_date    ON fact_cost(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_cost_svc     ON fact_cost(service_key);
CREATE INDEX IF NOT EXISTS idx_fact_cost_region  ON fact_cost(region_key);
CREATE INDEX IF NOT EXISTS idx_fact_cost_ts      ON fact_cost(ingested_at);
CREATE INDEX IF NOT EXISTS idx_forecast_ts       ON fact_forecast(forecast_ts);
CREATE INDEX IF NOT EXISTS idx_anomaly_detected  ON fact_anomaly(detected_at);

-- Convenience view for Power BI import (flat denormalised)
CREATE OR REPLACE VIEW vw_cost_full AS
SELECT
    fc.cost_id,
    fc.cost_usd,
    fc.resource_count,
    fc.ingested_at,
    dd.full_date,
    dd.year,
    dd.month,
    dd.day,
    dd.day_of_week,
    dd.quarter,
    dt.hour,
    dt.minute,
    dt.time_of_day,
    ds.service_name,
    ds.service_type,
    dr.region_code,
    dr.geography,
    fa.is_anomaly,
    fa.anomaly_score,
    fa.severity
FROM fact_cost fc
JOIN dim_date    dd ON dd.date_key   = fc.date_key
JOIN dim_time    dt ON dt.time_key   = fc.time_key
JOIN dim_service ds ON ds.service_key = fc.service_key
JOIN dim_region  dr ON dr.region_key  = fc.region_key
LEFT JOIN fact_anomaly fa ON fa.cost_id = fc.cost_id;

CREATE OR REPLACE VIEW vw_forecast_full AS
SELECT
    ff.forecast_id,
    ff.forecast_ts,
    ff.yhat,
    ff.yhat_lower,
    ff.yhat_upper,
    ff.created_at,
    ds.service_name,
    dr.region_code
FROM fact_forecast ff
JOIN dim_service ds ON ds.service_key = ff.service_key
JOIN dim_region  dr ON dr.region_key  = ff.region_key;
