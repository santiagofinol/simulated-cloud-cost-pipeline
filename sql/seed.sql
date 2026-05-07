-- Seed static dimension tables
-- Run once after schema.sql

INSERT INTO dim_service (service_name, service_type) VALUES
    ('EC2',    'compute'),
    ('RDS',    'database'),
    ('S3',     'storage'),
    ('Lambda', 'serverless'),
    ('EKS',    'container')
ON CONFLICT (service_name) DO NOTHING;

INSERT INTO dim_region (region_code, geography) VALUES
    ('us-east-1',      'US'),
    ('eu-west-1',      'EU'),
    ('ap-southeast-1', 'APAC')
ON CONFLICT (region_code) DO NOTHING;

-- Populate dim_date for 2 years (2024-01-01 to 2025-12-31)
INSERT INTO dim_date (date_key, full_date, year, month, day, day_of_week, week_of_year, quarter)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d::DATE                      AS full_date,
    EXTRACT(YEAR  FROM d)::INT   AS year,
    EXTRACT(MONTH FROM d)::INT   AS month,
    EXTRACT(DAY   FROM d)::INT   AS day,
    EXTRACT(DOW   FROM d)::INT   AS day_of_week,
    EXTRACT(WEEK  FROM d)::INT   AS week_of_year,
    EXTRACT(QUARTER FROM d)::INT AS quarter
FROM GENERATE_SERIES('2024-01-01'::DATE, '2026-12-31'::DATE, '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

-- Populate dim_time for every 30-minute slot
INSERT INTO dim_time (time_key, hour, minute, time_of_day)
SELECT
    h * 100 + m AS time_key,
    h           AS hour,
    m           AS minute,
    CASE
        WHEN h >= 6  AND h < 12 THEN 'morning'
        WHEN h >= 12 AND h < 18 THEN 'afternoon'
        WHEN h >= 18 AND h < 22 THEN 'evening'
        ELSE 'night'
    END AS time_of_day
FROM
    GENERATE_SERIES(0, 23) AS h,
    GENERATE_SERIES(0, 30, 30) AS m
ON CONFLICT (time_key) DO NOTHING;
