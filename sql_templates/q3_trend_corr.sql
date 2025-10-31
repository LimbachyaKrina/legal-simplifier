-- q3_trend_corr.sql
-- Params: {STATE}, {CROP_NAME}, {N_YEARS}

-- get last N years common to both series in this state
WITH prod_years AS (
  SELECT Year FROM crop_state_year WHERE State = '{STATE}' AND LOWER(Crop) LIKE LOWER('%{CROP_NAME}%')
),
rain_years AS (
  SELECT Year FROM state_year_rain WHERE State = '{STATE}'
),
common_years AS (
  SELECT Year FROM prod_years INTERSECT SELECT Year FROM rain_years ORDER BY Year DESC LIMIT {N_YEARS}
),
prod_ts AS (
  SELECT Year, SUM(Production_tonnes) AS production
  FROM crop_state_year
  WHERE State = '{STATE}' AND LOWER(Crop) LIKE LOWER('%{CROP_NAME}%') AND Year IN (SELECT Year FROM common_years)
  GROUP BY Year ORDER BY Year
),
rain_ts AS (
  SELECT Year, annual_rainfall_mm AS rainfall
  FROM state_year_rain
  WHERE State = '{STATE}' AND Year IN (SELECT Year FROM common_years)
  ORDER BY Year
)
-- Return aligned time-series for external correlation calculation
SELECT p.Year, p.production, r.rainfall
FROM prod_ts p JOIN rain_ts r ON p.Year = r.Year
ORDER BY p.Year;
