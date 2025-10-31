-- q4_policy_args.sql
-- Params: {STATE}, {CROP_A}, {CROP_B}, {N_YEARS}

-- common years for both crops and rainfall
WITH years_a AS (
  SELECT Year FROM crop_state_year WHERE State = '{STATE}' AND LOWER(Crop) LIKE LOWER('%{CROP_A}%')
),
years_b AS (
  SELECT Year FROM crop_state_year WHERE State = '{STATE}' AND LOWER(Crop) LIKE LOWER('%{CROP_B}%')
),
years_r AS (
  SELECT Year FROM state_year_rain WHERE State = '{STATE}'
),
common_years AS (
  SELECT Year FROM years_a
  INTERSECT SELECT Year FROM years_b
  INTERSECT SELECT Year FROM years_r
  ORDER BY Year DESC LIMIT {N_YEARS}
)
-- Aggregate production & area for both crops over the common years
SELECT 'summary' AS metric, c.Crop, SUM(c.Production_tonnes) AS total_prod, SUM(c.Area_ha) AS total_area, AVG(r.annual_rainfall_mm) AS avg_rain_mm
FROM crop_state_year c
JOIN state_year_rain r ON r.Year = c.Year AND r.State = c.State
WHERE c.State = '{STATE}' AND c.Year IN (SELECT Year FROM common_years) AND (LOWER(Crop) LIKE LOWER('%{CROP_A}%') OR LOWER(Crop) LIKE LOWER('%{CROP_B}%'))
GROUP BY c.Crop;
