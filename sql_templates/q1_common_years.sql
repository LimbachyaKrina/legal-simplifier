-- q1_common_years.sql
WITH years_a AS (
  SELECT Year FROM state_year_rain WHERE State = '{STATE_A}'
),
years_b AS (
  SELECT Year FROM state_year_rain WHERE State = '{STATE_B}'
)
SELECT Year FROM years_a INTERSECT SELECT Year FROM years_b ORDER BY Year DESC LIMIT {N_YEARS};
