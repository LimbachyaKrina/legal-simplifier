-- q1_avg_rain_top_crops.sql (FIXED)
-- Params: {STATE_A}, {STATE_B}, {N_YEARS}, {TOP_M}, {CEREAL_WHERE}
-- {CEREAL_WHERE} should be like: AND Crop IN ('Wheat','Rice','Maize') or left empty.

-- =================================================================
-- STATEMENT 1: AVERAGE RAINFALL (Requires Year CTEs)
-- =================================================================

WITH years_a AS (
SELECT Year FROM state_year_rain WHERE State = '{STATE_A}'
),
years_b AS (
SELECT Year FROM state_year_rain WHERE State = '{STATE_B}'
),
common_years AS (
SELECT Year FROM years_a
INTERSECT SELECT Year FROM years_b
ORDER BY Year DESC
LIMIT {N_YEARS}
),
yr AS (SELECT Year FROM common_years)

-- Average rainfall per state
SELECT 'rainfall' AS metric, r.State, AVG(r.annual_rainfall_mm) AS avg_rain_mm, COUNT(DISTINCT r.Year) AS years_count
FROM state_year_rain r
JOIN yr ON r.Year = yr.Year
WHERE r.State IN ('{STATE_A}','{STATE_B}')
GROUP BY r.State
ORDER BY r.State;

-- =================================================================
-- STATEMENT 2: TOP M CROPS (Requires RE-DEFINED Year CTEs)
-- The year-finding CTEs must be repeated here because they were destroyed after the first statement.
-- The subsequent CTEs (crop_tot, ranked) are chained using a comma.
-- =================================================================

WITH years_a AS (
SELECT Year FROM state_year_rain WHERE State = '{STATE_A}'
),
years_b AS (
SELECT Year FROM state_year_rain WHERE State = '{STATE_B}'
),
common_years AS (
SELECT Year FROM years_a
INTERSECT SELECT Year FROM years_b
ORDER BY Year DESC
LIMIT {N_YEARS}
),
yr AS (SELECT Year FROM common_years),

-- Top M crops per state by total production in same years (top per state)
crop_tot AS (
SELECT c.State, c.Crop, SUM(c.Production_tonnes) AS total_prod_tonnes
FROM crop_state_year c
JOIN yr ON c.Year = yr.Year -- yr is now in scope
WHERE c.State IN ('{STATE_A}','{STATE_B}')
{CEREAL_WHERE}
GROUP BY c.State, c.Crop
),
ranked AS (
SELECT ct.*, ROW_NUMBER() OVER (PARTITION BY ct.State ORDER BY ct.total_prod_tonnes DESC) AS rn
FROM crop_tot ct
)
SELECT 'top_crops' AS metric, State, Crop, total_prod_tonnes
FROM ranked
WHERE rn <= {TOP_M}
ORDER BY State, total_prod_tonnes DESC;