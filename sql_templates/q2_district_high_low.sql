-- q2_district_high_low.sql
-- Params: {STATE_HIGH}, {STATE_LOW}, {CROP_NAME}

-- most recent year available per state for this crop
WITH high_year AS (
  SELECT MAX(Year) AS Year FROM district_year_crop WHERE State = '{STATE_HIGH}' AND LOWER(Crop) LIKE LOWER('%{CROP_NAME}%')
),
low_year AS (
  SELECT MAX(Year) AS Year FROM district_year_crop WHERE State = '{STATE_LOW}' AND LOWER(Crop) LIKE LOWER('%{CROP_NAME}%')
),
high_agg AS (
  SELECT District, SUM(Production_tonnes) AS prod
  FROM district_year_crop WHERE State = '{STATE_HIGH}' AND Year = (SELECT Year FROM high_year) AND LOWER(Crop) LIKE LOWER('%{CROP_NAME}%')
  GROUP BY District ORDER BY prod DESC
),
low_agg AS (
  SELECT District, SUM(Production_tonnes) AS prod
  FROM district_year_crop WHERE State = '{STATE_LOW}' AND Year = (SELECT Year FROM low_year) AND LOWER(Crop) LIKE LOWER('%{CROP_NAME}%')
  GROUP BY District ORDER BY prod ASC
)
SELECT 'high' as which, (SELECT (District || '|' || prod) FROM high_agg LIMIT 1) AS top_district_prod,
       (SELECT Year FROM high_year) as year_used
UNION ALL
SELECT 'low' as which, (SELECT (District || '|' || prod) FROM low_agg LIMIT 1) AS low_district_prod,
       (SELECT Year FROM low_year) as year_used;
