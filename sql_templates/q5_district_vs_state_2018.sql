-- q5_district_vs_state_2018.sql
-- Params: {DISTRICT}, {STATE}, {YEAR}

-- If you have district_year_crop -> you have district-level rainfall from district2018 or mapped monthly
-- We'll try district-level rainfall view if exists; else fall back to state average
-- Assume you have a table/view district_year_rain (if not, we can compute later)

-- district annual rainfall (if district-year-level exists)
SELECT d.State, d.District, d.Year, d.annual_rainfall_mm as district_mm, s.avg_state_mm
FROM (
  SELECT State, District, Year, annual_rainfall_mm
  FROM district_year_rain  -- optional
  WHERE State = '{STATE}' AND District = '{DISTRICT}' AND Year = {YEAR}
) d
JOIN (
  SELECT State, Year, AVG(annual_rainfall_mm) AS avg_state_mm
  FROM state_year_rain
  WHERE State = '{STATE}' AND Year = {YEAR}
  GROUP BY State, Year
) s ON s.State = d.State AND s.Year = d.Year;
