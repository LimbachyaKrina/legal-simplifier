-- q1_yield_per_year.sql
SELECT Year,
  SUM(Production_tonnes) as prod_tonnes,
  SUM(Area_ha) as area_ha,
  CASE WHEN SUM(Area_ha) > 0 THEN SUM(Production_tonnes)/SUM(Area_ha) ELSE NULL END as yield_t_per_ha
FROM crop_state_year
WHERE State = '{STATE}' AND LOWER(Crop) LIKE LOWER('%{CROP_NAME}%')
GROUP BY Year
ORDER BY Year;
