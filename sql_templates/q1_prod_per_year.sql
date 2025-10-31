-- q1_prod_per_year.sql
SELECT Year, SUM(Production_tonnes) AS prod_tonnes, SUM(Area_ha) AS area_ha
FROM crop_state_year
WHERE State = '{STATE}' AND LOWER(Crop) LIKE LOWER('%{CROP_NAME}%')
GROUP BY Year
ORDER BY Year;
