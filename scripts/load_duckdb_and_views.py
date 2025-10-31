# scripts/load_duckdb_and_views.py
"""
Load canonical parquet files into a DuckDB file and create helpful views.
Run:
    python scripts/load_duckdb_and_views.py
Outputs:
 - data/agri_climate.duckdb
 - duckdb contains views: state_year_rain, crop_state_year, district_year_crop (if season_crop_clean exists)
"""
import os
import duckdb

DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "agri_climate.duckdb")
PAR_RAIN = os.path.join(DATA_DIR, "rain_state_year.parquet")
PAR_CROP = os.path.join(DATA_DIR, "crop_state_year.parquet")
SEASON_CLEAN = os.path.join(DATA_DIR, "season_crop_clean.csv")  # optional: district-level

con = duckdb.connect(DB_PATH)
print("Connected to", DB_PATH)

# register parquet files as views/tables
if os.path.exists(PAR_RAIN):
    con.execute(f"CREATE OR REPLACE VIEW state_year_rain AS SELECT State, Year::INTEGER AS Year, annual_rainfall_mm FROM read_parquet('{PAR_RAIN}');")
    print("Created view: state_year_rain")
else:
    print("Missing:", PAR_RAIN)

if os.path.exists(PAR_CROP):
    con.execute(f"CREATE OR REPLACE VIEW crop_state_year AS SELECT State, Year::INTEGER AS Year, Crop, Area_ha, Production_tonnes FROM read_parquet('{PAR_CROP}');")
    print("Created view: crop_state_year")
else:
    print("Missing:", PAR_CROP)

# If you have the cleaned season_crop file, make a district-level aggregate view as well
if os.path.exists(SEASON_CLEAN):
    # create a table from csv and aggregate district-year-crop
    con.execute("CREATE OR REPLACE TABLE season_crop_clean AS SELECT * FROM read_csv_auto('{}');".format(SEASON_CLEAN.replace("\\","/")))
    con.execute("""
        CREATE OR REPLACE VIEW district_year_crop AS
        SELECT State, District, Year::INTEGER AS Year, Crop, sum(Area) as Area_ha, sum(Production) as Production_tonnes
        FROM season_crop_clean
        GROUP BY State, District, Year, Crop;
    """)
    print("Created view: district_year_crop (from season_crop_clean)")
else:
    print("season_crop_clean.csv not found; skipping district view creation.")

# simple sanity queries
print("Sample years in rainfall:")
print(con.execute("SELECT MIN(Year), MAX(Year), COUNT(*) FROM state_year_rain").fetchall())
if os.path.exists(PAR_CROP):
    print("Sample years in crops:")
    print(con.execute("SELECT MIN(Year), MAX(Year), COUNT(*) FROM crop_state_year").fetchall())

con.close()
print("DuckDB setup complete.")
