# scripts/create_crop_state_year.py
"""
Aggregate season_crop_clean.csv into crop_state_year.parquet
Writes:
  data/crop_state_year.parquet  (State, Year, Crop, Area_ha, Production_tonnes)
  diagnostics/crop_state_year_sample.csv
"""
import os
import pandas as pd
DATA_DIR = "data"
DIAG_DIR = "diagnostics"
os.makedirs(DIAG_DIR, exist_ok=True)

infile = os.path.join(DATA_DIR, "season_crop_clean.csv")
if not os.path.exists(infile):
    raise SystemExit(f"Missing cleaned crop file: {infile}. Run scripts/clean_season_crop.py first.")

df = pd.read_csv(infile)
print("Loaded cleaned crop:", df.shape)

# normalize column names
for c in ['State','District','Year','Crop','Area','Production']:
    if c not in df.columns:
        raise SystemExit(f"Expected column '{c}' not found in cleaned crop file. Columns: {df.columns.tolist()}")

df['State'] = df['State'].astype(str).str.strip().str.title()
df['Crop'] = df['Crop'].astype(str).str.strip().str.title()
df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype('Int64')
df['Area'] = pd.to_numeric(df['Area'], errors='coerce')
df['Production'] = pd.to_numeric(df['Production'], errors='coerce')

# aggregate to State-Year-Crop
crop_state_year = df.groupby(['State','Year','Crop'], as_index=False).agg(
    Area_ha=('Area','sum'),
    Production_tonnes=('Production','sum')
)

# quick sanity stats
print("Aggregated rows:", crop_state_year.shape)
print("Year range:", crop_state_year['Year'].min(), "-", crop_state_year['Year'].max())

# save parquet and sample csv
out_parquet = os.path.join(DATA_DIR, "crop_state_year.parquet")
crop_state_year.to_parquet(out_parquet, index=False)
crop_state_year.sample(50).to_csv(os.path.join(DIAG_DIR, "crop_state_year_sample.csv"), index=False)
print("Wrote", out_parquet, "and diagnostics sample.")
