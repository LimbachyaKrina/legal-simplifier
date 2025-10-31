# scripts/create_rain_state_year.py
"""
Apply the subdivision->state mapping and create data/rain_state_year.parquet

Reads:
  - data/monthly_rainfall_distwise_1901-2017_data.csv (SUBDIVISION, YEAR, ANNUAL)
  - diagnostics/subdivision_final_mapping.csv (SUBDIVISION -> MAPPED_STATES)

Writes:
  - data/rain_state_year.parquet  (columns: State, Year, annual_rainfall_mm)
  - diagnostics/rain_state_year_summary.csv (sample & unmapped rows)
"""
import os
import pandas as pd

DATA_DIR = "data"
DIAG_DIR = "diagnostics"
os.makedirs(DIAG_DIR, exist_ok=True)

monthly_fp = os.path.join(DATA_DIR, "monthly_rainfall_distwise_1901-2017_data.csv")
map_fp = os.path.join(DIAG_DIR, "subdivision_final_mapping.csv")
out_parquet = os.path.join(DATA_DIR, "rain_state_year.parquet")
out_diag = os.path.join(DIAG_DIR, "rain_state_year_summary.csv")

if not os.path.exists(monthly_fp):
    raise SystemExit(f"Missing {monthly_fp}")
if not os.path.exists(map_fp):
    raise SystemExit(f"Missing mapping file: {map_fp}  (run scripts/map_subdivisions.py first)")

print("Loading monthly rainfall and mapping...")
m = pd.read_csv(monthly_fp, usecols=['SUBDIVISION','YEAR','ANNUAL'])
map_df = pd.read_csv(map_fp).fillna("")

# normalize names
m['SUBDIVISION'] = m['SUBDIVISION'].astype(str).str.strip()
map_df['SUBDIVISION'] = map_df['SUBDIVISION'].astype(str).str.strip()

# join mapping
m2 = m.merge(map_df[['SUBDIVISION','MAPPED_STATES']], on='SUBDIVISION', how='left')

# report unmapped subdivisions
unmapped = m2[m2['MAPPED_STATES'].isna() | (m2['MAPPED_STATES'].str.strip()=="")]['SUBDIVISION'].unique().tolist()
print("Unmapped subdivisions count (unique):", len(unmapped))
if len(unmapped) > 0:
    print("Sample unmapped:", unmapped[:20])

# explode rows where MAPPED_STATES has comma-separated multi-states
def explode_mapping(row):
    mapped = str(row['MAPPED_STATES']).strip()
    if not mapped:
        return []
    parts = [p.strip() for p in mapped.split(',') if p.strip()]
    out = []
    for st in parts:
        out.append({'State': st, 'Year': int(row['YEAR']), 'annual_rainfall_mm': float(row['ANNUAL'])})
    return out

rows = []
for _, r in m2.iterrows():
    exploded = explode_mapping(r)
    if exploded:
        rows.extend(exploded)
    else:
        # keep unmapped rows as State=None for diagnostics
        rows.append({'State': None, 'Year': int(r['YEAR']), 'annual_rainfall_mm': float(r['ANNUAL'])})

rain_df = pd.DataFrame(rows)
print("Exploded rows total:", len(rain_df))

# drop rows with missing State before aggregation (we'll keep a diagnostics file for them)
diag_unmapped = rain_df[rain_df['State'].isna()]
if not diag_unmapped.empty:
    diag_unmapped.to_csv(os.path.join(DIAG_DIR,"rain_unmapped_rows_sample.csv"), index=False)
    print("Wrote diagnostics/rain_unmapped_rows_sample.csv (first few unmapped rows)")

rain_df = rain_df.dropna(subset=['State'])

# group by State-Year and take mean of ANNUAL (if multiple subdivisions map to same state)
rain_state_year = rain_df.groupby(['State','Year'], as_index=False).agg(annual_rainfall_mm=('annual_rainfall_mm','mean'))
print("rain_state_year rows:", len(rain_state_year))
print("Year range:", rain_state_year['Year'].min(), "-", rain_state_year['Year'].max())

# write parquet
rain_state_year.to_parquet(out_parquet, index=False)
rain_state_year.sample(20).to_csv(out_diag, index=False)
print("Wrote", out_parquet, "and diagnostics sample to", out_diag)
print("If you edited diagnostics/subdivision_final_mapping.csv, re-run this script to update the rain_state_year file.")
