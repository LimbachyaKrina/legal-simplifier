# scripts/clean_season_crop.py
"""
Basic cleaning for season_crop_prod_1997_dist.csv
Produces: data/season_crop_clean.csv and diagnostics/season_crop_clean_sample.csv
"""
import os
import pandas as pd
DATA_DIR = "data"
OUT_DIR = "diagnostics"
os.makedirs(OUT_DIR, exist_ok=True)

infile = os.path.join(DATA_DIR, "season_crop_prod_1997_dist.csv")
outpath = os.path.join(DATA_DIR, "season_crop_clean.csv")
if not os.path.exists(infile):
    raise SystemExit(f"Missing input file: {infile}")

df = pd.read_csv(infile)
print("Loaded season crop:", df.shape)

# Normalize column names
df.columns = [c.strip() for c in df.columns]

# rename common columns
if 'State_Name' in df.columns:
    df = df.rename(columns={'State_Name':'State'})
if 'District_Name' in df.columns:
    df = df.rename(columns={'District_Name':'District'})
if 'Crop_Year' in df.columns:
    df = df.rename(columns={'Crop_Year':'Year'})

# strip + title-case state/district/crop
for c in ['State','District','Crop']:
    if c in df.columns:
        df[c] = df[c].astype(str).str.strip().str.title()

# ensure Year numeric
if 'Year' in df.columns:
    df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype('Int64')

# fix Production & Area to numeric
for c in ['Production','Area']:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors='coerce')

# aggregate duplicates (group)
group_cols = ['State','District','Year','Crop']
agg_cols = {}
if 'Area' in df.columns:
    agg_cols['Area'] = ('Area','mean')
if 'Production' in df.columns:
    agg_cols['Production'] = ('Production','sum')

if agg_cols:
    df_clean = df.groupby(group_cols, as_index=False).agg(**agg_cols)
else:
    df_clean = df.copy()

print("After aggregation:", df_clean.shape)
df_clean.to_csv(outpath, index=False)
df_clean.head(20).to_csv(os.path.join(OUT_DIR, "season_crop_clean_sample.csv"), index=False)
print("Wrote", outpath, "and sample to diagnostics/")
