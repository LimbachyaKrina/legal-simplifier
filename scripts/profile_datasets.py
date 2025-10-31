# scripts/profile_datasets.py
"""
Simple profiler for the raw CSVs. Produces diagnostics/profile_report.json
Run:
    python scripts/profile_datasets.py
"""
import os, json
import pandas as pd
DATA_DIR = "data"
OUT_DIR = "diagnostics"
os.makedirs(OUT_DIR, exist_ok=True)

expected = [
    "monthly_rainfall_distwise_1901-2017_data.csv",
    "district_rainfall_by_api.csv",
    "season_crop_prod_1997_dist.csv",
    "production_crops_19-20_him.csv"
]

found = [f for f in os.listdir(DATA_DIR) if f.lower().endswith('.csv')]
print("CSV files in data/:", found)
report = {"files":{}}

for fname in expected:
    path = os.path.join(DATA_DIR, fname)
    if not os.path.exists(path):
        report["files"][fname] = {"found": False}
        continue
    try:
        df = pd.read_csv(path)
    except Exception as e:
        report["files"][fname] = {"found": True, "error": str(e)}
        continue
    overview = {
        "found": True,
        "shape": df.shape,
        "columns": df.columns.tolist(),
        "sample_head": df.head(3).to_dict(orient="records"),
    }
    # try detect year column
    for yc in ["YEAR","Year","Crop_Year","Year ","YEAR "]:
        if yc in df.columns:
            try:
                yrs = sorted(df[yc].dropna().unique().astype(int).tolist())
                overview["year_col"] = yc
                overview["years_min_max"] = [int(yrs[0]), int(yrs[-1])]
            except Exception:
                pass
    report["files"][fname] = overview
    print(f"Profiled {fname}: shape={df.shape}, cols={len(df.columns)}")
    
# also write a short summary
with open(os.path.join(OUT_DIR,"profile_report.json"), "w") as f:
    json.dump(report, f, indent=2)
print("Wrote diagnostics/profile_report.json")
