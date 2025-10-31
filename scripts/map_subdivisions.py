# scripts/map_subdivisions.py
"""
Generate a SUBDIVISION -> STATE mapping for IMD monthly rainfall file.

Reads:
  data/monthly_rainfall_distwise_1901-2017_data.csv

Writes:
  diagnostics/subdivision_final_mapping.csv

Usage:
  python scripts/map_subdivisions.py
After running, open diagnostics/subdivision_final_mapping.csv and review rows with METHOD=='unmapped'
or low SCORE (<85). You can edit MAPPED_STATES (comma-separated) to override before running the merge.
"""
import os
import pandas as pd

try:
    from rapidfuzz import fuzz, process
except Exception:
    raise SystemExit("Please install rapidfuzz: pip install rapidfuzz")

DATA_DIR = "data"
DIAG_DIR = "diagnostics"
os.makedirs(DIAG_DIR, exist_ok=True)

monthly_fp = os.path.join(DATA_DIR, "monthly_rainfall_distwise_1901-2017_data.csv")
if not os.path.exists(monthly_fp):
    raise SystemExit(f"Missing file: {monthly_fp}")

print("Loading monthly rainfall data...")
m = pd.read_csv(monthly_fp, usecols=['SUBDIVISION','YEAR','ANNUAL'])
subs = sorted(m['SUBDIVISION'].dropna().astype(str).unique())
print("Unique subdivisions found:", len(subs))

# canonical state/UT list (includes common names)
canonical_states = sorted([
    "Andaman and Nicobar Islands","Andhra Pradesh","Arunachal Pradesh","Assam","Bihar",
    "Chhattisgarh","Goa","Gujarat","Haryana","Himachal Pradesh","Jammu and Kashmir",
    "Jharkhand","Karnataka","Kerala","Madhya Pradesh","Maharashtra","Manipur",
    "Meghalaya","Mizoram","Nagaland","Odisha","Punjab","Rajasthan","Sikkim",
    "Tamil Nadu","Telangana","Tripura","Uttar Pradesh","Uttarakhand",
    "West Bengal","Delhi","Puducherry","Chandigarh","Andaman and Nicobar Islands",
    "Ladakh","Lakshadweep","Daman and Diu","Dadra and Nagar Haveli"
])

# Pre-fill a manual mapping dictionary for common multi-name subdivisions
manual_map = {
    "Andaman & Nicobar Islands": ["Andaman and Nicobar Islands"],
    "Assam & Meghalaya": ["Assam","Meghalaya"],
    "Naga Mani Mizo Tripura": ["Nagaland","Manipur","Mizoram","Tripura"],
    "Sub Himalayan West Bengal & Sikkim": ["West Bengal","Sikkim"],
    "Gangetic West Bengal": ["West Bengal"],
    "Konkan & Goa": ["Goa","Maharashtra"],
    "Saurashtra & Kutch": ["Gujarat"],
    "East Uttar Pradesh": ["Uttar Pradesh"],
    "West Uttar Pradesh": ["Uttar Pradesh"],
    "East Rajasthan": ["Rajasthan"],
    "West Rajasthan": ["Rajasthan"],
    "North Interior Karnataka": ["Karnataka"],
    "South Interior Karnataka": ["Karnataka"],
    "Coastal Karnataka": ["Karnataka"],
    "Marathwada": ["Maharashtra"],
    "Madhya Maharashtra": ["Maharashtra"],
    "Rayalseema": ["Andhra Pradesh"],
    "Coastal Andhra Pradesh": ["Andhra Pradesh"],
    "Sub Himalayan West Bengal & Sikkim": ["West Bengal","Sikkim"]
}

records = []
for sub in subs:
    if sub in manual_map:
        records.append((sub, ", ".join(manual_map[sub]), "manual", 100))
        continue
    # try exact normalized match
    normalized = sub.strip().title()
    if normalized in canonical_states:
        records.append((sub, normalized, "exact", 100))
        continue
    # fuzzy match
    match = process.extractOne(sub, canonical_states, scorer=fuzz.token_sort_ratio)
    if match:
        cand_state, score, _ = match
        if score >= 85:
            records.append((sub, cand_state, "fuzzy", int(score)))
        else:
            # attempt to split names with delimiters and map parts
            parts = []
            for sep in ['&',' and ',',','/','-','/','\\']:
                if sep in sub:
                    parts = [p.strip().title() for p in sub.replace('/',',').replace('\\',',').replace('&',',').split(',') if p.strip()]
                    break
            if parts:
                mapped = []
                for p in parts:
                    m2 = process.extractOne(p, canonical_states, scorer=fuzz.token_sort_ratio)
                    if m2 and m2[1] >= 75:
                        mapped.append(m2[0])
                if mapped:
                    records.append((sub, ", ".join(sorted(set(mapped))), "split_fuzzy", int(score)))
                else:
                    records.append((sub, "", "unmapped", int(score)))
            else:
                records.append((sub, "", "unmapped", int(score)))
    else:
        records.append((sub, "", "unmapped", 0))

df_out = pd.DataFrame(records, columns=["SUBDIVISION","MAPPED_STATES","METHOD","SCORE"])
out_fp = os.path.join(DIAG_DIR, "subdivision_final_mapping.csv")
df_out.to_csv(out_fp, index=False)
print("Wrote mapping suggestions to", out_fp)
print("Summary:")
print(" Total subdivisions:", len(df_out))
print(" Manual:", len(df_out[df_out['METHOD']=='manual']))
print(" Exact:", len(df_out[df_out['METHOD']=='exact']))
print(" Fuzzy (>=85):", len(df_out[df_out['METHOD']=='fuzzy']))
print(" Split fuzzy:", len(df_out[df_out['METHOD']=='split_fuzzy']))
print(" Unmapped (need manual review):", len(df_out[df_out['METHOD']=='unmapped']))
print("")
print("Open diagnostics/subdivision_final_mapping.csv and edit MAPPED_STATES for any 'unmapped' or low SCORE rows if you want different mapping.")
