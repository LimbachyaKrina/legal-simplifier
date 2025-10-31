# nl_parser.py
"""
Rule-based NL parser with LLM fallback.
It returns a dict with:
  { "template": "q1_avg_rain_top_crops.sql", "params": {...} }
"""
import re
from typing import Optional, Dict
from llm_adapter import llm_generate_short

# list of available templates (file names)
TEMPLATES = {
    "compare_rain_and_top_crops": "sql_templates/q1_avg_rain_top_crops.sql",
    "district_high_low": "sql_templates/q2_district_high_low.sql",
    "trend_corr": "sql_templates/q3_trend_corr.sql",
    "policy_args": "sql_templates/q4_policy_args.sql",
    "district_vs_state": "sql_templates/q5_district_vs_state_2018.sql",
}

# simple entity extraction heuristics
def extract_states(text):
    # naive list: you can load canonical states from crop_state_year table for better matching
    STATES = ["Punjab","Rajasthan","Uttar Pradesh","Bihar","Maharashtra","Karnataka","Kerala","Tamil Nadu","Andhra Pradesh","Odisha","Jharkhand","Himachal Pradesh","Assam","West Bengal","Gujarat","Madhya Pradesh","Telangana","Chhattisgarh","Uttarakhand","Haryana","Punjab","Sikkim","Tripura","Nagaland","Manipur","Meghalaya","Mizoram","Andaman and Nicobar Islands","Dadra and Nagar Haveli","Daman and Diu","Lakshadweep","Puducherry","Delhi","Jammu and Kashmir","Ladakh"]
    found = []
    text_low = text.lower()
    for s in STATES:
        if s.lower() in text_low:
            found.append(s)
    return list(dict.fromkeys(found))  # unique preserve order

def extract_years(text):
    # capture 4-digit numbers in reasonable range
    ys = re.findall(r"\b(19\d{2}|20\d{2})\b", text)
    ys = [int(y) for y in ys if 1900 <= int(y) <= 2100]
    return sorted(set(ys))

def extract_numbers(text):
    # e.g., last 10 years, top 3
    m = re.search(r"last\s+(\d+)\s+years", text, re.I)
    last_n = int(m.group(1)) if m else None
    m2 = re.search(r"top\s+(\d+)", text, re.I)
    top_m = int(m2.group(1)) if m2 else None
    return last_n, top_m

def rule_based_parse(question: str) -> Optional[Dict]:
    q = question.lower()
    # Q1-like: compare average rainfall STATE_X and STATE_Y for the last N years + top M cereals
    if ("compare" in q and "rain" in q) or ("average annual rainfall" in q):
        states = extract_states(question)
        last_n, top_m = extract_numbers(question)
        # get crop filter (e.g., cereals)
        cereal_filter = ""
        if "cereal" in q or "cereals" in q:
            cereal_filter = "AND Crop IN ('Wheat','Rice','Maize')"
        if len(states) >= 2:
            return {
                "template": TEMPLATES["compare_rain_and_top_crops"],
                "params": {
                    "STATE_A": states[0],
                    "STATE_B": states[1],
                    "N_YEARS": last_n or 10,
                    "TOP_M": top_m or 3,
                    "CEREAL_WHERE": cereal_filter
                }
            }
    # Q3-like trend/corr
    if ("trend" in q or "correlat" in q) and any(w in q for w in ["trend","correl","impact","correlation"]):
        states = extract_states(question)
        yrs = extract_years(question)
        last_n, _ = extract_numbers(question)
        return {
            "template": TEMPLATES["trend_corr"],
            "params": {
                "STATE": states[0] if states else "Punjab",
                "CROP_NAME": re.findall(r"rice|wheat|maize|bajra|jowar|ragi", question, re.I)[0] if re.search(r"rice|wheat|maize|bajra|jowar|ragi", question, re.I) else "",
                "N_YEARS": last_n or 8
            }
        }

    return None

def llm_fallback_parse(question: str) -> Dict:
    """
    Ask the LLM to return a JSON-like mapping:
    {
      "template_key": "<one of compare_rain_and_top_crops|district_high_low|trend_corr|policy_args|district_vs_state>",
      "params": {"STATE_A":"Punjab", ...}
    }
    Keep the prompt explicit and strict about returning only JSON.
    """
    prompt = f"""
You are a strict parser. Given a user question about agriculture and climate, return ONLY a JSON object (no explanation).
The JSON should contain:
- template_key: one of compare_rain_and_top_crops, district_high_low, trend_corr, policy_args, district_vs_state
- params: a dictionary of parameters to substitute into the SQL template.

Question: \"\"\"{question}\"\"\"

Return only JSON. If you are not sure, pick the closest template and set params sensibly.
"""
    out = llm_generate_short(prompt)
    # try to parse JSON-ish result
    import json
    try:
        # LLM might include text; extract the first {...}
        s = out.strip()
        idx = s.find('{')
        if idx>=0:
            s = s[idx:]
        obj = json.loads(s)
        # map template key -> file
        tk = obj.get("template_key")
        if tk not in TEMPLATES:
            # try to guess mapping
            return {"template": TEMPLATES["compare_rain_and_top_crops"], "params": obj.get("params",{})}
        return {"template": TEMPLATES[tk], "params": obj.get("params",{})}
    except Exception as e:
        # fallback generic
        return {"template": TEMPLATES["compare_rain_and_top_crops"], "params": {"STATE_A":"Punjab","STATE_B":"Rajasthan","N_YEARS":10,"TOP_M":3,"CEREAL_WHERE":"AND Crop IN ('Wheat','Rice','Maize')"}}

def parse(question: str):
    parsed = rule_based_parse(question)
    if parsed:
        return parsed
    # LLM fallback
    return llm_fallback_parse(question)
