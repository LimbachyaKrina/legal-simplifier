# query_executor.py
import duckdb, os
import re
from dotenv import load_dotenv
load_dotenv()
DB = os.getenv("DUCKDB_PATH","data/agri_climate.duckdb")
from pathlib import Path

def run_template_get_results(template_path: str, params: dict):
    # read template, substitute with str.format_map
    with open(template_path, 'r', encoding='utf8') as f:
        sql_raw = f.read()
    # validate and sanitize params before formatting
    safe_params = {}
    # allowlisted simple validators
    state_pattern = re.compile(r"^[A-Za-z .'-]{2,40}$")
    ident_pattern = re.compile(r"^[A-Za-z0-9_./]{1,64}$")
    int_pattern = re.compile(r"^\d{1,3}$")
    for k, v in (params or {}).items():
        if isinstance(v, int):
            safe_params[k] = v
            continue
        if isinstance(v, str):
            # heuristic by param name
            if k.upper().startswith("STATE"):
                if not state_pattern.match(v):
                    raise RuntimeError(f"Invalid state parameter: {k}")
                # quote strings safely
                safe_params[k] = v.replace("'", "''")
            elif k.upper() in ("CROP", "CROP_NAME"):
                if not state_pattern.match(v):
                    raise RuntimeError(f"Invalid crop parameter: {k}")
                safe_params[k] = v.replace("'", "''")
            elif k.upper() in ("CEREAL_WHERE",):
                # restrict to a small safe pattern: only letters, commas, spaces, quotes, parentheses and = _
                if not re.fullmatch(r"[A-Za-z0-9_(),' =]+", v or ""):
                    raise RuntimeError("Invalid filter expression")
                safe_params[k] = v
            elif k.upper().endswith("YEARS") or k.upper().startswith("TOP_") or k.upper() in ("N_YEARS", "TOP_M"):
                if not int_pattern.match(str(v)):
                    raise RuntimeError(f"Invalid integer parameter: {k}")
                safe_params[k] = int(v)
            else:
                # default to identifier-safe
                if not ident_pattern.match(v):
                    raise RuntimeError(f"Invalid parameter: {k}")
                safe_params[k] = v
        else:
            raise RuntimeError(f"Unsupported parameter type for {k}")

    sql = sql_raw.format_map(safe_params)
    # Safety checks: disallow modification statements
    forbidden = ["insert ", "update ", "delete ", "drop ", "create ", "alter ", "replace "]
    if any(tok in sql.lower() for tok in forbidden):
        raise RuntimeError("Unsafe SQL detected")
    con = duckdb.connect(DB)
    try:
        # We support multi-statement templates; reuse your run_sql_template logic: split and execute sequentially
        res = con.execute(sql).fetchdf()
    finally:
        con.close()
    return sql, res
