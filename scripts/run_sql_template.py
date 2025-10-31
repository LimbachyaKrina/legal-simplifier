# scripts/run_sql_template.py
import duckdb
import argparse
import os
from textwrap import shorten

DB = "data/agri_climate.duckdb"

class SafeDict(dict):
    def __missing__(self, key):
        return ""

def safe_prepare_params(params):
    return SafeDict(params)

def split_statements(sql):
    parts = [p.strip() for p in sql.split(';')]
    parts = [p for p in parts if p]
    return parts

def statement_uses_cte(stmt, cte_name_hint="yr"):
    return (f" {cte_name_hint} " in stmt.lower()) or (f".{cte_name_hint} " in stmt.lower()) or (f" {cte_name_hint}," in stmt.lower())

def run_template(template_path, params):
    if not os.path.exists(template_path):
        raise SystemExit(f"Template not found: {template_path}")
    with open(template_path, 'r', encoding='utf8') as f:
        sql_raw = f.read()
    sql = sql_raw.format_map(safe_prepare_params(params))

    stmts = split_statements(sql)
    if not stmts:
        raise SystemExit("No SQL statements found in template after substitution.")

    con = duckdb.connect(DB)
    results = []
    try:
        # If first stmt is a CTE (WITH ...), we'll NOT execute it alone.
        first_is_cte = stmts[0].strip().lower().startswith("with ")
        if first_is_cte:
            cte_block = stmts[0]  # keep the whole first stmt (CTE + possibly a select)
            remaining = stmts[1:]
            # For each remaining statement:
            idx = 1
            for s in remaining:
                # If statement references the CTE hint (e.g., yr), prepend the CTE block (without executing it separately)
                if statement_uses_cte(s, cte_name_hint="yr"):
                    combined = cte_block + "\n" + s
                    print(f"=== Executing combined statement (CTE + stmt {idx+1}) preview ===")
                    print(shorten(combined.replace("\n"," "), width=1000, placeholder=" ... [truncated]"))
                    try:
                        res = con.execute(combined).fetchdf()
                        results.append((f"combined_stmt_{idx+1}", combined, res))
                    except Exception as e:
                        results.append((f"combined_stmt_{idx+1}_error", combined, str(e)))
                else:
                    print(f"=== Executing statement {idx+1} preview ===")
                    print(shorten(s.replace("\n"," "), width=1000, placeholder=" ... [truncated]"))
                    try:
                        res = con.execute(s).fetchdf()
                        results.append((f"stmt_{idx+1}", s, res))
                    except Exception as e:
                        results.append((f"stmt_{idx+1}_error", s, str(e)))
                idx += 1
        else:
            # No initial CTE: execute statements sequentially
            idx = 1
            for s in stmts:
                print(f"=== Executing statement {idx} preview ===")
                print(shorten(s.replace("\n"," "), width=1000, placeholder=" ... [truncated]"))
                try:
                    res = con.execute(s).fetchdf()
                    results.append((f"stmt_{idx}", s, res))
                except Exception as e:
                    results.append((f"stmt_{idx}_error", s, str(e)))
                idx += 1
    finally:
        con.close()
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("template", help="path to SQL template")
    parser.add_argument("--params", nargs="*", help="key=value pairs", default=[])
    args = parser.parse_args()
    params = {}
    for kv in args.params:
        if '=' not in kv:
            continue
        k,v = kv.split("=",1)
        params[k] = v
    results = run_template(args.template, params)
    for name, stmt, res in results:
        print("\n" + "="*60)
        print(f"Result for {name}:")
        if isinstance(res, str):
            print("ERROR:", res)
        elif res is None or (hasattr(res, "empty") and res.empty):
            print("No rows returned.")
        else:
            print(res.head(200).to_string(index=False))
