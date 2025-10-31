# streamlit_app.py
import streamlit as st
from nl_parser import parse
from query_executor import run_template_get_results
from llm_adapter import llm_generate_short
import pandas as pd
import matplotlib.pyplot as plt
import os, re, csv, json, hashlib
from datetime import datetime

st.set_page_config(page_title="Agri-Climate Q&A", layout="wide")
st.title("Agri-Climate Q&A — Punjab, Rajasthan, and all India datasets")

st.markdown("""Ask natural-language questions about rainfall and crop production.""")

question = st.text_input("Type your question here", value="Compare the average annual rainfall in Punjab and Rajasthan for the last 10 years and list the top 3 cereals in each state.")
offline = st.checkbox("Offline mode (no external LLM calls)", value=os.getenv("OFFLINE", "0") == "1")
os.environ["OFFLINE"] = "1" if offline else "0"

if st.button("Ask"):
    if not question.strip():
        st.warning("Please enter a question.")
    else:
        st.info("Parsing question...")
        parsed = parse(question)
        template = parsed.get("template")
        params = parsed.get("params",{})
        st.write("**Parsed template**:", template)
        st.write("**Parameters**:", params)

        st.info("Executing SQL...")
        try:
            sql, df = run_template_get_results(template, params)
        except Exception as e:
            st.error(f"SQL execution failed: {e}")
            st.stop()

        st.subheader("Executed SQL")
        with st.expander("Show SQL"):
            st.code(sql, language="sql")

        st.subheader("Results")
        if df is None or df.empty:
            st.write("No results returned.")
        else:
            st.dataframe(df)

            # quick plot if numeric time-series (Year present)
            if 'Year' in df.columns and ('production' in "".join(df.columns).lower() or 'prod' in "".join(df.columns).lower() or 'rain' in "".join(df.columns).lower()):
                st.subheader("Chart")
                try:
                    fig, ax = plt.subplots(figsize=(8,3))
                    # try to plot first numeric column vs Year
                    ycol = [c for c in df.columns if c.lower() not in ('state','crop','metric') and df[c].dtype.kind in 'fi']
                    if ycol:
                        ax.plot(df['Year'], df[ycol[0]], marker='o')
                        ax.set_xlabel('Year'); ax.set_ylabel(ycol[0])
                        st.pyplot(fig)
                except Exception as e:
                    st.write("Could not plot:", e)

        # Extract sources/views from SQL for citations
        def extract_sources_from_sql(q: str):
            identifiers = set()
            for m in re.finditer(r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_./]+)", q, re.IGNORECASE):
                identifiers.add(m.group(1).split(" ")[0].strip())
            return sorted(identifiers)

        sources = extract_sources_from_sql(sql)
        dataset_map = {
            "state_year_rain": "data/rain_state_year.parquet",
            "crop_state_year": "data/crop_state_year.parquet",
        }

        # Compose narrative using LLM (compose only; send small summary)
        try:
            st.info("Composing narrative (LLM)...")
            # prepare small factual summary to send
            summary = df.head(50).to_dict(orient='records') if df is not None and not df.empty else []
            prompt = f"""
You are an assistant that composes short factual summaries for a Q&A app.
Do NOT invent numbers. Use ONLY the facts provided in the 'facts' variable.
facts = {summary}
sql = {sql}
question = {question}
sources = {sources}
Write a short answer (3-6 sentences). After each numeric claim, include a parenthetical citation like (source: <view-name>). Only use these sources: {sources}.
Return only text.
"""
            answer_text = llm_generate_short(prompt)
            st.subheader("Answer (composed by LLM)")
            st.write(answer_text)
        except Exception as e:
            st.write("LLM composition skipped:", e)

        # show data provenance
        st.subheader("Provenance")
        st.write("- Rainfall source: data/rain_state_year.parquet (derived from IMD monthly data).")
        st.write("- Crop source: data/crop_state_year.parquet (derived from season crop production).")
        st.write("- Executed SQL (shown above).")

        # Citations section (derived from executed SQL)
        st.subheader("Citations")
        if sources:
            for s in sources:
                path = dataset_map.get(s, "(view or table)")
                st.write(f"- {s}: {path}")
        else:
            st.write("- No sources detected from SQL.")

        # Audit log write
        try:
            os.makedirs("logs", exist_ok=True)
            audit_path = os.path.join("logs", "audit.csv")
            sql_hash = hashlib.sha256(sql.encode("utf-8")).hexdigest()
            payload = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "question": question,
                "template": template,
                "params": json.dumps(params, ensure_ascii=False),
                "sql_hash": sql_hash,
                "sources": json.dumps(sources),
                "offline": "1" if offline else "0",
            }
            write_header = not os.path.exists(audit_path)
            with open(audit_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(payload.keys()))
                if write_header:
                    writer.writeheader()
                writer.writerow(payload)
        except Exception as e:
            st.write("Audit log skipped:", e)
