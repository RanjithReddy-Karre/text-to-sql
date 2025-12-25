import streamlit as st
from openai import OpenAI
from pathlib import Path
import pandas as pd
import snowflake.connector

from config import operrouter, snowflake_connection

# ---------------- CONFIG ----------------
MODEL = "kwaipilot/kat-coder-pro:free"
BASE_URL = "https://openrouter.ai/api/v1"
API_KEY = operrouter.API_KEY
# ----------------------------------------

def read_file(path):
    return Path(path).read_text(encoding="utf-8")

def build_prompt(question: str) -> str:
    rules = read_file("schema/rules.txt")
    schema = read_file("schema/schema_summary.txt")
    template = read_file("prompt_template.txt")

    return (
        template
        .replace("{{RULES}}", rules)
        .replace("{{SCHEMA_SUMMARY}}", schema)
        .replace("{{QUESTION}}", question)
    )

def snowflake_sql_to_df(sql: str) -> pd.DataFrame:
    conn = snowflake.connector.connect(
        user=snowflake_connection.user,
        password=snowflake_connection.password,
        account=snowflake_connection.account,
        warehouse=snowflake_connection.warehouse,
        database=snowflake_connection.database,
        schema=snowflake_connection.schema,
        role=snowflake_connection.role
    )

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetch_pandas_all()
    finally:
        cursor.close()
        conn.close()

@st.cache_resource
def get_llm_client():
    return OpenAI(base_url=BASE_URL, api_key=API_KEY)

# ---------------- UI ----------------
st.set_page_config(page_title="IPL Analytics AI", layout="wide")

st.title("🏏 IPL Analytics – Ask in English")
st.caption("Ask questions about IPL data. SQL and results are generated automatically.")

question = st.text_area(
    "Ask a question",
    placeholder="Example: What is the average first innings score by venue?",
    height=120
)

if st.button("Run Query") and question.strip():

    client = get_llm_client()
    prompt = build_prompt(question)

    with st.spinner("Generating SQL..."):
        completion = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            top_p=1.0,
            frequency_penalty=0.0,
            presence_penalty=0.0,
            extra_body={
                "provider": {
                    "order": ["streamlake/fp16"],
                    "allow_fallbacks": False
                }
            }
        )

    sql = completion.choices[0].message.content.strip()

    st.subheader("🧾 Generated SQL")
    st.code(sql, language="sql")

    with st.spinner("Running query on Snowflake..."):
        try:
            df = snowflake_sql_to_df(sql)

            st.subheader("📊 Query Result")
            st.dataframe(df, use_container_width=True)

            st.caption(f"Rows returned: {len(df)}")

        except Exception as e:
            st.error("❌ Error executing SQL")
            st.exception(e)
