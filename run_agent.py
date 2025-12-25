from openai import OpenAI
from pathlib import Path
from config import operrouter, snowflake_connection
import snowflake.connector



import pandas as pd

# ---------------- CONFIG ----------------
MODEL = "kwaipilot/kat-coder-pro:free"
BASE_URL = "https://openrouter.ai/api/v1"
API_KEY = operrouter.API_KEY   # <-- replace
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

import snowflake.connector
import pandas as pd

def snowflake_sql_to_df(sql: str):
    """
    Execute Snowflake SQL and return a Pandas DataFrame.
    """

    conn = snowflake.connector.connect(
        user=snowflake_connection.user,
        password=snowflake_connection.password,
        account=snowflake_connection.account,          # e.g. xy12345.us-east-1
        warehouse=snowflake_connection.warehouse,
        database=snowflake_connection.database,
        schema=snowflake_connection.schema,
        role=snowflake_connection.role
    )

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        df = cursor.fetch_pandas_all()
        return df
    finally:
        cursor.close()
        conn.close()




def main():
    client = OpenAI(
        base_url=BASE_URL,
        api_key=API_KEY
    )

    print("\n🏏 IPL Text-to-SQL Agent")
    print("Type a question and press Enter.")
    print("Type 'exit' to quit.\n")

    while True:
        question = input("> ").strip()

        if question.lower() in {"exit", "quit"}:
            print("Goodbye 👋")
            break

        prompt = build_prompt(question)

        completion = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "user", "content": prompt}
            ],
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

        sql = completion.choices[0].message.content

        print("\n📄 Generated SQL:\n")
        print(sql)
        df = snowflake_sql_to_df(sql)
        pd.set_option('display.max_columns', None)
        print(df.head())
        print("\n" + "-" * 70 + "\n")

if __name__ == "__main__":
    main()
