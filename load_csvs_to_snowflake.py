import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pathlib import Path
from config import snowflake_connection

# =========================
# File mapping
# =========================
DATA_PATH = Path("D:/git/text-to-sql/data/output/processed")

TABLE_FILE_MAP = {
    "BATTING_SCORECARD": "all_competitions_all_matches_batting_scorecard.csv",
    "BOWLING_SCORECARD": "all_competitions_all_matches_bowling_scorecard.csv",
    "EXTRAS": "all_competitions_all_matches_extras_scorecard.csv",
    "FALL_OF_WICKETS": "all_competitions_all_matches_fow_scorecard.csv",
    "OVER_HISTORY": "all_competitions_all_matches_over_history_scorecard.csv",
    "MATCH_SUMMARY": "match_summary_archive_list.csv",
    "TEAMS": "teams.csv",
    "PLAYERS": "players.csv",
    "SQUADS": "all_competitions_all_matches_match_squad.csv"
}

# =========================
# Connect to Snowflake
# =========================
conn = snowflake.connector.connect(
    user=snowflake_connection.user,
    password=snowflake_connection.password,
    account=snowflake_connection.account,          # e.g. xy12345.us-east-1
    warehouse=snowflake_connection.warehouse,
    database=snowflake_connection.database,
    schema=snowflake_connection.schema,
    role=snowflake_connection.role
)
cursor = conn.cursor()

# =========================
# Load data
# =========================
for table_name, file_name in TABLE_FILE_MAP.items():
    file_path = DATA_PATH / file_name

    if not file_path.exists():
        print(f"⚠️ File not found: {file_name}, skipping")
        continue

    print(f"🚀 Loading {file_name} into {table_name}")

    df = pd.read_csv(file_path)

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=True,   # set True if table doesn't exist
        overwrite=True,            # set True for full reload
        quote_identifiers=False
    )

    print(
        f"✅ {table_name}: success={success}, "
        f"chunks={nchunks}, rows_loaded={nrows}"
    )

# =========================
# Cleanup
# =========================
cursor.close()
conn.close()
