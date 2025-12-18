import urllib.request
import urllib.error
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict


# =========================
# Configuration
# =========================
INPUT_PATH = Path("D:/git/text-to-sql/data/input")
OUTPUT_PATH = Path("D:/git/text-to-sql/data/output")
MATCH_FEED_PATH = OUTPUT_PATH / "match_feed1"

ARCHIVE_BASE_URL = ("https://ipl-stats-sports-mechanic.s3.ap-south-1.amazonaws.com/ipl/feeds/archievefeeds/")
LIVE_BASE_URL = ("https://ipl-stats-sports-mechanic.s3.ap-south-1.amazonaws.com/ipl/feeds/")

INNINGS = ("Innings1", "Innings2")


# =========================
# Utilities
# =========================
def ensure_directories() -> None:
    MATCH_FEED_PATH.mkdir(parents=True, exist_ok=True)


def build_feed_url(base_url: str, match_id: int, innings: str) -> str:
    return f"{base_url}{match_id}-{innings}.js"


def clean_js_wrapped_json(raw_text: str) -> dict:
    cleaned = raw_text.replace("onScoring(", "").replace(");", "")
    return json.loads(cleaned)


# =========================
# Network
# =========================
def fetch_innings_data(url: str, innings: str) -> Optional[dict]:
    try:
        with urllib.request.urlopen(url) as response:
            raw_text = response.read().decode("utf-8")
            parsed_json = clean_js_wrapped_json(raw_text)
            return parsed_json.get(innings)

    except urllib.error.HTTPError as e:
        if e.code != 404 or innings != "Innings2":
            print(f"⚠️ HTTP {e.code} → {url}")

    except urllib.error.URLError as e:
        print(f"⚠️ Network error → {url} ({e.reason})")

    except Exception as e:
        print(f"⚠️ Unexpected error → {url} ({e})")

    return None


# =========================
# File I/O
# =========================
def save_match_feed( competition_id: int, match_id: int, match_name: str, data: Dict[str, Optional[dict]], ) -> None:
    filename = f"{competition_id}-{match_id}-{match_name}.json"
    file_path = MATCH_FEED_PATH / filename

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    print(f"✅ Saved: {file_path.name}")


# =========================
# Processing Logic
# =========================
def process_match(competition_id: int,match_id: int,match_name: str,base_url: str,) -> None:
    match_data = {}

    for innings in INNINGS:
        url = build_feed_url(base_url, match_id, innings)
        match_data[innings] = fetch_innings_data(url, innings)

    save_match_feed(competition_id, match_id, match_name, match_data)


def process_competition(competition_id: int,competition_year: int,base_url: str,) -> None:
    summary_file = OUTPUT_PATH / f"{competition_year}_IPL_MATCH_SUMMERY.csv"
    match_summary_df = pd.read_csv(summary_file)

    for _, row in match_summary_df.iterrows():
        match_id = int(row["match_id"])
        match_name = str(row["match_name"]).replace(" ", "-")

        process_match(
            competition_id=competition_id,
            match_id=match_id,
            match_name=match_name,
            base_url=base_url,
        )


# =========================
# Main
# =========================
def main() -> None:
    start_time = datetime.now()
    print(f"🚀 Process started at: {start_time}")

    ensure_directories()

    competition_df = pd.read_csv(INPUT_PATH / "IPL_COMPETETION_LINKS.csv")

    archived = competition_df[competition_df["is_archive"] == "yes"]
    live = competition_df[competition_df["is_archive"] == "no"]

    for _, row in archived.iterrows():
        process_competition(
            competition_id=row["competition_id"],
            competition_year=row["competition_year"],
            base_url=ARCHIVE_BASE_URL,
        )

    for _, row in live.iterrows():
        process_competition(
            competition_id=row["competition_id"],
            competition_year=row["competition_year"],
            base_url=LIVE_BASE_URL,
        )

    end_time = datetime.now()
    print(f"🏁 Process ended at: {end_time}")
    print(f"⏱ Total time: {(end_time - start_time).total_seconds()} seconds")


if __name__ == "__main__":
    main()
