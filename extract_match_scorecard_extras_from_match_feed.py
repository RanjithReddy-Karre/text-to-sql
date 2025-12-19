import json
import pandas as pd
from datetime import datetime
from pathlib import Path

# =========================
# Process start time
# =========================
start_time = datetime.now()
print(f"Process started at : {start_time}")

# =========================
# Configuration
# =========================
OUTPUT_PATH_RAW = Path("D:/git/text-to-sql/data/output/raw")
OUTPUT_PATH_PROCESSED = Path("D:/git/text-to-sql/data/output/processed")

MATCH_FEED_PATH = OUTPUT_PATH_RAW / "match_feed"
INNINGS = ("Innings1", "Innings2")

# =========================
# Helper functions
# =========================
def safe_float(value, default=0.0):
    if value in (None, "", "-"):
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    if value in (None, "", "-"):
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_str(value, default=""):
    if value in (None, ""):
        return default
    return str(value).strip()


def get_player_id(data: dict) -> str | int:
    if data.get("PLAYER_ID") not in (None, "", "-"):
        return data.get("PLAYER_ID")
    return data.get("PlayerID")

# =========================
# Batting extractor
# =========================
def extract_batting_stats(data: dict, competition_id: int) -> dict:
    return {
        "competition_id": competition_id,
        "match_id": safe_int(data.get("MatchID")),
        "innings_no": safe_int(data.get("InningsNo")),
        "team_id": safe_int(data.get("TeamID")),
        "player_id": get_player_id(data),
        "player_name": safe_str(data.get("PlayerName")),
        "playing_order": safe_int(data.get("PlayingOrder")),
        "bowler_name": safe_str(data.get("BowlerName")),
        "out_description": safe_str(data.get("OutDesc")),
        "runs": safe_int(data.get("Runs")),
        "balls": safe_int(data.get("Balls")),
        "ones": safe_int(data.get("Ones")),
        "twos": safe_int(data.get("Twos")),
        "threes": safe_int(data.get("Threes")),
        "fours": safe_int(data.get("Fours")),
        "sixes": safe_int(data.get("Sixes")),
        "strike_rate": safe_float(data.get("StrikeRate")),
        "boundary_percentage": safe_float(data.get("BoundaryPercentage")),
        "dot_balls": safe_int(data.get("DotBalls")),
        "dot_ball_percentage": safe_float(data.get("DotBallPercentage")),
        "wicket_no": data.get("WicketNo"),
    }

# =========================
# Bowling extractor (NO short name)
# =========================
def extract_bowling_stats(data: dict, competition_id: int) -> dict:
    return {
        "competition_id": competition_id,
        "match_id": safe_int(data.get("MatchID")),
        "innings_no": safe_int(data.get("InningsNo")),
        "team_id": safe_int(data.get("TeamID")),
        "player_id": get_player_id(data),
        "player_name": safe_str(data.get("PlayerName")),
        "overs": safe_float(data.get("Overs")),
        "maidens": safe_int(data.get("Maidens")),
        "runs_conceded": safe_int(data.get("Runs")),
        "wickets": safe_int(data.get("Wickets")),
        "wides": safe_int(data.get("Wides")),
        "no_balls": safe_int(data.get("NoBalls")),
        "economy": safe_float(data.get("Economy")),
        "bowling_order": safe_int(data.get("BowlingOrder")),
        "legal_balls": safe_int(data.get("TotalLegalBallsBowled")),
        "scoring_balls": safe_int(data.get("ScoringBalls")),
        "dot_balls": safe_int(data.get("DotBalls")),
        "dot_ball_percent": safe_float(data.get("DBPercent")),
        "dot_ball_frequency": safe_float(data.get("DBFrequency")),
        "ones": safe_int(data.get("Ones")),
        "twos": safe_int(data.get("Twos")),
        "threes": safe_int(data.get("Threes")),
        "fours_conceded": safe_int(data.get("Fours")),
        "sixes_conceded": safe_int(data.get("Sixes")),
        "boundary_percent": safe_float(data.get("BdryPercent")),
        "boundary_frequency": safe_float(data.get("BdryFreq")),
        "strike_rate": safe_float(data.get("StrikeRate")),
        "four_percent": safe_float(data.get("FourPercent")),
        "six_percent": safe_float(data.get("SixPercent")),
    }

# =========================
# Validation
# =========================
def is_valid_row(row: dict) -> bool:
    return row.get("match_id", 0) > 0

# =========================
# Output directories
# =========================
batting_dir = OUTPUT_PATH_PROCESSED / "batting_scorecard"
bowling_dir = OUTPUT_PATH_PROCESSED / "bowling_scorecard"

batting_dir.mkdir(parents=True, exist_ok=True)
bowling_dir.mkdir(parents=True, exist_ok=True)

# =========================
# Consolidated containers
# =========================
all_batting_rows = []
all_bowling_rows = []

# =========================
# Main loop
# =========================
for file in MATCH_FEED_PATH.glob("*.json"):

    with file.open(encoding="utf-8") as f:
        data = json.load(f)

    competition_id = safe_int(file.name.split("-")[0])

    match_batting = []
    match_bowling = []

    for innings in INNINGS:
        innings_data = data.get(innings)
        if not innings_data:
            continue

        # Batting
        for player in innings_data.get("BattingCard", []):
            row = extract_batting_stats(player, competition_id)
            if is_valid_row(row):
                match_batting.append(row)
                all_batting_rows.append(row)

        # Bowling
        for bowler in innings_data.get("BowlingCard", []):
            row = extract_bowling_stats(bowler, competition_id)
            if is_valid_row(row):
                match_bowling.append(row)
                all_bowling_rows.append(row)

    if not match_batting and not match_bowling:
        print(f"⚠️ Skipping abandoned/no-play match: {file.name}")
        continue

    match_id = (
        match_batting[0]["match_id"]
        if match_batting
        else match_bowling[0]["match_id"]
    )

    # Write individual files
    if match_batting:
        pd.DataFrame(match_batting).to_csv(
            batting_dir / f"{competition_id}_{match_id}_batting_scorecard.csv",
            index=False
        )

    if match_bowling:
        pd.DataFrame(match_bowling).to_csv(
            bowling_dir / f"{competition_id}_{match_id}_bowling_scorecard.csv",
            index=False
        )

    print(f"✅ Processed match {competition_id}_{match_id}")

# =========================
# Write consolidated files
# =========================
if all_batting_rows:
    pd.DataFrame(all_batting_rows).to_csv(
        OUTPUT_PATH_PROCESSED / "all_competitions_all_matches_batting_scorecard.csv",
        index=False
    )

if all_bowling_rows:
    pd.DataFrame(all_bowling_rows).to_csv(
        OUTPUT_PATH_PROCESSED / "all_competitions_all_matches_bowling_scorecard.csv",
        index=False
    )

# =========================
# Process end
# =========================
end_time = datetime.now()
print(f"Process ended at : {end_time}")
print(f"Total process time (sec): {(end_time - start_time).total_seconds()}")
