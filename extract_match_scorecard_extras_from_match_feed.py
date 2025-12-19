import json
import pandas as pd
from datetime import datetime
from pathlib import Path

# =========================
# Process start
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
# Helpers
# =========================
def safe_int(v, d=0):
    if v in (None, "", "-"):
        return d
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return d


def safe_float(v, d=0.0):
    if v in (None, "", "-"):
        return d
    try:
        return float(v)
    except (ValueError, TypeError):
        return d


def safe_str(v, d=""):
    if v in (None, ""):
        return d
    return str(v).strip()


def get_player_id(d):
    return d.get("PLAYER_ID") or d.get("PlayerID")

# =========================
# Extractors
# =========================
def extract_batting(d, cid):
    return {
        "competition_id": cid,
        "match_id": safe_int(d.get("MatchID")),
        "innings_no": safe_int(d.get("InningsNo")),
        "team_id": safe_int(d.get("TeamID")),
        "player_id": get_player_id(d),
        "player_name": safe_str(d.get("PlayerName")),
        "playing_order": safe_int(d.get("PlayingOrder")),
        "runs": safe_int(d.get("Runs")),
        "balls": safe_int(d.get("Balls")),
        "fours": safe_int(d.get("Fours")),
        "sixes": safe_int(d.get("Sixes")),
        "strike_rate": safe_float(d.get("StrikeRate")),
        "dot_balls": safe_int(d.get("DotBalls")),
    }


def extract_bowling(d, cid):
    return {
        "competition_id": cid,
        "match_id": safe_int(d.get("MatchID")),
        "innings_no": safe_int(d.get("InningsNo")),
        "team_id": safe_int(d.get("TeamID")),
        "player_id": get_player_id(d),
        "player_name": safe_str(d.get("PlayerName")),
        "overs": safe_float(d.get("Overs")),
        "maidens": safe_int(d.get("Maidens")),
        "runs_conceded": safe_int(d.get("Runs")),
        "wickets": safe_int(d.get("Wickets")),
        "economy": safe_float(d.get("Economy")),
        "dot_balls": safe_int(d.get("DotBalls")),
    }


def extract_extras(d, cid):
    return {
        "competition_id": cid,
        "match_id": safe_int(d.get("MatchID")),
        "innings_no": safe_int(d.get("InningsNo")),
        "team_id": safe_int(d.get("TeamID")),
        "total_extras": safe_int(d.get("TotalExtras")),
        "byes": safe_int(d.get("Byes")),
        "leg_byes": safe_int(d.get("LegByes")),
        "no_balls": safe_int(d.get("NoBalls")),
        "wides": safe_int(d.get("Wides")),
        "current_run_rate": safe_float(d.get("CurrentRunRate")),
    }


def extract_fow(d, cid):
    return {
        "competition_id": cid,
        "match_id": safe_int(d.get("MatchID")),
        "innings_no": safe_int(d.get("InningsNo")),
        "team_id": safe_int(d.get("TeamID")),
        "player_id": get_player_id(d),
        "player_name": safe_str(d.get("PlayerName")),
        "fall_score": safe_int(d.get("FallScore")),
        "fall_wickets": safe_int(d.get("FallWickets")),
        "fall_overs": safe_float(d.get("FallOvers")),
    }


def extract_over_history_ball(d, cid):
    return {
        "competition_id": cid,
        "match_id": safe_int(d.get("MatchID")),
        "innings_no": safe_int(d.get("InningsNo")),
        "over_no": safe_int(d.get("OverNo")),
        "ball_no": safe_int(d.get("BallNo")),
        "ball_id": safe_str(d.get("BallID")),
        "ball_unique_id": safe_int(d.get("BallUniqueID")),

        "batting_team_id": safe_int(d.get("BattingTeamID")),
        "striker_id": safe_str(d.get("StrikerID")),
        "non_striker_id": safe_str(d.get("NonStrikerID")),
        "bowler_id": safe_str(d.get("BowlerID")),

        # 🔑 IMPORTANT CHANGE (as requested)
        "runs_off_bat": safe_int(d.get("ActualRuns")),

        "extras": safe_int(d.get("Extras")),
        "total_runs": safe_int(d.get("TotalRuns")),
        "total_wickets": safe_int(d.get("TotalWickets")),
        "is_dot_ball": safe_int(d.get("IsDotball")),
        "is_four": safe_int(d.get("IsFour")),
        "is_six": safe_int(d.get("IsSix")),
        "is_wicket": safe_int(d.get("IsWicket")),
        "wicket_type": safe_str(d.get("WicketType")),

        "is_wide": safe_int(d.get("IsWide")),
        "is_no_ball": safe_int(d.get("IsNoBall")),
        "is_bye": safe_int(d.get("IsBye")),
        "is_leg_bye": safe_int(d.get("IsLegBye")),

        "bowling_line": safe_str(d.get("BOWLING_LINE_ID")),
        "bowling_length": safe_str(d.get("BOWLING_LENGTH_ID")),
        "bowl_type": safe_str(d.get("BowlTypeName")),
        "shot_type": safe_str(d.get("ShotType")),
        "is_bouncer": safe_int(d.get("IsBouncer")),
        "is_free_hit": safe_int(d.get("IsFreeHit")),
    }

# =========================
# Validation
# =========================
def valid(row):
    return row.get("match_id", 0) > 0

def is_valid_over_history_ball(row: dict) -> bool:
    return (
        row.get("match_id", 0) > 0
        and 1 <= row.get("ball_no", 0) <= 6
    )

# =========================
# Output directories
# =========================
dirs = {
    "batting": OUTPUT_PATH_PROCESSED / "batting_scorecard",
    "bowling": OUTPUT_PATH_PROCESSED / "bowling_scorecard",
    "extras": OUTPUT_PATH_PROCESSED / "extras_scorecard",
    "fow": OUTPUT_PATH_PROCESSED / "fall_of_wickets",
    "over_history": OUTPUT_PATH_PROCESSED / "over_history",
}

for d in dirs.values():
    d.mkdir(parents=True, exist_ok=True)

# =========================
# Consolidated containers
# =========================
all_data = {k: [] for k in dirs}

# =========================
# Main loop (SINGLE JSON READ)
# =========================
for file in MATCH_FEED_PATH.glob("*.json"):

    with file.open(encoding="utf-8") as f:
        data = json.load(f)

    competition_id = safe_int(file.name.split("-")[0])
    per_match = {k: [] for k in dirs}

    for innings in INNINGS:
        inn = data.get(innings)
        if not inn:
            continue

        for r in inn.get("BattingCard", []):
            row = extract_batting(r, competition_id)
            if valid(row):
                per_match["batting"].append(row)
                all_data["batting"].append(row)

        for r in inn.get("BowlingCard", []):
            row = extract_bowling(r, competition_id)
            if valid(row):
                per_match["bowling"].append(row)
                all_data["bowling"].append(row)

        for r in inn.get("Extras", []):
            row = extract_extras(r, competition_id)
            if valid(row):
                per_match["extras"].append(row)
                all_data["extras"].append(row)

        for r in inn.get("FallOfWickets", []):
            row = extract_fow(r, competition_id)
            if valid(row):
                per_match["fow"].append(row)
                all_data["fow"].append(row)

        for r in inn.get("OverHistory", []):
            row = extract_over_history_ball(r, competition_id)
            if not is_valid_over_history_ball(row):
                continue
            per_match["over_history"].append(row)
            all_data["over_history"].append(row)

    if not any(per_match.values()):
        continue

    match_id = next(v[0]["match_id"] for v in per_match.values() if v)

    for k, rows in per_match.items():
        if rows:
            pd.DataFrame(rows).to_csv(
                dirs[k] / f"{competition_id}_{match_id}_{k}_scorecard.csv",
                index=False
            )

    print(f"✅ Processed {competition_id}_{match_id}")

# =========================
# Write consolidated files
# =========================
for k, rows in all_data.items():
    if rows:
        pd.DataFrame(rows).to_csv(
            OUTPUT_PATH_PROCESSED / f"all_competitions_all_matches_{k}_scorecard.csv",
            index=False
        )

# =========================
# End
# =========================
end_time = datetime.now()
print(f"Process ended at : {end_time}")
print(f"Total process time (sec): {(end_time - start_time).total_seconds()}")
