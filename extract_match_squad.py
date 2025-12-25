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

MATCH_SQUAD_PATH = OUTPUT_PATH_RAW / "squad_feed"
SQUAD = ("squadA", "squadB")

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
    return d.get("ClientPlayerID") or d.get("PlayerID")

# =========================
# Extractors
# =========================
def extract_squad(d, mid,  cid):
    return {
        "competition_id": cid,
        "match_id": mid,
        "team_id": safe_int(d.get("TeamID")),
        "player_id": get_player_id(d),
        "player_name": safe_str(d.get("PlayerName")),
        "player_batting_type": safe_str(d.get("BattingType")),
        "player_bowling_proficiency": safe_str(d.get("BowlingProficiency")),
        "player_skill": safe_str(d.get("PlayerSkill")),
        "is_captain": safe_int(d.get("IsCaptain")),
        "is_vicecaptain": safe_int(d.get("IsViceCaptain")),
        "is_wicketkeeper": safe_int(d.get("IsWK")),
        "playing_order": safe_int(d.get("PlayingOrder")),
        "is_foreign_player": safe_int(d.get("IsNonDomestic")),
        "player_image": d.get("PlayerImage"),
    }



# =========================
# Output directories
# =========================
dirs = {
    "match_squad": OUTPUT_PATH_PROCESSED / "match_squad",
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

for file in MATCH_SQUAD_PATH.glob("*.json"):

    with file.open(encoding="utf-8") as f:
        data = json.load(f)

    # -------------------------
    # Extract IDs from filename
    # -------------------------
    # Expected format: competitionId-matchId.json
    stem_parts = file.stem.split("-")

    if len(stem_parts) < 2:
        print(f"⚠️ Skipping file (invalid filename): {file.name}")
        continue

    competition_id = safe_int(stem_parts[0])
    match_id = safe_int(stem_parts[1])

    if not competition_id or not match_id:
        print(f"⚠️ Skipping file (bad IDs): {file.name}")
        continue

    per_match = {k: [] for k in dirs}

    # -------------------------
    # Process both squads
    # -------------------------
    for squad in SQUAD:
        squad_players = data.get(squad, [])

        if not isinstance(squad_players, list):
            continue

        for player in squad_players:
            row = extract_squad(
                d=player,
                mid=match_id,
                cid=competition_id
            )

            per_match["match_squad"].append(row)
            all_data["match_squad"].append(row)

    # -------------------------
    # Write per-match CSV
    # -------------------------
    if per_match["match_squad"]:
        pd.DataFrame(per_match["match_squad"]).to_csv(
            dirs["match_squad"] /
            f"{competition_id}_{match_id}_match_squad_scorecard.csv",
            index=False
        )

        print(f"✅ Processed {competition_id}_{match_id}")
    else:
        print(f"⚠️ No squad data for {competition_id}_{match_id}")

# =========================
# Write consolidated files
# =========================
for k, rows in all_data.items():
    if rows:
        pd.DataFrame(rows).to_csv(
            OUTPUT_PATH_PROCESSED /
            f"all_competitions_all_matches_{k}.csv",
            index=False
        )

        print(f"✅ Processed all_competitions_all_matches_{k}.csv")
# =========================
# End
# =========================
end_time = datetime.now()
print(f"Process ended at : {end_time}")
print(f"Total process time (sec): {(end_time - start_time).total_seconds()}")



