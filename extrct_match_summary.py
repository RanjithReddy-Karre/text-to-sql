import json 
from pathlib import Path
import pandas as pd
from datetime import datetime

start_time = datetime.now()
print(f"Process started at : {start_time}")

input_path = Path("D:\\git\\text-to-sql\\data\\output\\raw\\match_summary")
output_path = Path("D:\\git\\text-to-sql\\data\\output\\processed")

def extract_match_summary(match_json: dict) -> dict:
    """
    Extract important match-level summary fields from raw match JSON.
    """

    return {
        # IDs
        "competition_id": match_json.get("CompetitionID"),
        "season": match_json.get("MatchDate").split('-')[0],
        "match_id": match_json.get("MatchID"),

        # Match info
        "match_name": match_json.get("MatchName"),
        "match_type": match_json.get("MatchType"),
        "match_order": match_json.get("MatchOrder"),

        # Date & time
        "match_date": match_json.get("MatchDateNew"),
        "match_start_time": match_json.get("MatchTime"),
        "match_end_date": match_json.get("MatchEndDate"),
        "match_end_time": match_json.get("MatchEndTime"),
        "timezone": match_json.get("timezone1"),

        # Venue
        "ground_id": match_json.get("GroundID"),
        "ground_name": match_json.get("GroundName"),
        "city": match_json.get("city"),

        # Teams
        "home_team_id": match_json.get("HomeTeamID"),
        "home_team_name": match_json.get("HomeTeamName"),
        "home_team_logo": match_json.get("MatchHomeTeamLogo"),

        "away_team_id": match_json.get("AwayTeamID"),
        "away_team_name": match_json.get("AwayTeamName"),
        "away_team_logo": match_json.get("MatchAwayTeamLogo"),

        # Toss
        "toss_winner": match_json.get("TossTeam"),

        # Scores (final innings summaries)
        
        "first_batting_team": match_json.get("FirstBattingTeamName"),
        "second_batting_team": match_json.get("SecondBattingTeamName"),
        "first_innings_summary": match_json.get("1Summary"),
        "second_innings_summary": match_json.get("2Summary"),
        "first_innings_run_rate": match_json.get("1RunRate"),
        "second_innings_run_rate": match_json.get("2RunRate"),

        # Result
        "result": match_json.get("Comments") or match_json.get("Commentss")
    }

match_summary_archive_list = []

for file in input_path.glob("*.json"):
    
    with open(file, "r", encoding="utf-8-sig") as f:
        json_data = json.load(f)

    match_summery_data = []

    for match_summery_json in json_data['Matchsummary']:
        match_summery_data.append(extract_match_summary(match_summery_json))
        match_summary_archive_list.append(extract_match_summary(match_summery_json))
    
    output_file  = output_path / "match_summary" / f"{str(file).split("\\")[-1].replace('.json', '.csv')}"
    match_summery_df = pd.DataFrame (match_summery_data)
    match_summery_df.to_csv(output_file, index=False)
    print(f"✅ Saved {output_file} .")

output_file_archive  = output_path / f"match_summary_archive_list.csv"
match_summery_archive_df = pd.DataFrame (match_summary_archive_list)
match_summery_archive_df.to_csv(output_file_archive, index=False)
print(f"✅ Saved {output_file_archive} .")


end_time = datetime.now()
print(f"Process ended at : {end_time}")
print(f"Total process time (sec): {(end_time-start_time).total_seconds()}")



