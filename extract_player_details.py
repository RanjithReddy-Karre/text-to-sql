import pandas as pd
from pathlib import Path
from datetime import datetime

start_time = datetime.now()
print(f"Process started at : {start_time}")

INPUT_PATH = Path("D:/git/text-to-sql/data/input")
OUTPUT_PATH_PROCESSED = Path("D:/git/text-to-sql/data/output/processed")


all_matches_squad_df = pd.read_csv(OUTPUT_PATH_PROCESSED / f"all_competitions_all_matches_match_squad.csv")
ipl_competetion_list = pd.read_csv(INPUT_PATH / f"IPL_COMPETETION_LINKS.csv")

merged_df = pd.merge(all_matches_squad_df, ipl_competetion_list, on='competition_id', how='inner')

all_matches_squad_sorted_df = merged_df.sort_values(by=['competition_year', 'match_id'])[['player_id','player_name','player_batting_type','player_bowling_proficiency','player_skill', 'is_foreign_player','player_image', 'competition_year']]
all_matches_squad_sorted_df['player_name'] = all_matches_squad_sorted_df['player_name'].str.replace(r"\s[^A-Za-z].*", "", regex=True)
players_df = all_matches_squad_sorted_df.sort_values(by=['player_id', 'player_batting_type'])
players_df = all_matches_squad_sorted_df.drop_duplicates(subset = 'player_id', keep= 'last').rename(columns = {'competition_year' : 'last_played_in'}).sort_values('player_id')

players_df.to_csv(OUTPUT_PATH_PROCESSED / f"players.csv", index=False)

end_time = datetime.now()
print(f"Process ended at : {end_time}")
print(f"Total process time (sec): {(end_time-start_time).total_seconds()}")
