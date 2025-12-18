import pandas as pd
from pathlib import Path
from datetime import datetime

start_time = datetime.now()
print(f"Process started at : {start_time}")

input_path  = Path(f"D:\\git\\text-to-sql\\data\\input")
output_path  = Path(f"D:\\git\\text-to-sql\\data\\output")

match_summary_archive_df = pd.read_csv(f"{output_path}\\match_summary_archive_list.csv")

match_summary_archive_df = match_summary_archive_df[['home_team_id','home_team_name','home_team_logo']]
match_summary_archive_df = match_summary_archive_df.drop_duplicates(subset = ['home_team_id','home_team_name'], keep= 'last').dropna(subset = 'home_team_logo').sort_values(by='home_team_id')
match_summary_archive_df = match_summary_archive_df.rename(columns={
    'home_team_id': 'team_id',
    'home_team_name': 'team_name',
    'home_team_logo': 'team_logo'
})
match_summary_archive_df.to_csv(f"{output_path}\\teams.csv", index=False)

end_time = datetime.now()
print(f"Process ended at : {end_time}")
print(f"Total process time (sec): {(end_time-start_time).total_seconds()}")
