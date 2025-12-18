import pandas as pd
from pathlib import Path
from datetime import datetime

start_time = datetime.now()
print(f"Process started at : {start_time}")

input_path  = Path(f"D:\\git\\text-to-sql\\data\\output\\processed\\match_summary")
output_path  = Path(f"D:\\git\\text-to-sql\\data\\output")

match_summary_archive_df = pd.read_csv(f"{input_path}\\match_summary_archive_list.csv")

venue_details_df = match_summary_archive_df[['ground_id','ground_name','city']]
venue_details_df = venue_details_df.drop_duplicates(subset = ['ground_id','ground_name','city'], keep= 'last').dropna(subset = 'ground_name').sort_values(by='ground_id')
venue_details_df.to_csv(f"{output_path}\\venue.csv", index=False)

end_time = datetime.now()
print(f"Process ended at : {end_time}")
print(f"Total process time (sec): {(end_time-start_time).total_seconds()}")
