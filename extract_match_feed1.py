




import urllib.request
import urllib.error
import json
import pandas as pd
from datetime import datetime
from pathlib import Path


start_time = datetime.now()
print(f"Process started at : {start_time}")

input_path = Path("D:\\git\\text-to-sql\\data\\input")
output_path = Path("D:\\git\\text-to-sql\\data\\output")
base_archive_url = f"https://ipl-stats-sports-mechanic.s3.ap-south-1.amazonaws.com/ipl/feeds/archievefeeds/"
base_url = f"https://ipl-stats-sports-mechanic.s3.ap-south-1.amazonaws.com/ipl/feeds/"

def create_json_file(competition, match_id, match_name, json_data):
        try:
            # data_json = json.loads(json_data)

            filename = f"{output_path}\\match_feed\\{competition}-{match_id}-{match_name}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=4)  # pretty print

            return(f"✅ Saved {filename} to {output_path}\\match_feed.")

        except json.JSONDecodeError as e:
            print(f"⚠️ Could not parse JSON for {url}: {e}")

def fetch_json_from_url(url, innings):
    try:
        with urllib.request.urlopen(url) as response:
            data_text = response.read().decode("utf-8")
            # Clean the wrapper function call
            data_text = data_text.replace('onScoring(', '').replace(');', '')
            data_json = json.loads(data_text)
        return data_json[innings]

    except urllib.error.HTTPError as e:
        if e.code == 404 and innings == "Innings2":
            # Skip silently if Innings2 doesn't exist
            pass
        elif e.code == 404:
            print(f"⚠️ Warning: 404 Not Found -> {url}")
        else:
            print(f"⚠️ Warning: HTTP Error {e.code} -> {url}")

    except urllib.error.URLError as e:
        print(f"⚠️ Warning: Failed to reach {url} - {e.reason}")

    except Exception as e:
        print(f"⚠️ Warning: Unexpected error with {url} - {e}")

ipl_competetion_df = pd.read_csv(f"{input_path}\\IPL_COMPETETION_LINKS.csv")

archived_competition_list = ipl_competetion_df[ipl_competetion_df['is_archive']=='yes']['competition_id'].tolist()
unarchived_competition_list = ipl_competetion_df[ipl_competetion_df['is_archive']=='no']['competition_id'].tolist()

for competition in archived_competition_list:
    
    competetion_year = ipl_competetion_df[ipl_competetion_df['competition_id']==competition]['competition_year'].max()
    match_summary_df = pd.read_csv(f"{output_path}\\{competetion_year}_IPL_MATCH_SUMMERY.csv")
    match_id_list = match_summary_df['match_id'].tolist()
    for match_id in match_id_list:
        match_name = match_summary_df[match_summary_df['match_id']==match_id]['match_name'].max().replace(" ","-")
        # if match_id == 10000:
        data_dict = {}
        for innings in ["Innings1", "Innings2"]:
            # print(fetch_json_from_url(f"{base_archive_url}{match_id}-{innings}.js"))
            url = f"{base_archive_url}{match_id}-{innings}.js"
            data_dict[innings]= fetch_json_from_url(url, innings)
            print(f"✅ Success: {url}")
        
        print(create_json_file(competition, match_id, match_name, data_dict))

for competition in unarchived_competition_list:
    
    competetion_year = ipl_competetion_df[ipl_competetion_df['competition_id']==competition]['competition_year'].max()
    match_summary_df = pd.read_csv(f"{output_path}\\{competetion_year}_IPL_MATCH_SUMMERY.csv")
    match_id_list = match_summary_df['match_id'].tolist()

    for match_id in match_id_list:
        match_name = match_summary_df[match_summary_df['match_id']==match_id]['match_name'].max().replace(" ","-")
        data_dict = {}
        for innings in ["Innings1", "Innings2"]:
            url = f"{base_url}{match_id}-{innings}.js"
            data_dict[innings]= fetch_json_from_url(url, innings)
            print(f"✅ Success: {url}")
        
        print(create_json_file(competition, match_id, match_name, data_dict))
        
end_time = datetime.now()
print(f"Process ended at : {end_time}")
print(f"Total process time : {(end_time-start_time).total_seconds()}")
