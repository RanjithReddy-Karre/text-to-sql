import urllib.request
import urllib.error
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

start_time = datetime.now()
print(f"Process started at : {start_time}")


# base_url = "https://ipl-stats-sports-mechanic.s3.ap-south-1.amazonaws.com/ipl/feeds/archievefeeds"
year = ''
destination_path = Path("D:\\git\\text-to-sql\\data\\output\\raw")
ipl_competition_links_df = pd.read_csv("D:\\git\\text-to-sql\\data\\input\\IPL_COMPETETION_LINKS.csv")
competition_list = ipl_competition_links_df["link"].tolist()

for competiton_link in competition_list:
    # for innings in ["Innings1", "Innings2"]:

    try:
        with urllib.request.urlopen(competiton_link) as response:
            data_text = response.read().decode("utf-8-sig")
            # Clean the wrapper function call
            data_text = data_text.replace('MatchSchedule(', '').replace(');', '')
        print(f"✅ Success: {competiton_link}")

        try:
            data_json = json.loads(data_text)
            year = data_json['Matchsummary'][0]['MatchDate'].split('-')[0]

            filename = f"{year}_IPL_MATCH_SUMMERY.json"
            with open( destination_path / filename, "w", encoding="utf-8-sig") as f:
                json.dump(data_json, f, indent=4)  # pretty print

            print(f"✅ Saved {filename} to {destination_path} path.")

        except json.JSONDecodeError as e:
            print(f"⚠️ Could not parse JSON for {competiton_link}: {e}")

    except urllib.error.HTTPError as e:
        # if e.code == 404 and innings == "Innings2":
        #     # Skip silently if Innings2 doesn't exist
        #     continue
        if e.code == 404:
            print(f"⚠️ Warning: 404 Not Found -> {competiton_link}")
        else:
            print(f"⚠️ Warning: HTTP Error {e.code} -> {competiton_link}")

    except urllib.error.URLError as e:
        print(f"⚠️ Warning: Failed to reach {competiton_link} - {e.reason}")

    except Exception as e:
        print(f"⚠️ Warning: Unexpected error with {competiton_link} - {e}")

end_time = datetime.now()
print(f"Process ended at : {end_time}")
print(f"Total process time (sec): {(end_time-start_time).total_seconds()}")
