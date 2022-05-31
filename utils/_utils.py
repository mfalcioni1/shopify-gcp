# built in
import json
import os
import datetime
import re

def load_config(path = None):
    if path == None:
        with open(os.getenv('AUTH')) as f:
            config = json.load(f)
    else:
        with open(path) as f:
            config = json.load(f)
    return config

def gcp_config(path = None):
    config = load_config(path)
    os.environ["GOOGLE_CLOUD_PROJECT"] = config["project-id"]
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "") == "":
        print("Looking for local service account...")
        if "data-bot" in config:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["data-bot"]
        else:
            print("No credentials found!")
            raise
    
    data_bot = str.split(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), "/")[-1]
    data_bot = str.split(data_bot, ".")[0]
    
    return print(
    f"""
    Cloud Project: {config['project-id']} 
    Service Account: {data_bot}.
    """
    )

def date_stamp(name: str):
    utc = datetime.datetime.utcnow()
    s_utc = utc.strftime("%Y%m%d")
    return f"{name}{s_utc}"

def check_latest(api_res, gcs_iter):
    newest = api_res[api_res['created_at'] == api_res['created_at'].max()]['created_at'].iloc[0]
    latest_api = int(datetime.datetime.fromisoformat(newest).strftime("%Y%m%d"))
    if gcs_iter:
        r = re.compile("\d+")
        latest_db = max([int(r.search(x).group(0)) for x in gcs_iter])
        update_bool = latest_api > latest_db
    else:
        print("No files in GCS. Setting update to True.")
        latest_db = None
        update_bool = True
    return latest_api, latest_db, update_bool