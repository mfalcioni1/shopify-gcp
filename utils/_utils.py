# built in
import json
import os

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