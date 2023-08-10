from apify_client import ApifyClient
import json
import pandas as pd
from helpers import convert_lists_to_json, data_to_sql
from dotenv import load_dotenv
from os import getenv

load_dotenv('.env')

def get_data_apify_res():
    apify_client = ApifyClient(getenv('apify_api'))

    # Start an actor and wait for it to finish
    run_input = {
        "locationFullName": "Lisbon",
        "maxItems": 5
    }

    # Run the actor and wait for it to finish
    run = apify_client.actor("voyager/tripadvisor-restaurants-scraper").call(run_input=run_input)
    rec_data = apify_client.dataset(run['defaultDatasetId']).iterate_items()
    
    output_file = 'data/trip_res_data.json'
    
    with open(output_file, 'w') as f:
        json.dump(list(apify_client.dataset(run['defaultDatasetId']).iterate_items()), f, indent=4)

    with open(output_file, 'r') as json_file:
        data = json.load(json_file)

    df = pd.json_normalize(data)

    # Apply the function to all columns containing lists in the DataFrame
    for column in df.columns:
        df[column] = df[column].apply(convert_lists_to_json)
    
    db_table = 'trip_restaurants_data'

    data_to_sql(df, db_table)
    
    return rec_data

if __name__ == "__main__":
    get_data_apify_res()
