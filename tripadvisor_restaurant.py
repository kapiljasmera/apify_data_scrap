from apify_client import ApifyClient
import json
import pandas as pd
from helpers import convert_columns_to_json, data_to_sql, create_restaurant_dim_table_trip
from dotenv import load_dotenv
from os import getenv

load_dotenv('.env')

def get_data_apify_res():
    # apify_client = ApifyClient(getenv('apify_api'))

    # # Start an actor and wait for it to finish
    # run_input = {
    #     "locationFullName": "Lisbon",
    #     "maxItems": 20
    # }

    # # Run the actor and wait for it to finish
    # run = apify_client.actor("voyager/tripadvisor-restaurants-scraper").call(run_input=run_input)
    
    output_file = 'data/trip_res_data.json'
    
    # with open(output_file, 'w') as f:
    #     json.dump(list(apify_client.dataset(run['defaultDatasetId']).iterate_items()), f, indent=4)

    df = pd.read_json(output_file)

    # Apply the function to all columns containing lists in the DataFrame
    for column in df.columns:
        df[column] = df[column].apply(convert_columns_to_json)
    
    db_table = 'trip_restaurants_data'
    dim_table = 'restaurant_dim'

    data_to_sql(df, db_table)

    create_restaurant_dim_table_trip(dim_table, db_table)
    
    return

if __name__ == "__main__":
    get_data_apify_res()
