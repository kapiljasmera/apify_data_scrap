from apify_client import ApifyClient
import json
import pandas as pd
from helpers import convert_lists_to_json, data_to_sql, get_url_from_sql
from dotenv import load_dotenv
from os import getenv

load_dotenv('.env')

def get_reviews():
    apify_client = ApifyClient(getenv('apify_api'))
    get_db_table = 'trip_hotel_data'
    raw_url = get_url_from_sql(get_db_table)
    rev = []
    for url in raw_url:
        run_input = {
            "language": "en", 
            "maxReviews": 2, 
            "scrapeReviewerInfo": True, 
            "startUrls": [{"url": url[0]}]
        }
        run = apify_client.actor("maxcopell/tripadvisor-reviews").call(run_input=run_input)
        rec_review = apify_client.dataset(run['defaultDatasetId']).iterate_items()
        
        output_file = 'data/trip_hotel_reviews.json'
        
        rev.extend(list(rec_review))
    with open(output_file, 'w') as f:
        json.dump(rev, f, indent=4)

        # Load JSON data from the file
    with open(output_file, 'r') as json_file:
        data = json.load(json_file)

    for item in data:
        for i in item:
            item[i] = json.dumps(item[i])

    df = pd.json_normalize(data)

    # Apply the function to all columns containing lists in the DataFrame
    for column in df.columns:
        df[column] = df[column].apply(convert_lists_to_json)

    db_table = 'trip_hotel_review'

    data_to_sql(df, db_table)
    return


if __name__ == '__main__':
    get_reviews()