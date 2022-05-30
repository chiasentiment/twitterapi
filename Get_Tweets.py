# from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, Float
import requests
import datetime
from DB_Connection import DBConnection
import time
import os
db_connection = DBConnection()

TWEET_QUERIES = ["CHIA", "DFK"]

bearer_token = os.environ.get("SECRET_KEY")
twitter_api = "https://api.twitter.com/2/tweets/search/recent"
parameters = {
    "user.fields": "username,name,created_at,entities,description,verified",
    "tweet.fields": "text,created_at,lang,conversation_id,author_id,in_reply_to_user_id,public_metrics,"
                    "referenced_tweets",
    "max_results": 100,

}
headers = {"Authorization": f"Bearer {bearer_token}"}

with open("Last_run_date.txt", "r") as last_run_time_handler:
    last_run_time = last_run_time_handler.readline()

x = datetime.datetime.now()
current_time = x.strftime("%Y-%m-%dT%H:%M:%SZ")
parameters["start_time"] = f"{last_run_time.strip()}"
parameters["end_time"] = f"{current_time}"
print(f"Running Time: {current_time}")


for area in TWEET_QUERIES:
    tweet_area = 0
    table = db_connection.create_table(area)
    with open(f'{area}_parameters', "r") as query_file_handler:
        query_params = query_file_handler.readlines()
    for query in query_params:
        tweet_per_query = 0
        page = 0
        pagination = True
        time.sleep(2)
        while pagination:
            page += 1
            query = query.strip()
            print(f"Query: {query}, Page: {page} ")
            parameters["query"] = f"{query}"
            response = requests.get(url=twitter_api, headers=headers, params=parameters)
            contents = response.json()
            try:
                db_connection.insert_tables(table, contents)
                tweet_per_query += int(contents["meta"]["result_count"])
                try:
                    parameters["pagination_token"] = contents["meta"]["next_token"]
                    # print ('pagination token:{parameters["pagination_token"]}')
                except KeyError:
                    pagination = False
                    parameters.pop("pagination_token")
            except KeyError:
                pagination = False
        print (f'tweet_count for {query}: {tweet_per_query}')
        tweet_area += tweet_per_query
    print(f'tweet_count for {area}: {tweet_area}')
with open("Last_run_date.txt", "w") as file_handler:
    file_handler.write(current_time)

