import requests
import time
import polars as pl
import psycopg2
import json

from configparser import ConfigParser


class RateLimitedAPI:
    def __init__(self):
        self.base_url = None
        self.db_host = None
        self.db_port = None
        self.db_url = None
        self.requests_made = 0
        self.start_time = time.time()
        self.load_config()

    def load_config(self):
        config = ConfigParser()
        config.read('./config.ini')
        self.base_url = config.get('API', 'base_url')
        self.db_host = config.get('DB', 'host')
        self.db_port = config.get('DB', 'port')
        self.db_url = config.get('DB', 'url')

    def make_request(self, endpoint, return_type="json"):
        elapsed_time = time.time() - self.start_time
        if elapsed_time < 10 and self.requests_made >= 120:
            wait_time = 10 - elapsed_time
            time.sleep(wait_time)
            self.reset_timer()

        if elapsed_time < 60 and self.requests_made >= 200:
            wait_time = 60 - elapsed_time
            time.sleep(wait_time)
            self.reset_timer()

        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url)
        self.requests_made += 1
        result = response.json()

        if response.status_code == 200:
            if return_type == "pl_df":
                try:
                        return pl.from_dict(result["items"])
                except KeyError:
                    if return_type == "pl_df":
                        return pl.from_dict(result)
            else:
                try:
                    return result["items"]
                except KeyError:
                    return result
        else:
            raise Exception(f"Request failed with status code {response.status_code}")

    def reset_timer(self):
        self.start_time = time.time()
        self.requests_made = 0

    def create_table_if_not_exists(self, table_name):
        conn = psycopg2.connect(host=self.db_host, port=self.db_port, dbname=self.db_url)
        cursor = conn.cursor()

        create_query = """
            CREATE TABLE IF NOT EXISTS "{}" (
                id SERIAL PRIMARY KEY,
                data JSONB
            )
            """.format(table_name)

        cursor.execute(create_query)
        conn.commit()
        cursor.close()
        conn.close()

    def add_to_database(self,table_name, data):
        conn = psycopg2.connect(host=self.db_host, port=self.db_port, dbname=self.db_url)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO "{}" (data) VALUES (%s)
            """.format(table_name)

        json_data = json.dumps(data)
        cursor.execute(insert_query, [json_data])

        conn.commit()
        cursor.close()
        conn.close()

    def get_population_types(self, return_type="json"):
        endpoint = "population-types"
        self.create_table_if_not_exists(endpoint)
        response = self.make_request(endpoint, return_type)

        self.add_to_database(endpoint, response)

        return response
