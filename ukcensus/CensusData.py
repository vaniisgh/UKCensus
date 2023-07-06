import requests
import time
import pandas as pd
import polars as pl
import psycopg2
from psycopg2.errors import UndefinedTable
import json

from configparser import ConfigParser


class RateLimitedAPI:
    def __init__(self):
        self.base_url = None
        self.db_host = None
        self.db_port = None
        self.db_url = None
        self.db_user = None
        self.db_password = None
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
        self.db_user = config.get('DB', 'username')
        self.db_password = config.get('DB', 'password')

    def make_request(self, endpoint, params={}):
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
        response = requests.get(url, params=params)
        self.requests_made += 1
        result = response.json()

        if response.status_code == 200:
            return result
        else:
            raise Exception(f"Request failed with status code {response.status_code}")
    
    def fetch_all_data(self, endpoint, return_type="json", p={}):
        limit = 100
        offset = 0
        total_count = -1

        results = []

        while total_count == -1 or offset < total_count:
            params = {
                "limit": limit,
                "offset": offset,
                **p
            }
            response = self.make_request(endpoint, params=params)
            if total_count == -1:
                total_count = response["total_count"]
            
            if response["items"] is not None:
                results.extend(response["items"])
            
            offset += limit

            print(f"Done Fetching {endpoint} {offset}/{total_count}")

            # TODO: Remove this
            if offset > limit:
                break

        
        if return_type == "df":
            return pd.DataFrame(results)
        return results

    def reset_timer(self):
        self.start_time = time.time()
        self.requests_made = 0

    def create_table_if_not_exists(self, table_name):
        conn = psycopg2.connect(host=self.db_host, port=self.db_port, dbname=self.db_url, user=self.db_user)
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
        conn = psycopg2.connect(host=self.db_host, port=self.db_port, dbname=self.db_url, user=self.db_user)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO "{}" (data) VALUES (%s)
            """.format(table_name)

        json_data = json.dumps(data)
        cursor.execute(insert_query, [json_data])

        conn.commit()
        cursor.close()
        conn.close()
    

    def get_results_from_database(self, select_query):
        conn = psycopg2.connect(host=self.db_host, port=self.db_port, dbname=self.db_url, user=self.db_user)
        cursor = conn.cursor()

        cursor.execute(select_query)
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        return pd.DataFrame(data=results, columns=[x[0] for x in cursor.description])

    def get_population_types(self, return_type="json"):
        endpoint = "population-types"
        self.create_table_if_not_exists("population-types")
        response = self.fetch_all_data(endpoint, return_type)

        for item in response: 
            self.add_to_database("population-types", item)

        return response

    def get_area_types(self, return_type="json"):
        areas_query = """
        SELECT data->>'id' as id FROM "area-types"
        """
        try:
            response = self.get_results_from_database(areas_query)

        except UndefinedTable:
            endpoint = "area-types"
            self.create_table_if_not_exists("area-types")

            select_query = """
                SELECT data->>'name' as name FROM "population-types" where data->>'label' = '{}'
                """.format("All usual residents")

            response = self.get_results_from_database(select_query)

            self.create_table_if_not_exists("area-types")
            
            for _,name in response['name'].items():
                endpoint = 'population-types/{population_type}/area-types'.format(population_type=name)
                response = self.fetch_all_data(endpoint, return_type)
                response = [dict(item, **{'population-type':name}) for item in response]
                for item in response:
                    self.add_to_database("area-types", item)
            
            response = self.get_results_from_database(areas_query)
        
        return response
    

    def get_area_infos(self, return_type="json"):
        areas_query = """
        SELECT data->>'id' as id FROM "area-infos"
        """
        try:
            response = self.get_results_from_database(areas_query)

        except UndefinedTable:
            endpoint = "area-infos"
            self.create_table_if_not_exists("area-infos")

            select_query = """
                SELECT data->>'id' as id, data ->>'population-type' as population FROM "area-types"
                """

            response = self.get_results_from_database(select_query)

            self.create_table_if_not_exists("area-infos")
            
            for _,row in response.iterrows():
                endpoint = 'population-types/{population_type}/area-types/{area_type}/areas'.format(population_type=row.population, area_type=row.id)
                response = self.fetch_all_data(endpoint, return_type)
                for item in response:
                    self.add_to_database("area-infos", item)
            
            response = self.get_results_from_database(areas_query)
        
        return response
    

    def get_dimensions(self, q_param, return_type="json"):
        dimension_query = """
        SELECT data->>'id' as id FROM "dimensions"
        """
        try:
            response = self.get_results_from_database(dimension_query)

        except UndefinedTable:
            endpoint = "dimensions"
            self.create_table_if_not_exists("dimensions")

            select_query = """
                SELECT data->>'name' as name FROM "population-types" where data->>'label' = '{}'
                """.format("All usual residents")

            response = self.get_results_from_database(select_query)

            self.create_table_if_not_exists("dimensions")
            
            for _,name in response['name'].items():
                endpoint = 'population-types/{population_type}/dimensions'.format(population_type=name)
                response = self.fetch_all_data(endpoint, return_type, p={"q": q_param})
                response = [dict(item, **{'population-type':name}) for item in response]
                for item in response:
                    self.add_to_database("dimensions", item)
            
            response = self.get_results_from_database(dimension_query)
        
        return response
    

