import requests
import time
import pandas as pd
import polars as pl
import psycopg2
from psycopg2.errors import UndefinedTable
import json

from configparser import ConfigParser

from ast import literal_eval

from ukcensus.utils import generate_subsets
    
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
        print(f"Elapsed time: {elapsed_time}")  
        print(f"Requests made: {self.requests_made}")
        if elapsed_time < 10 and self.requests_made >= 80:
            wait_time = 10 - elapsed_time
            time.sleep(wait_time)
            self.reset_timer()

        if elapsed_time < 60 and self.requests_made >= 180:
            wait_time = 60 - elapsed_time
            time.sleep(wait_time)
            self.reset_timer()

        url = f"{self.base_url}/{endpoint}"
        print(f"Making request to {url}")
        response = requests.get(url, params=params)
        self.requests_made += 1

        if response.status_code == 400:
            print("400 error")
            print(response.json())
            return None
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
            if response is None:
                return []

            if "observations" in response:
                results = response["observations"]
                total_count = 1
                break
            
            if total_count == -1:
                total_count = response["total_count"]
            
            if response["items"] is not None:
                print(response["items"])
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

    def get_area_types(self, return_type="json", population_type=None) :

        areas_query = """
        SELECT data->>'id' as id FROM "area-types"
        """
        if population_type:
            areas_query = """
            SELECT data->>'id' as id FROM "area-types"
            WHERE data->>'population-type' = '{}'
            """.format(population_type)

        try:
            response = self.get_results_from_database(areas_query)
            if len(response) <1:
                raise UndefinedTable
        except UndefinedTable:
            endpoint = "area-types"
            self.create_table_if_not_exists("area-types")

            if population_type:
                areas_query = """
                SELECT data->>'id' as id FROM "area-types"
                WHERE data->>'population-type' = '{}'
                """
                endpoint = "population-types/{population_type}/area-types".format(population_type=population_type)
                response = self.fetch_all_data(endpoint, return_type)
                response = [dict(item, **{'population-type':population_type}) for item in response]
                for item in response:
                    self.add_to_database("area-types", item)

                response = self.get_results_from_database(areas_query)
                return response
            select_query = """
                SELECT data->>'name' as name FROM "population-types" where data->>'type' = '{}'
                """.format("microdata")

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
    

    def get_area_infos(self, return_type="json", population_type = None):
        areas_query = """
        SELECT data->>'id' as id FROM "area-infos"
        """
        try:
            response = self.get_results_from_database(areas_query)
            if population_type:
                areas_query = """
                SELECT data->>'id' as id FROM "area-types"
                WHERE data->>'population-type' = '{}'
                """.format(population_type)
                response = self.get_results_from_database(areas_query)
                if len(response) <1:
                    raise UndefinedTable
        except UndefinedTable:
            endpoint = "area-infos"
            self.create_table_if_not_exists("area-infos")
            
            if population_type:
                select_query = """
                SELECT data->>'id' as id, data ->>'population-type' as population FROM "area-types"
                WHERE data->>'population-type' = '{}'
                """.format(population_type)
            else:
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
    

    def get_dimensions(self, q_param, population_type=None,return_type="json"):
        # dimension_query = """
        # SELECT data->>'id' as id FROM "dimensions"
        # """
        # if population_type:
        #     dimension_query = """
        #     SELECT data->>'id' as id FROM "dimensions"
        #     WHERE data->>'population-type' = '{}'
        #     """.format(population_type)

        # try:
        #     response = self.get_results_from_database(dimension_query)
        #     response = response[response['id'].str.contains(q_param)]
        #     if len(response) < 1:
        #         raise UndefinedTable
        #     return response
        # except UndefinedTable:
        endpoint = "dimensions"
        self.create_table_if_not_exists("dimensions")

        select_query = """
            SELECT data->>'name' as name FROM "population-types" where data->>'type' = '{}'
            """.format("microdata")

        response = self.get_results_from_database(select_query)

        self.create_table_if_not_exists("dimensions")
        
        for _,name in response['name'].items():
            endpoint = 'population-types/{population_type}/dimensions'.format(population_type=name)
            print(q_param)
            response = self.fetch_all_data(endpoint, return_type, p={"q": q_param})
            response = [dict(item, **{'population-type':name}) for item in response]
            for item in response:
                self.add_to_database("dimensions", item)
        
        # response = self.get_results_from_database(dimension_query)
        
        return 
    

    def get_categories(self,dimension_id = "hh_multi_religion"):
        select_categories = """
                SELECT * from "categories" where data->>'dimension' = '{}'
                """.format(dimension_id)
        try:
            response = self.get_results_from_database(select_categories)
            if response.empty:
                raise UndefinedTable
        except UndefinedTable:
            self.create_table_if_not_exists("categories")

            select_query = """
                    SELECT data->>'id' as dimension, data->> 'population-type' as population
                        FROM "dimensions" where data->>'id' = '{}'
                    """.format(dimension_id)
            dimension = self.get_results_from_database(select_query)
            for _, row in dimension.iterrows():
                population = row.population
                dimension_id = row.dimension
                endpoint = '/population-types/{population_type}/' \
                        '/dimensions/{dimension_id}/categorisations'\
                            .format(population_type=population, dimension_id=dimension_id)
                response = self.fetch_all_data(endpoint, return_type="json")
                for item in response:
                    self.add_to_database("categories", item)
        
        response = self.get_results_from_database(select_categories)
        return response


    def get_data_final(self,return_type="json", dimension_id = "hh_multi_religion"):
        data_query = """
        SELECT * FROM "data_mt" where data->>'dimension_id' = '{}'
        """.format(dimension_id)

        try:
            response = self.get_results_from_database(data_query)
            if len(response) <1 :
                raise UndefinedTable
        except UndefinedTable:
            endpoint = "data_mt"
            self.create_table_if_not_exists("data_mt")

            select_query = """
                SELECT data->>'name' as population
                  FROM "population-types" where data->>'type' = '{}'
                """.format("microdata")


            populations = self.get_results_from_database(select_query)

            for _,row in populations.iterrows():
                select_query = """
                SELECT data->>'id' as dimension
                  FROM "dimensions" where data->>'population-type' = '{}'
                """.format(row.population)
                dimension = self.get_results_from_database(select_query)

                get_area_types = """
                SELECT data->>'id' as area_type
                    FROM "area-types" where data->>'population-type' = '{}'
                """.format(row.population)
                area_types = self.get_results_from_database(get_area_types)['area_type'].to_list()

                get_area_codes = """
                SELECT data->>'id' as area_code, data->>'area_type' as area_type
                    FROM "area-infos" where data->>'area_type' = ANY(ARRAY['{}'])
                """.format("','".join(area_types))
                area_codes = self.get_results_from_database(get_area_codes)

                for _, sub_row in dimension.iterrows():
                    population_type = row.population
                    dimension_id = sub_row.dimension
                    for _, area in area_codes.iterrows():
                        endpoint = 'population-types/{population_type}/census-observations?area-type={area_type},{area_code}&dimensions={dimestion_id}'\
                            '&limit={limit}'.format(population_type=population_type, dimestion_id=dimension_id, area_type=area.area_type, area_code=area.area_code, limit=1000)
                        response = self.fetch_all_data(endpoint, return_type, p={})
                        response = [dict(item, **{'population-type':population_type, 'dimension-id': dimension_id}) for item in response]
                        for item in response:
                            self.add_to_database("data_mt", item)
            
            response = self.get_results_from_database(data_query)
        
        return response


    def get_multi_final_data(self, population_type,return_type="json", dimension = [], how="all",n=1):
        """
        how -> all or any
        n -> number of dimensions to be taken at a time
        """
        data_query = """
                SELECT distinct(data->>'dimension-id') as dimensions_present
                FROM "data_mt" where data->>'population-type' = '{}'
            """.format(population_type)
        data = self.get_results_from_database(data_query)
        data = data['dimensions_present'].apply(literal_eval).to_list()

        # except (UndefinedTable, TypeError) as e:
        endpoint = "data_mt"
        self.create_table_if_not_exists("data_mt")

        get_area_types = """
        SELECT data->>'id' as area_type
            FROM "area-types" where data->>'population-type' = '{}'
        """.format(population_type)
        area_types = self.get_results_from_database(get_area_types)['area_type'].to_list()

        get_area_codes = """
        SELECT data->>'id' as area_code, data->>'area_type' as area_type
            FROM "area-infos" where data->>'area_type' = ANY(ARRAY['{}'])
        """.format("','".join(area_types))
        area_codes = self.get_results_from_database(get_area_codes)

        dimensions = []
        if how == "any":
            # make all powerset of all sets of length n 
            dimensions.extend(generate_subsets(dimension))
        elif how == "all":
            dimensions = dimension[0]
        else:
            raise ValueError("how can only be any or all")
        for dimension_id in dimensions:
            # check if data is already present in database
            print("checking for dimension {}".format(dimension_id))
            if list(dimension_id) in data:
                print("data already present for dimension {}".format(dimension_id))
                continue
            for _, area in area_codes.iterrows():
        
                endpoint = 'population-types/{population_type}/census-observations?area-type={area_type},{area_code}&dimensions={dimestion_id}'\
                    '&limit={limit}'.format(population_type=population_type, dimestion_id=','.join(list(dimension_id)), area_type=area.area_type, area_code=area.area_code, limit=1000)
                response = self.fetch_all_data(endpoint, return_type, p={})
                if not response:
                    continue
                response = [dict(item, **{'population-type':population_type, 'dimension-id': list(dimension_id)}) for item in response]
                for item in response:
                    self.add_to_database("data_mt", item)
        # response = self.get_results_from_database(data_query)
        
        return 



    def get_filtered_dimension(self,population_type='UR', return_type="list", _filter=''):
        """
        filter -> string to be filtered
        """
        select_query = """
            SELECT data->>'id' as dimension
              FROM "dimensions" where  data->>'id' like '%\{}%'
              AND data->>'population-type' = '{}'
            """.format(_filter,population_type)
        response = self.get_results_from_database(select_query)
        if return_type == "list":
            response = response['dimension'].to_list()
        return response