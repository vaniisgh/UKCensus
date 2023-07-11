from ukcensus.CensusData import RateLimitedAPI

from pandas import json_normalize, concat
import json


data = RateLimitedAPI().get_dimensional_data(dimension_id=['religion_tb','resident_age_8a'],population_type='UR')
#split pandas column to multiple columns json
column = "data"
data = json_normalize(data[column].tolist()).add_prefix(f"{column}.")

column = "data.dimensions"
data = json_normalize(data[column]).add_prefix(f"{column}.")
data = data.rename(columns={'data.dimensions.0': 'area_info', 'data.dimensions.1': 'age_info', 'data.dimensions.2': 'religion_info'})

column = "area_info"
area_info = json_normalize(data[column].tolist()).add_prefix(f"{column}.")

data = concat([data, area_info], axis=1)

column = "age_info"
age_info = json_normalize(data[column].tolist()).add_prefix(f"{column}.")

data = concat([data, age_info], axis=1)

column = "religion_info"
religion_info = json_normalize(data[column].tolist()).add_prefix(f"{column}.")

data = concat([data, religion_info], axis=1)


data.drop(columns=['area_info','age_info','religion_info'], inplace=True)

for name, group in data.groupby(['area_info.dimension_id']):
    temp = group.drop_duplicates()
    temp.to_csv('religion_data_{}.csv'.format(name))
    print(name, len(group))
