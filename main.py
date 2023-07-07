from ukcensus.CensusData import RateLimitedAPI

if __name__ == "__main__":
    api = RateLimitedAPI()

    population_types = api.get_population_types()
    print(api.get_area_types())
    print(api.get_area_infos())
    print(api.get_dimensions("religion"))
    print(api.get_data_final())
    print(api.get_categories())
