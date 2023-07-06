from ukcensus.CensusData import RateLimitedAPI

if __name__ == "__main__":
    api = RateLimitedAPI()

    # population_types = api.get_population_types()


    # print(population_types)
    print(api.get_area_types("dsf"))
