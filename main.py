from ukcensus.CensusData import RateLimitedAPI

if __name__ == "__main__":
    api = RateLimitedAPI()

    population_types = api.get_population_types(return_type="pl_df")

    print(population_types)