from ukcensus.CensusData import RateLimitedAPI

if __name__ == "__main__":
    api = RateLimitedAPI()

    population_types = api.get_population_types()
    for pop in population_types:
        if pop['type'] == 'microdata':
            print(pop['name'], pop['label'])


    print("which population type do you want to query for please enter name")
    population_type = str(input())

    # api.get_dimensions("age",population_type=population_type)
    api.get_dimensions("religion",population_type=population_type)


    api.get_area_types(population_type=population_type)
    api.get_area_infos(population_type=population_type)


    print("how many dimensions do you want to query")
    num_dimensions = int(input())
    dimensions = []
    dimensions_list = []

    for i in range(num_dimensions):
        print("enter dimension filter")
        dimensions.append(input())
        dimensions_list.append( api.get_filtered_dimension(return_type="list",population_type=population_type,_filter='_{}_'.format(dimensions[i])))
    print(dimensions_list)
    print(api.get_multi_final_data(population_type=population_type,dimension=dimensions_list,how='any',n=num_dimensions))

    # print(api.get_data_final())
    print(api.get_categories())
