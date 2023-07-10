from itertools import product

def generate_subsets(set_list):
    print("generating subsets")
    temp =list(product(*set_list))
    return temp