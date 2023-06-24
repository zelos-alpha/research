import pickle

if __name__ == "__main__":
    file_path = "/home/sun/AA-labs-FE/14_uni_pool_evaluate/data/position_liquidity.pkl"
    with open(file_path, "rb") as f:
        content = pickle.load(f)

    pass
