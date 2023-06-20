from utils import config, LivePosition
import os
import pickle

file_name = "positon_liuqudity.pkl"

if __name__ == "__main__":
    path = os.path.join(config["save_path"], file_name)
    with open(path, "rb") as f:
        pos_list = pickle.load(f)

    min_tick = 9999999999
    max_tick = -999999999999
    for tick_range in pos_list.keys():
        if tick_range[0] < min_tick:
            min_tick = tick_range[0]
        if tick_range[1] > max_tick:
            max_tick = tick_range[1]
    print(min_tick, max_tick)
