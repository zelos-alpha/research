import pandas as pd
import os
from utils import LivePosition, format_date, config, to_decimal
from tqdm import tqdm
from multiprocessing import Pool

"""
9. 通过累乘每小时收益率, 得到当前时刻相对于起始时间的收益率
"""
def process_one(file):
    # if not file in ['223637.csv', '224135.csv', '227281.csv', '233152.csv']:
    #     return
    path = os.path.join(config["save_path"], f"position_fee", file)
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df["final_return_rate"] = 1
    val = 1
    for index, row in df.iterrows():
        val *= row["return_rate"]
        df.loc[index, "final_return_rate"] = val
    df.to_csv(path)


if __name__ == "__main__":
    files = os.listdir(os.path.join(config["save_path"], "position_fee"))
    with Pool(20) as p:
        res = list(tqdm(p.imap(process_one, files), ncols=120, total=len(files)))
