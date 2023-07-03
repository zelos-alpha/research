from dataclasses import dataclass
import pandas as pd
from datetime import date, datetime, timedelta
import os
import time
from tqdm import tqdm
from utils import format_date, config, to_decimal, get_minute_time, get_time_index
import numpy as np
import math
from decimal import Decimal

"""
step 4 把swap交易的手续费, 分配到tick上. (核心)
原理: 根据swap. 把手续费分配到对应的tick上. 
"""


def get_tick_width(max_tick, min_tick):
    return int(max_tick - min_tick + 1)


def get_tick_index(tick, min_tick):
    return int(tick - min_tick)


def process_day_swap(df: pd.DataFrame, min_tick, max_tick):
    arr_nv = np.zeros((1440, get_tick_width(max_tick, min_tick)))
    for index, row in df.iterrows():
        time_index = get_time_index(row["block_timestamp"])
        price = price_df.loc[get_minute_time(row["block_timestamp"]), "price"]
        if row["amount0"] > 0:
            v0 = float(config["pool_fee_rate"] * row["amount0"] / 10 ** config["decimal0"])
            v1 = 0
        else:
            v0 = 0
            v1 = float(config["pool_fee_rate"] * row["amount1"] / 10 ** config["decimal1"])
        arr_nv[time_index][get_tick_index(row["current_tick"], min_tick)] += (v0 + v1 * price) \
            if config["is_0_base"] else (v1 + v0 * price)
    day = df.head(1).iloc[0]["block_timestamp"].date()
    time_serial = pd.date_range(datetime.combine(day, datetime.min.time()),
                                datetime.combine(day + timedelta(days=1), datetime.min.time()) - timedelta(minutes=1),
                                freq="1T")

    day_fee = pd.DataFrame(arr_nv, index=time_serial)
    day_fee.to_csv(os.path.join(config["save_path"], f"fees/{day_str}.csv"))


@dataclass
class tick_range:
    day_str: str
    min_tick: int
    max_tick: int


if __name__ == "__main__":
    start = date(2021, 12, 21)
    day = start
    total_df = None
    start_time = datetime.now()
    end = date(2023, 6, 20)
    day_length = ((end - start).days + 1) * 1440
    tick_range_list = []
    price_df = pd.read_csv("/home/sun/AA-labs-FE/14_uni_pool_evaluate/data/price.csv", parse_dates=["block_timestamp"])
    price_df = price_df.set_index(["block_timestamp"])
    with tqdm(total=(end - start).days + 1, ncols=100) as pbar:
        while day <= end:
            day_str = format_date(day)
            file = os.path.join(
                config["path"],
                f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv",
            )

            # if day != date(2022, 12, 22):
            #     day = day + timedelta(days=1)
            #     pbar.update()
            #     continue

            df: pd.DataFrame = pd.read_csv(
                file,
                parse_dates=["block_timestamp"],
                converters={
                    "amount0": to_decimal,
                    "amount1": to_decimal,
                    "total_liquidity": to_decimal,
                    "liquidity": to_decimal,
                },
            )
            df = df[df["tx_type"] == "SWAP"]
            max_tick = df["current_tick"].max()
            min_tick = df["current_tick"].min()
            process_day_swap(df, min_tick, max_tick)
            tick_range_list.append(tick_range(day_str, min_tick, max_tick))
            day = day + timedelta(days=1)
            pbar.update()

    range_df = pd.DataFrame(tick_range_list)
    range_df.to_csv(
        os.path.join(config["save_path"], "fees/swap_tick_range.csv"), index=False
    )
