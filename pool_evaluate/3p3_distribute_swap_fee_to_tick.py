from dataclasses import dataclass
import pandas as pd
from datetime import date, datetime, timedelta
import os
import time
from tqdm import tqdm
from utils import format_date, config, to_decimal
import numpy as np
import math
from decimal import Decimal

"""
step 3.3 把swap交易的手续费, 分配到tick上. (核心)
"""


def get_time_index(t: datetime) -> int:
    return t.hour * 60 + t.minute


def get_tick_width(max_tick, min_tick):
    return int(max_tick - min_tick + 1)


def get_tick_index(tick, min_tick):
    return int(tick - min_tick)


def process_day_swap(df: pd.DataFrame, min_tick, max_tick):
    arr0 = np.zeros((1440, get_tick_width(max_tick, min_tick)))
    arr1 = np.zeros((1440, get_tick_width(max_tick, min_tick)))

    for index, row in df.iterrows():
        time_index = get_time_index(row["block_timestamp"])
        if row["amount0"] > 0:
            arr0[time_index][get_tick_index(row["current_tick"], min_tick)] += float(
                config["pool_fee_rate"] * row["amount0"]
            )
        else:
            arr1[time_index][get_tick_index(row["current_tick"], min_tick)] += float(
                config["pool_fee_rate"] * row["amount1"]
            )
    day_fee_0 = pd.DataFrame(arr0)
    day_fee_1 = pd.DataFrame(arr1)
    day_fee_0.to_csv(
        os.path.join(config["save_path"], f"fees/{day_str}_0.csv"), index=False
    )
    day_fee_1.to_csv(
        os.path.join(config["save_path"], f"fees/{day_str}_1.csv"), index=False
    )



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
    end = date(2023, 5, 30)
    price_df = pd.DataFrame()
    day_length = ((end - start).days + 1) * 1440
    tick_range_list = []
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
