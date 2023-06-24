import pandas as pd
import os.path
from _decimal import Decimal
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import List, Tuple, Dict
from utils import LivePosition, format_date, config
from tqdm import tqdm
import time
import sys
import pickle

"""
step 3.3: 计算swap的所有current_tick的范围, 193013.0 208585.0
"""


if __name__ == "__main__":
    start = date(2021, 12, 21)
    end = date(2023, 5, 30)
    day = start
    min_tick = 9999999999
    max_tick = -999999999
    with tqdm(total=(end - start).days, ncols=100) as pbar:
        while day <= end:
            day_str = format_date(day)

            file = os.path.join(
                config["path"],
                f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv",
            )

            df_tx_day: pd.DataFrame = pd.read_csv(file)

            df_tx_day = df_tx_day[df_tx_day["tx_type"] == "SWAP"]

            day_min = df_tx_day["current_tick"].min()
            day_max = df_tx_day["current_tick"].max()
            if day_min < min_tick:
                min_tick = day_min

            if day_max > max_tick:
                max_tick = day_max
            day += timedelta(days=1)

            pbar.update()
    print(min_tick, max_tick)
    pass
