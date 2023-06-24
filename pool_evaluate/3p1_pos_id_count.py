import pandas as pd
import os.path
from _decimal import Decimal
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import NamedTuple, List, Tuple, Dict
from utils import format_date, config

"""
step 3.1: (准备工作)统计position的数量
"""


def append_addr(l: pd.Series, attach: pd.Series):
    l = pd.concat([l, attach])
    return pd.Series(l.unique())


if __name__ == "__main__":
    start = date(2021, 12, 20)
    day = start
    total_df = None
    senders = pd.Series()

    while day <= date(2023, 5, 30):
        day_str = format_date(day)

        file = os.path.join(
            config["path"],
            f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv",
        )

        # if day != date(2023, 5, 21):
        #     day = day + timedelta(days=1)
        #     continue

        df: pd.DataFrame = pd.read_csv(file, engine="pyarrow", dtype_backend="pyarrow")
        df = df[df["tx_type"] != "SWAP"]
        senders = append_addr(senders, df["position_id"])
        day = day + timedelta(days=1)
        print(day_str)
    print(len(senders.index))

pass
