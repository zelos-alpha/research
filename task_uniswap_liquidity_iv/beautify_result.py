import os.path
from datetime import date, datetime, timedelta
from decimal import Decimal

import pandas as pd
import math
import numpy as np

from task_uniswap_liquidity_iv.main import map_to_dataframe


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


if __name__ == "__main__":
    start = date(2022, 12, 20)
    day = start
    total_df = None
    start_time = datetime.now()
    while day <= date(2023, 3, 20):
        day_str = format_date(day)
        file = "./result/" + day_str + ".csv"
        # if day != date(2022, 12, 20):
        #     day = day + timedelta(days=1)
        #     continue

        df: pd.DataFrame = pd.read_csv(file)
        df = df.loc[df.iv > 0]
        ndf = df.groupby(pd.cut(df['iv'], np.arange(0, 1.1, 0.1))).sum()
        iv_range = np.arange(0, 1.1, 0.1)
        result = {}
        for i, iv in enumerate(iv_range):
            if i == 0:
                continue
            result[iv] = 0
        total_sum = 0
        for index, row in df.iterrows():
            for i, iv in enumerate(iv_range):
                if i == 0:
                    continue
                if row["iv"] > iv:
                    continue
                result[iv] += int(row["liquidity"])
                total_sum += int(row["liquidity"])
                break
        df_result = map_to_dataframe(result)
        df_result["liquidity"] = df_result["liquidity"] / total_sum * 100
        df_result.to_csv("./result_beautiy/" + day_str + ".csv", index=False)
        day = day + timedelta(days=1)
