import os.path
from datetime import date, datetime, timedelta

import pandas as pd
import math


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


def fill_nan(a, b):
    if math.isnan(a):
        a = b
    elif math.isnan(b):
        b = a
    return a, b


def find_iv(df_iv: pd.DataFrame, lower_tick: int, upper_tick: int):
    """
    找到对于指定tick(lower, upper), 在df_iv中, 最接近的四个组合的平均iv
    """
    df_iv["upper_diff"] = df_iv["upper_tick"] - upper_tick
    df_iv["lower_diff"] = df_iv["lower_tick"] - lower_tick
    upper_max = df_iv["upper_diff"].loc[lambda x: x >= 0].min() + upper_tick
    upper_min = df_iv["upper_diff"].loc[lambda x: x <= 0].max() + upper_tick
    upper_max, upper_min = fill_nan(upper_max, upper_min)
    lower_max = df_iv["lower_diff"].loc[lambda x: x >= 0].min() + lower_tick
    lower_min = df_iv["lower_diff"].loc[lambda x: x <= 0].max() + lower_tick
    lower_max, lower_min = fill_nan(lower_max, lower_min)
    l = []

    try:
        if upper_max != lower_max:
            l.append(df_iv.loc[lambda x: (x.upper_tick == upper_max) & (x.lower_tick == lower_max)].iv.iloc[0])
        if upper_max != lower_min:
            l.append(df_iv.loc[lambda x: (x.upper_tick == upper_max) & (x.lower_tick == lower_min)].iv.iloc[0])
        if upper_min != lower_max:
            l.append(df_iv.loc[lambda x: (x.upper_tick == upper_min) & (x.lower_tick == lower_max)].iv.iloc[0])
        if upper_min != lower_min:
            l.append(df_iv.loc[lambda x: (x.upper_tick == upper_min) & (x.lower_tick == lower_min)].iv.iloc[0])

    except Exception as e:
        print(e)
    l = list(filter(lambda x: math.isnan(x) == False, l))
    val = sum(l) / len(l) if len(l) > 0 else 0
    if math.isnan(val):
        print(lower_tick, upper_tick)
    return val


def map_to_dataframe(map={}) -> pd.DataFrame:
    l = []
    for k, v in map.items():
        l.append((k, v))
    l.sort(key=lambda x: x[0])
    return pd.DataFrame(l, columns=["iv", "liquidity"])


if __name__ == "__main__":
    start = date(2022, 12, 20)
    day = start
    total_df = None
    start_time = datetime.now()
    while day <= date(2023, 3, 20):
        day_str = format_date(day)
        file = "/data/research_data/uni/polygon/usdc_weth/tick/0x45dda9cb7c25131df268515131f647d726f50608-" + day_str + ".csv"
        # if day != date(2022, 12, 20):
        #     day = day + timedelta(days=1)
        #     continue

        df_iv = pd.read_csv(f"./iv_data/{day_str}.csv")
        df = pd.read_csv(file)
        mints = df[df.tx_type == "MINT"]

        print(day, len(mints.index))
        iv_liquidity = {}
        for index, row in mints.iterrows():
            iv = find_iv(df_iv, row.tick_lower, row.tick_upper)
            if iv not in iv_liquidity:
                iv_liquidity[iv] = 0
            iv_liquidity[iv] += int(row.liquidity)
            # print(row.tick_lower, row.tick_upper, row.liquidity, iv)
        df_result = map_to_dataframe(iv_liquidity)
        df_result.to_csv(os.path.join("./result", day_str + ".csv"), index=False, header=True)
        day = day + timedelta(days=1)
