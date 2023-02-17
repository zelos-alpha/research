from datetime import date, timedelta

from toys.toy_common import format_date
import pandas as pd
from demeter.broker.helper import tick_to_quote_price

folder = "/home/sun/AA-labs-FE/05_op_reward_phase2/data_minute/"
to_folder = "/home/sun/AA-labs-FE/05_op_reward_phase2/data-aid/"
"""

get op price list from op-usdc pool

"""
if __name__ == "__main__":
    contract = "0x1d751bc1a723accf1942122ca9aa82d49d08d2ae"
    start = date(2023, 1, 4)
    day = start
    total_df = None
    while day < date(2023, 2, 9):
        """Optimism-0x1c3140ab59d6caf9fa7459c6f83d4b52ba881d36-2023-01-08.csv"""
        df = pd.read_csv(f"{folder}Optimism-{contract}-{format_date(day)}.csv")
        if total_df is None:
            total_df = df
        else:
            total_df = pd.concat([total_df, df])
        print(day)
        day = day + timedelta(days=1)

    total_df["price"] = total_df.apply(lambda row: tick_to_quote_price(row.closeTick, 18, 6, False), axis=1)
    total_df["timestamp"] = pd.to_datetime(total_df["timestamp"])
    total_df.set_index("timestamp", inplace=True)
    prices_df: pd.DataFrame = total_df.loc[:, ["price"]]
    prices_df.to_csv(f"{to_folder}op_price.csv", index=True)
    print(prices_df)
