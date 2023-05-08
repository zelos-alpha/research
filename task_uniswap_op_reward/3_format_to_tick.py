import time
from datetime import date, timedelta

import pandas as pd

import tool_process.v2.preprocess as preprocess


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


folder = "/home/sun/AA-labs-FE/05_op_reward_phase2/data-bq/"
to_folder = "/home/sun/AA-labs-FE/05_op_reward_phase2/data-tick/"
contract = "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31"

if __name__ == "__main__":
    start = date(2023, 1, 4)
    day = start
    while day < date(2023, 2, 9):
        start_time = time.time()
        df = pd.read_csv(f"{folder}{contract}-{format_date(day)}.csv")
        new_df = preprocess.preprocess_one(df)
        new_df.to_csv(f"{to_folder}{contract}-{format_date(day)}.csv", index=False)
        print(format_date(day), time.time() - start_time, "s")
        day = day + timedelta(days=1)
