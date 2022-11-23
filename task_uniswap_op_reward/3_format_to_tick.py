import time
from datetime import date, timedelta

import pandas as pd

import tool_process.v2.preprocess as preprocess


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


folder = "../data-op-bq/"
to_folder = "../data-op-bq-tick/"
contract = "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31"
# contract = "0x85149247691df622eaf1a8bd0cafd40bc45154a9"
# contract = "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6"
if __name__ == "__main__":
    start = date(2022, 10, 26)
    day = start
    while day < date(2022, 10, 27):
        start_time = time.time()
        df = pd.read_csv(f"{folder}{contract}-{format_date(day)}.csv")
        new_df = preprocess.preprocess_one(df)
        new_df.to_csv(f"{to_folder}{contract}-{format_date(day)}.csv", index=False)
        print(format_date(day), time.time() - start_time, "s")
        day = day + timedelta(days=1)
