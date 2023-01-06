import time
from datetime import date, timedelta

import pandas as pd

import v2.preprocess as preprocess


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


folder = "../data/"
to_folder = "../data_tick/"
# contract = "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31"
# contract = "0x85149247691df622eaf1a8bd0cafd40bc45154a9"
# contract = "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6"
contract = "0x45dda9cb7c25131df268515131f647d726f50608"
# contract = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
if __name__ == "__main__":

    start = date(2022, 12, 13)
    day = start
    while day < date(2023, 1, 1):
        start_time = time.time()
        df = pd.read_csv(f"{folder}{contract}-{format_date(day)}.csv")
        new_df = preprocess.preprocess_one(df)
        new_df.to_csv(f"{to_folder}{contract}-{format_date(day)}.csv", index=False)
        print(format_date(day), time.time() - start_time, "s")
        day = day + timedelta(days=1)
