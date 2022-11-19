import time
from datetime import date, timedelta

import pandas as pd

import v2.preprocess as preprocess


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


if __name__ == "__main__":
    start = date(2022, 10, 10)
    day = start

    while day < date(2022, 11, 15):
        start_time = time.time()
        df = pd.read_csv("../data-op-bq/0x03af20bdaaffb4cc0a521796a223f7d85e2aac31-" + format_date(day) + ".csv")
        new_df = preprocess.preprocess_one(df)
        new_df.to_csv("../data-op-bq-tick/0x03af20bdaaffb4cc0a521796a223f7d85e2aac31-" + format_date(day) + ".csv",
                      index=False)
        print(format_date(day), time.time() - start_time, "s")
        day = day + timedelta(days=1)
