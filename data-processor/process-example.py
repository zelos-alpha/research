from .v2 import preprocess
from datetime import date, timedelta, datetime
import pandas as pd
import time


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


if __name__ == "__main__":
    start = date(2021, 12, 20)
    day = start

    while day < date(2022, 10, 19):
        start_time = time.time()
        df = pd.read_csv("./data/0x45dda9cb7c25131df268515131f647d726f50608-" + format_date(day) + ".csv")
        new_df = preprocess.preprocess_one(df)
        new_df.to_csv("./data_processed/0x45dda9cb7c25131df268515131f647d726f50608-" + format_date(day) + ".csv",
                      index=False)
        print(format_date(day), time.time() - start_time, "s")
        day = day + timedelta(days=1)
