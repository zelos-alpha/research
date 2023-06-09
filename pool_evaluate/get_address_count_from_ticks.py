import pandas as pd
from datetime import date, datetime, timedelta
import os
import time


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


def append_addr(l: pd.Series, attach: pd.Series):
    l = pd.concat([l, attach])
    return pd.Series(l.unique())


if __name__ == "__main__":
    start = date(2021, 12, 20)
    day = start
    total_df = None
    start_time = datetime.now()
    senders = pd.Series()
    while day <= date(2023, 5, 30):
        day_str = format_date(day)
        start_time = time.time()

        file = os.path.join("/data/research_data/uni/polygon/usdc_weth-updated", f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv")

        # if day != date(2023, 5, 21):
        #     day = day + timedelta(days=1)
        #     continue

        df: pd.DataFrame = pd.read_csv(file, engine="pyarrow", dtype_backend="pyarrow")
        df = df[df["tx_type"] != "SWAP"]
        senders = append_addr(senders, df["sender"])
        print(day_str, time.time() - start_time)
        # print(senders)
        day = day + timedelta(days=1)
    print(len(senders.index))
