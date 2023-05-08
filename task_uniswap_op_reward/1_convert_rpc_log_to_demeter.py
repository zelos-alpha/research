import json
from datetime import date, timedelta

import pandas as pd
from toys.toy_common import format_date
import sys


import demeter.download.process as processor

folder = "/home/sun/AA-labs-FE/05_op_reward_phase2/data/"
to_folder = "/home/sun/AA-labs-FE/05_op_reward_phase2/data_minute/"
chain = "Optimism"


def format_df(df: pd.DataFrame):
    df["timestamp"] = df.timestamp.apply(lambda x: x.replace("T", " "))
    df = df.rename(columns={
        "timestamp": "block_timestamp",
        "tx_hash": "transaction_hash",
        "tx_index": "transaction_index",
        "log_index": "log_index",
        "data": "DATA",
        "topics": "topics",
    })

    return df


if __name__ == "__main__":

    contract = "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31"
    start = date(2023, 2, 8)
    day = start
    while day < date(2023, 2, 9):
        logs = pd.read_csv(f"{folder}{contract}-{format_date(day)}.csv")
        logs = format_df(logs)
        processed: pd.DataFrame = processor.process_raw_data(logs)
        processed.to_csv(f"{to_folder}{chain}-{contract}-{format_date(day)}.csv", index=False)
        print(day)
        day = day + timedelta(days=1)
