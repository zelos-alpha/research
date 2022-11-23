import json
from datetime import date, timedelta

import pandas as pd
from toys.toy_common import format_date
import sys


import demeter.download.source_bigquery as processor

folder = "../../data-op/"
to_folder = "../../data-op-minute/"
chain = "optimism"


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
    # contract = "0x85149247691df622eaf1a8bd0cafd40bc45154a9"
    # contract = "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6"
    start = date(2022, 10, 10)
    day = start
    while day < date(2022, 11, 15):
        logs = pd.read_csv(f"{folder}{contract}-{format_date(day)}.csv")
        logs = format_df(logs)
        processed: pd.DataFrame = processor.process_raw_data(logs)
        processed.to_csv(f"{to_folder}{chain}-{contract}-{format_date(day)}.csv", index=False)
        print(day)
        day = day + timedelta(days=1)
