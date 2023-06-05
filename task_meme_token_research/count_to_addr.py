import random
from enum import Enum
from typing import NamedTuple, List
from tqdm import tqdm
import requests

import pandas as pd

pd.options.display.max_columns = None
pd.options.display.max_rows = 99999
pd.set_option('display.width', 5000)
pd.options.display.max_colwidth = 100

if __name__ == "__main__":
    path = "/home/sun/AA-labs-FE/12_meme_research/single_swap_tx.csv"
    print("loading transfer")
    transfers = pd.read_csv(path)
    print("loading tx")
    full_tx_df = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/token_0x7a58c0be72be218b41c608b7fe7c5bb630736c71.tx.csv")
    transfers["idx"] = transfers.apply(lambda x: str(x["blockNumber"]) + "_" + str(x["transactionIndex"]), axis=1)
    full_tx_df["idx"] = full_tx_df.apply(lambda x: str(x["blockNumber"]) + "_" + str(x["transactionIndex"]), axis=1)
    full_tx_df = full_tx_df[full_tx_df["idx"].isin(transfers["idx"])]
    to_count = full_tx_df.groupby("to")["idx"] \
        .count() \
        .reset_index(name='count').sort_values(['count'], ascending=False)
    print(to_count)
