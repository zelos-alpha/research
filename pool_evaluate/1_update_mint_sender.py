import pandas as pd
from datetime import date, datetime, timedelta
import os
import time
from tqdm import tqdm

"""
step1: 在通过proxy交易的情况下, mint的sender, 只能看到proxy合约地址, 因此需要通过proxy的nfttransfer, 查找一下sender 
"""


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


if __name__ == "__main__":

    transfers = pd.read_csv("/data/research_data/uni/polygon/matic-proxy-transfer/joined.csv", engine="pyarrow", dtype_backend="pyarrow")

    start = date(2021, 12, 26)
    day = start

    while day <= date(2021, 12, 26):
        day_str = format_date(day)
        start_time = time.time()

        file = os.path.join("/data/research_data/uni/polygon/usdc_weth", f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv")

        # if day != date(2022, 1, 28):
        #     day = day + timedelta(days=1)
        #     continue

        uni_tx = pd.read_csv(file)
        mint_tx = uni_tx[uni_tx["tx_type"] == "MINT"]
        mint_tx = mint_tx[~ pd.isnull(mint_tx["position_id"])]

        with tqdm(total=len(mint_tx.index), ncols=150) as pbar:
            for index, tx in mint_tx.iterrows():
                rel_transfer = transfers[(transfers["position_id"] == int(tx["position_id"])) & (transfers["block_number"] <= tx["block_number"])]
                if len(rel_transfer.index) == 0:
                    raise RuntimeError(day_str + " no nft transfer found " + tx.transaction_hash)
                rel_transfer = rel_transfer.tail(1)
                uni_tx.loc[index, "receipt"] = uni_tx.loc[index, "sender"]
                uni_tx.loc[index, "sender"] = rel_transfer.iloc[0]["to"]
                pbar.update()
        file_to = os.path.join("/data/research_data/uni/polygon/usdc_weth-updated", f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv")
        uni_tx.to_csv(file_to, index=False)
        print(day_str)
        day = day + timedelta(days=1)
