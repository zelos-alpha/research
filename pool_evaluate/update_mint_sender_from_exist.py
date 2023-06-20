import pandas as pd
from datetime import date, datetime, timedelta
import os
import time
from tqdm import tqdm

"""
"""


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


if __name__ == "__main__":
    start = date(2021, 12, 20)
    day = start

    while day <= date(2023, 5, 30):
        day_str = format_date(day)
        start_time = time.time()

        file = os.path.join(
            "/home/sun/data/polygon/usdc_weth_1",
            f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv",
        )

        # if day != date(2022, 1, 28):
        #     day = day + timedelta(days=1)
        #     continue

        uni_tx = pd.read_csv(file)
        modified_tx = pd.read_csv(
            os.path.join(
                "/home/sun/data/polygon/usdc_weth-updated",
                f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv",
            )
        )
        mint_tx = uni_tx[uni_tx["tx_type"] == "MINT"]
        mint_tx = mint_tx[~pd.isnull(mint_tx["position_id"])]

        for index, tx in mint_tx.iterrows():
            found = modified_tx[
                (modified_tx["transaction_hash"] == tx["transaction_hash"])
                & (modified_tx["pool_log_index"] == tx["pool_log_index"])
            ]
            if len(found.index)<1:
                raise RuntimeError(f'{ tx["transaction_hash"]}')
            uni_tx.loc[index, "receipt"] = found.iloc[0]["receipt"]
            uni_tx.loc[index, "sender"] = found.iloc[0]["sender"]
        file_to = os.path.join(
            "/home/sun/data/polygon/usdc_weth_1",
            f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv",
        )
        uni_tx.to_csv(file_to, index=False)
        print(day_str)
        day = day + timedelta(days=1)
