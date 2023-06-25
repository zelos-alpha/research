import pandas as pd
from datetime import date, datetime, timedelta
import os
import time
from tqdm import tqdm

from utils import to_decimal, config, format_date

"""
step2.通过swap交易 获取价格序列
"""


def x96_sqrt_to_decimal(sqrt_priceX96):
    price = int(sqrt_priceX96) / 2 ** 96
    tmp = (price ** 2) * (10 ** (config["decimal0"] - config["decimal1"]))
    return 1 / tmp if config["is_0_base"] else tmp


def datetime2timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


if __name__ == "__main__":
    start = date(2021, 12, 20)
    day = start
    total_df = None
    start_time = datetime.now()
    count = 0
    end = date(2023, 6, 20)
    price_df = pd.DataFrame()
    with tqdm(total=(end - start).days + 1, ncols=120) as pbar:
        while day <= end:
            day_str = format_date(day)
            file = os.path.join(config["path"], f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv")

            # if day != date(2023, 5, 21):
            #     day = day + timedelta(days=1)
            #     continue

            df: pd.DataFrame = pd.read_csv(file, parse_dates=['block_timestamp'], converters={
                "total_liquidity": to_decimal,
                "sqrtPriceX96": to_decimal,
            })
            df = df[df["tx_type"] == "SWAP"]
            count += len(df.index)
            df["price"] = df["sqrtPriceX96"].apply(x96_sqrt_to_decimal)

            day = day + timedelta(days=1)
            tmp_price_df: pd.DataFrame = df[["block_timestamp", "price", "total_liquidity","sqrtPriceX96"]].copy()
            # tmp_price_df["timestamp"] = tmp_price_df["block_timestamp"].apply(datetime2timestamp)
            tmp_price_df = tmp_price_df.drop_duplicates(subset="block_timestamp")

            price_df = pd.concat([price_df, tmp_price_df])
            pbar.update()

    price_df: pd.DataFrame = price_df.set_index("block_timestamp")
    print("resampling, please wait")
    price_df = price_df.resample("1T").first().ffill()
    print("save file")
    price_df.to_csv(os.path.join(config["save_path"], "price.csv"), index=True)
    print(count)
