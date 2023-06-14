import pandas as pd
from datetime import date, datetime, timedelta
import os
import time
from tqdm import tqdm

"""
step2.通过swap交易 获取价格序列
"""


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


def x96_sqrt_to_decimal(sqrt_priceX96):
    price = int(sqrt_priceX96) / 2 ** 96
    tmp = (price ** 2) / 10 ** (config["decimal0"] - config["decimal1"])
    return tmp if config["is_0_base"] else 1 / tmp


def datetime2timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


config = {
    "is_0_base": True,
    "decimal0": 6,
    "decimal1": 18
}

if __name__ == "__main__":
    start = date(2021, 12, 20)
    day = start
    total_df = None
    start_time = datetime.now()
    count = 0
    end = date(2023, 5, 30)
    price_df = pd.DataFrame()
    with tqdm(total=(end - start).days + 1, ncols=150) as pbar:
        while day <= date(2023, 5, 30):
            day_str = format_date(day)
            file = os.path.join("/data/research_data/uni/polygon/usdc_weth-updated", f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv")

            # if day != date(2023, 5, 21):
            #     day = day + timedelta(days=1)
            #     continue

            df: pd.DataFrame = pd.read_csv(file, parse_dates=['block_timestamp'])
            df = df[df["tx_type"] == "SWAP"]
            count += len(df.index)
            df["price"] = df["sqrtPriceX96"].apply(x96_sqrt_to_decimal)
            day = day + timedelta(days=1)
            tmp_price_df: pd.DataFrame = df[["block_timestamp", "price"]].copy()
            # tmp_price_df["timestamp"] = tmp_price_df["block_timestamp"].apply(datetime2timestamp)
            tmp_price_df = tmp_price_df.drop_duplicates(subset="block_timestamp")

            price_df = pd.concat([price_df, tmp_price_df])
            pbar.update()

    price_df: pd.DataFrame = price_df.set_index("block_timestamp")
    price_df = price_df.resample("1T").mean().ffill()
    price_df.to_csv(os.path.join("/data/research_data/uni/polygon/usdc_weth-updated", "price.csv"), index=True)
    print(count)
