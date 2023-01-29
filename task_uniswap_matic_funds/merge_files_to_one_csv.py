import math
from datetime import date, timedelta, datetime
from decimal import Decimal

import pandas as pd
from toys.toy_common import format_date


def decimal_to_x96(number: Decimal):
    return int(Decimal(number) * 2 ** 96)


def tick_to_sqrt_price_x96(ticker) -> int:
    ticker = int(ticker)
    return int(decimal_to_x96(Decimal((Decimal.sqrt(Decimal(1.0001))) ** ticker)))


def _x96_to_decimal(number: int):
    return Decimal(number) / 2 ** 96


def tick_to_quote_price(tick: int, token_0_decimal, token_1_decimal, is_token0_base: bool):
    if math.isnan(tick):
        return math.nan
    sqrt_price = tick_to_sqrt_price_x96(tick)
    decimal_price = _x96_to_decimal(sqrt_price) ** 2
    pool_price = decimal_price * Decimal(10 ** (token_0_decimal - token_1_decimal))
    return Decimal(1 / pool_price) if is_token0_base else pool_price


def decimal_from_value(value):
    return Decimal(value) if value != "" else Decimal(0)


folder = "../data_tick/"

if __name__ == "__main__":
    start = date(2021, 12, 21)
    day = start
    total_df = None
    start_time = datetime.now()
    while day < date(2023, 1, 1):
        df = pd.read_csv(f"{folder}0x45dda9cb7c25131df268515131f647d726f50608-{format_date(day)}.csv",
                         converters={'sqrtPriceX96': decimal_from_value, 'liquidity': decimal_from_value})
        if total_df is None:
            total_df = df
        else:
            total_df = pd.concat([total_df, df])
        print(day)
        day = day + timedelta(days=1)
    total_df: pd.DataFrame = total_df.reset_index()
    total_df = total_df.sort_values(["block_number"])
    # fill the blank fields (tick, price, sqrtPriceX96), when a day start, there might be no swap in first minutes. 
    last_value = {
        "tick": 0,
        "sqrt_price": 0
    }
    for index, row in total_df.iterrows():
        if math.isnan(row.current_tick):
            total_df.loc[index, "sqrtPriceX96"] = last_value["sqrt_price"]
            total_df.loc[index, "current_tick"] = last_value["tick"]
        last_value = {
            "tick": total_df.loc[index, "current_tick"],
            "sqrt_price": total_df.loc[index, "sqrtPriceX96"]
        }

    total_df = total_df.drop(total_df.loc[total_df.tx_type == "SWAP"].index)  # !!! remove swap

    # total_df = pd.read_pickle(f"{folder}merge1.pkl")

    total_df["price"] = total_df.apply(lambda row: tick_to_quote_price(row["current_tick"], 6, 18, True), axis=1)
    order = ["position_id", "block_number", "block_timestamp", "tx_type", "transaction_hash", "pool_log_index",
             "proxy_log_index", "amount0", "amount1", "sqrtPriceX96", "current_tick",
             "tick_lower", "tick_upper", "liquidity", "sender", "receipt", "price"]
    total_df = total_df[order]
    total_df = total_df.sort_values(["position_id", "block_number", "pool_log_index"])
    total_df["key"] = total_df.apply(lambda x: x.transaction_hash + "-" + str(x.pool_log_index), axis=1)

    total_df.to_csv(f"{folder}merge1.csv", index=False, header=True)
    total_df.to_pickle(f"{folder}merge1.pkl")
