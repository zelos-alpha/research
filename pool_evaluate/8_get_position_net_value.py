import os.path
import pickle
import time
from decimal import Decimal
from datetime import timedelta, date, datetime
from typing import List, Tuple, Dict
from multiprocessing import Pool

import numpy as np
import pandas as pd
from tqdm import tqdm
from demeter.uniswap.liquitidy_math import get_amounts

from utils import format_date, config, to_decimal, PositionLiquidity, get_pos_key, get_minute_time, get_time_index, get_hour_time


def load_fees() -> Dict[date, pd.DataFrame]:
    day = start
    day_fee_dfs = {}
    with tqdm(total=(end - start).days + 1, ncols=150) as pbar:
        while day <= end:
            day_str = format_date(day)
            fee_df = pd.read_csv(os.path.join(config["save_path"], f"fees/{day_str}.csv"), index_col=0, parse_dates=True)
            fee_df = fee_df.resample(freq).sum()
            day_fee_dfs[day] = fee_df
            day += timedelta(days=1)
            pbar.update()
    return day_fee_dfs


def set_in_range(v, min_v, max_v):
    if v < min_v:
        return min_v
    if v > max_v:
        return max_v
    return v


def get_hour_fee(t: datetime, lower_tick: int, upper_tick: int):
    d = t.date()
    fee_df: pd.DataFrame = day_fee_dfs[d]
    fee_rows = fee_df.loc[t]
    day_min_tick = tick_range_df.loc[d]["min_tick"]
    day_max_tick = tick_range_df.loc[d]["max_tick"]
    lower_tick = set_in_range(lower_tick, day_min_tick, day_max_tick)
    upper_tick = set_in_range(upper_tick, day_min_tick, day_max_tick)

    return fee_rows[int(lower_tick - day_min_tick):int(upper_tick - day_min_tick)].sum()


def get_lp_net_value(liquidity: int, lower_tick: int, upper_tick: int, price, sqrt_price_x96):
    amount0, amount1 = get_amounts(sqrt_price_x96, lower_tick, upper_tick, liquidity, config["decimal0"], config["decimal1"])
    amount0 = float(amount0)
    amount1 = float(amount1)
    return (amount0 + price * amount1) if config["is_0_base"] else (amount1 + price * amount0)


def write_empty_log(position_id):
    with open(os.path.join(config["save_path"], "empty_log.txt"), "a") as p:
        p.write(f"{position_id}")
        p.write("\n")


def process_one_position(param: Tuple[str, pd.DataFrame]):
    position_id, rel_actions = param
    mint_burn_actions = rel_actions[rel_actions["tx_type"] != "COLLECT"]
    if len(mint_burn_actions) <= 0:
        write_empty_log(position_id)
        return

    pos_start = get_hour_time(mint_burn_actions.head(1).iloc[0]["blk_time"])
    pos_end = get_hour_time(mint_burn_actions.tail(1).iloc[0]["blk_time"])

    sum_liquidity = mint_burn_actions["liquidity"].sum()
    if sum_liquidity > 100:
        pos_end = current_time
    time_serial = pd.date_range(pos_start, pos_end, freq=freq)
    position_df = pd.DataFrame(index=time_serial)
    position_df["total_net_value"] = 0  # 手续费+lp的净值, 相当于一个小时开头的净值(叠加过本小时手续费和流动性的)
    position_df["lp_net_value"] = 0  # lp的净值,
    position_df["current_fee"] = 0  # 这个小时叠加了多少手续费
    position_df["accumulation_fee"] = 0  # 这个小时为止, 累计的手续费是多少
    position_df["net_value_pre_hour"] = 0  # 一个小时的零点的净值: 之前手续费的累计+这个小时流动性净值
    position_df["net_value_post_hour"] = 0  # 一个小时最后时刻的净值: 本小时手续费的累计 + 这个小时流动性净值(用最后的价格计算, 也就是下一个小时开始的价格)
    current_liquidity = 0
    mint_burn_actions_idx = 0
    current_sum_fee = 0
    lower_tick = mint_burn_actions.iloc[mint_burn_actions_idx]["lower_tick"]
    upper_tick = mint_burn_actions.iloc[mint_burn_actions_idx]["upper_tick"]
    last_index = position_df.tail(1).index[0]
    for index, row in position_df.iterrows():
        next_index = index + timedelta(hours=1)
        if next_index > last_index:
            next_index = last_index
        while mint_burn_actions_idx < len(mint_burn_actions.index) and \
                index == get_hour_time(mint_burn_actions.iloc[mint_burn_actions_idx]["blk_time"]):
            current_liquidity += mint_burn_actions.iloc[mint_burn_actions_idx]["liquidity"]
            mint_burn_actions_idx += 1
        total_fee = get_hour_fee(index, lower_tick, upper_tick)

        position_df.loc[index, "current_fee"] = total_fee * current_liquidity / prices_df.loc[index]["total_liquidity"]
        position_df.loc[index, "lp_net_value"] = get_lp_net_value(
            current_liquidity,
            lower_tick,
            upper_tick,
            prices_df.loc[index]["price"],
            int(prices_df.loc[index]["sqrtPriceX96"])
        )
        position_df.loc[index, "net_value_pre_hour"] = position_df.loc[index, "lp_net_value"] + current_sum_fee

        # 开始叠加手续费
        current_sum_fee += position_df.loc[index, "current_fee"]
        position_df.loc[index, "accumulation_fee"] = current_sum_fee

        next_lp_net_value = get_lp_net_value(
            current_liquidity,
            lower_tick,
            upper_tick,
            prices_df.loc[next_index]["price"],
            int(prices_df.loc[next_index]["sqrtPriceX96"])
        )
        position_df.loc[index, "net_value_post_hour"] = next_lp_net_value + current_sum_fee

        position_df.loc[index, "total_net_value"] = position_df.loc[index, "accumulation_fee"] + position_df.loc[index, "lp_net_value"]
    position_df["return_rate"] = position_df["net_value_post_hour"] / position_df["net_value_pre_hour"]
    position_df.to_csv(os.path.join(config["save_path"], f"fee_result/{position_id}.csv"))


init_fee = False
freq = "1H"
multi_thread = True

if __name__ == "__main__":
    start_time = time.time()
    start = date(2021, 12, 21)
    end = date(2023, 6, 20)
    day = start
    print("loading fees")
    if init_fee:
        day_fee_dfs = load_fees()
        with open(os.path.join(config["save_path"], "all_daily_fee.pkl"), "wb") as f:
            pickle.dump(day_fee_dfs, f)
    else:
        with open(os.path.join(config["save_path"], "all_daily_fee.pkl"), "rb") as f:
            day_fee_dfs = pickle.load(f)
    tick_range_df = pd.read_csv(os.path.join(config["save_path"], "fees/swap_tick_range.csv"), parse_dates=["day_str"])
    tick_range_df["day_str"] = tick_range_df["day_str"].apply(lambda a: a.date())

    tick_range_df = tick_range_df.set_index(["day_str"])
    positions_df = pd.read_csv(os.path.join(config["save_path"], "position_liquidity.csv"),
                               dtype={"amount0": float, "amount1": float, "liquidity": float, "id": str},
                               parse_dates=["blk_time"])
    prices_df = pd.read_csv(os.path.join(config["save_path"], "price.csv"), parse_dates=["block_timestamp"])
    prices_df = prices_df.set_index(["block_timestamp"])
    prices_df = prices_df.resample(freq).first().ffill()
    print("start calc")
    ids = positions_df.groupby("id")
    current_time = datetime.combine(end + timedelta(days=1), datetime.min.time()) - timedelta(minutes=1)
    if multi_thread:
        ids = list(ids)
        with Pool(8) as p:
            res = list(tqdm(p.imap(process_one_position, ids), ncols=120, total=len(ids)))
    else:
        with tqdm(total=len(ids), ncols=150) as pbar:
            for position_id, rel_actions in ids:
                # if position_id != "0x1556ff873a3e0285b4615213386cd0ef67995139-202110-202510":
                #     pbar.update()
                #     continue
                process_one_position((str(position_id), rel_actions))
                pbar.update()
    print("exec time", time.time() - start_time)
