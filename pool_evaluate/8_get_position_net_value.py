import os.path
import pickle
import time
from datetime import timedelta, date, datetime
from multiprocessing import Pool
from typing import Tuple, Dict

import pandas as pd
from demeter.uniswap.liquitidy_math import get_amounts
from tqdm import tqdm

from utils import format_date, config, get_hour_time, get_value_in_base_token

"""
8. 获取position的净值. (重要)

算法: 
"""


def load_fees(fee_type: str = "0") -> Dict[date, pd.DataFrame]:
    day = start
    day_fee_df = {}
    with tqdm(total=(end - start).days + 1, ncols=150) as pbar:
        while day <= end:
            day_str = format_date(day)
            fee_df = pd.read_csv(os.path.join(config["save_path"], f"tick_fee/{day_str}_{fee_type}.csv"), index_col=0, parse_dates=True)
            fee_df = fee_df.resample(freq).sum()
            day_fee_df[day] = fee_df
            day += timedelta(days=1)
            pbar.update()
    return day_fee_df


def limit_value(v, min_v, max_v):
    if v < min_v:
        return min_v
    if v > max_v:
        return max_v
    return v


def get_hour_fee(t: datetime, lower_tick: int, upper_tick: int):
    d = t.date()
    if d not in day_fee_df_0:
        return 0

    day_min_tick = tick_range_df.loc[d]["min_tick"]
    day_max_tick = tick_range_df.loc[d]["max_tick"]
    lower_tick = limit_value(lower_tick, day_min_tick, day_max_tick)
    upper_tick = limit_value(upper_tick, day_min_tick, day_max_tick)

    fee_rows_0 = day_fee_df_0[d].loc[t]
    fee_rows_1 = day_fee_df_1[d].loc[t]

    lower_index = int(lower_tick - day_min_tick)
    upper_index = int(upper_tick - day_min_tick)
    return fee_rows_0[lower_index:upper_index].sum(), fee_rows_1[lower_index:upper_index].sum()


def get_lp_net_value(liquidity: int, lower_tick: int, upper_tick: int, price, sqrt_price_x96):
    amount0, amount1 = get_amounts(sqrt_price_x96, lower_tick, upper_tick, liquidity, config["decimal0"], config["decimal1"])
    amount0 = float(amount0)
    amount1 = float(amount1)
    return (amount0 + price * amount1) if config["is_0_base"] else (amount1 + price * amount0)


def get_lp_values(liquidity: int, lower_tick: int, upper_tick: int, sqrt_price_x96) -> (float, float):
    amount0, amount1 = get_amounts(sqrt_price_x96, lower_tick, upper_tick, liquidity, config["decimal0"], config["decimal1"])
    return float(amount0), float(amount1)


def write_empty_log(position_id):
    with open(os.path.join(config["save_path"], "empty_log.txt"), "a") as p:
        p.write(f"{position_id}")
        p.write("\n")


def process_one_position(param: Tuple[str, pd.DataFrame]):
    position_id, rel_actions = param
    if os.path.exists(os.path.join(config["save_path"], f"position_fee/{position_id}.csv")):
        return
    # if position_id not in ['223637', '224135', '227281', '233152']:
    #     return
    rel_actions["hour_time"] = rel_actions.apply(lambda x: get_hour_time(x["blk_time"]), axis=1)
    # mint_burn_actions = rel_actions[rel_actions["tx_type"] != "COLLECT"]
    useful_actions = rel_actions[~((rel_actions["tx_type"] == "COLLECT") &
                                   (rel_actions["final_amount0"] == 0) &
                                   (rel_actions["final_amount1"] == 0))]
    if len(useful_actions.index) <= 0:
        # 通常是amount==0引起的
        write_empty_log(position_id)
        return

    pos_start = get_hour_time(useful_actions.head(1).iloc[0]["blk_time"])
    pos_end = get_hour_time(useful_actions.tail(1).iloc[0]["blk_time"])

    sum_liquidity = useful_actions["liquidity"].sum()
    if sum_liquidity > 100:
        pos_end = current_time
    time_serial = pd.date_range(pos_start, pos_end, freq=freq)
    position_df = pd.DataFrame(index=time_serial)
    position_df["lp_net_value_begin"] = 0  # 当前小时开始时刻lp的净值, 计算回报率用, f(Liq-begin,Price-start)
    position_df["lp_net_value_end"] = 0  # 当前小时结束时刻lp的净值, 计算回报率用, f(Liq-begin,Price-end)
    position_df["lp_net_value_this_hour"] = 0  # 当前小时LP的净值, 计算总净值用. f(Liq-end,Price-end)
    position_df["current_fee0"] = 0  # 这个小时新增了多少token0手续费
    position_df["current_fee1"] = 0  # 这个小时新增了多少token1手续费
    position_df["liquidity"] = 0  # 当前小时 经过操盘之后的流动性

    current_liquidity = 0
    last_collect_index_in_user_action = 0
    last_collect_index_in_position_df = position_df.head(1).index[0] - timedelta(seconds=1)
    lower_tick = useful_actions.iloc[0]["lower_tick"]
    upper_tick = useful_actions.iloc[0]["upper_tick"]
    last_index = position_df.tail(1).index[0]
    collect_index_list = []
    # 计算lp净值, 并记录这个小时分配的手续费
    # 根据collect的操作中amount0和amount1的值, 将预估手续费和实际手续费的差值, 均摊到之前的手续费收入中
    for index, row in position_df.iterrows():
        next_index = index + timedelta(hours=1)  # 后一条数据的索引, 用于取后1小时的价格. 作为这个小时的结束价格.
        if next_index > last_index:
            next_index = last_index
        if index not in prices_df.index:
            continue
        begin_price = prices_df.loc[index]["price"]
        end_price = prices_df.loc[next_index]["price"]
        begin_price_x96 = int(prices_df.loc[index]["sqrtPriceX96"])
        end_price_x96 = int(prices_df.loc[next_index]["sqrtPriceX96"])
        position_df.loc[index, "lp_net_value_begin"] = get_lp_net_value(
            current_liquidity, lower_tick, upper_tick, begin_price, begin_price_x96)
        position_df.loc[index, "lp_net_value_end"] = get_lp_net_value(
            current_liquidity, lower_tick, upper_tick, end_price, end_price_x96)
        # 根据当前小时的操盘, 增减流动性
        current_liquidity += useful_actions[useful_actions["hour_time"] == index].liquidity.sum()

        position_df.loc[index, "liquidity"] = current_liquidity
        position_df.loc[index, "lp_net_value_this_hour"] = get_lp_net_value(
            current_liquidity, lower_tick, upper_tick, end_price, end_price_x96)

        fee0_in_this_range, fee1_in_this_range = get_hour_fee(index, lower_tick, upper_tick)

        position_df.loc[index, "current_fee0"] = fee0_in_this_range * current_liquidity / prices_df.loc[index]["total_liquidity"]
        position_df.loc[index, "current_fee1"] = fee1_in_this_range * current_liquidity / prices_df.loc[index]["total_liquidity"]

        hour_collect_actions = rel_actions[(rel_actions["tx_type"] == "COLLECT") & (rel_actions["hour_time"] == index)]

        # 如果有多次collect, 则计算手续费收益的差值, 然后均摊到之前的小时上
        if len(hour_collect_actions.index) > 0:  # 可能一个小时内有多次collect
            actual_fee0_sum = 0
            actual_fee1_sum = 0
            for collect_index, collect_row in hour_collect_actions.iterrows():
                # 获取从上次collect到现在, 手续费收入是多少 (注意: collect不是手续费, 而是手续费和质押资金的和, 所以要先把burn的金额剥离出去)
                #       先获取从上次collect到现在, 所有burn的流动性
                rel_burn = useful_actions[(useful_actions.index < collect_index) &
                                          (useful_actions.index > last_collect_index_in_user_action) &
                                          (useful_actions.tx_type == "BURN")]

                burned_amount0 = rel_burn["final_amount0"].sum()
                burned_amount1 = rel_burn["final_amount1"].sum()
                #       得到手续费
                actual_fee0_sum += collect_row["final_amount0"] - burned_amount0
                actual_fee1_sum += collect_row["final_amount1"] - burned_amount1

                last_collect_index_in_user_action = collect_index

            # 将手续费的差值均摊
            #    计算和之前的差值
            hour_since_last_collect = position_df[(position_df.index > last_collect_index_in_position_df) & (position_df.index <= index)]
            calc_fee0 = hour_since_last_collect.current_fee0.sum()
            calc_fee1 = hour_since_last_collect.current_fee1.sum()
            fee_diff0 = actual_fee0_sum / 10 ** config["decimal0"] - calc_fee0
            fee_diff1 = actual_fee1_sum / 10 ** config["decimal1"] - calc_fee1
            position_df.loc[hour_since_last_collect.index, "current_fee0"] += fee_diff0 / len(hour_since_last_collect.index)
            position_df.loc[hour_since_last_collect.index, "current_fee1"] += fee_diff1 / len(hour_since_last_collect.index)
            collect_index_list.append(index)
            last_collect_index_in_position_df = index
    # 手续费和lp的净值准备完毕, 开始计算总净值
    position_df["total_net_value"] = 0  # 手续费+lp的净值, 相当于一个小时开头的净值(叠加过本小时手续费和流动性的)
    position_df["accumulation_fee0"] = 0  # 这个小时为止, 累计的手续费是多少
    position_df["accumulation_fee1"] = 0  # 这个小时为止, 累计的手续费是多少
    position_df["net_value_begin"] = 0  # 一个小时的零点的净值: 之前手续费的累计+这个小时流动性净值
    position_df["net_value_end"] = 0  # 一个小时最后时刻的净值: 本小时手续费的累计 + 这个小时流动性净值(用最后的价格计算, 也就是下一个小时开始的价格)

    current_sum_fee0 = 0
    current_sum_fee1 = 0

    for index, row in position_df.iterrows():
        next_index = index + timedelta(hours=1)
        if next_index > last_index:
            next_index = last_index
        begin_price = prices_df.loc[index]["price"]
        end_price = prices_df.loc[next_index]["price"]

        current_sum_fee0 += row["current_fee0"]
        current_sum_fee1 += row["current_fee1"]
        position_df.loc[index, "accumulation_fee0"] = current_sum_fee0
        position_df.loc[index, "accumulation_fee1"] = current_sum_fee1

        begin_fee = get_value_in_base_token(current_sum_fee0, current_sum_fee1, begin_price)
        end_fee = get_value_in_base_token(current_sum_fee0, current_sum_fee1, end_price)

        position_df.loc[index, "net_value_begin"] = position_df.loc[index, "lp_net_value_begin"] + begin_fee
        position_df.loc[index, "net_value_end"] = position_df.loc[index, "lp_net_value_end"] + end_fee
        position_df.loc[index, "total_net_value"] = position_df.loc[index, "lp_net_value_this_hour"] + end_fee
        if index in collect_index_list:  # 当前这个小时是否有collect ,如果有, 累计手续费变为0, 假装这些钱已经提走了.
            current_sum_fee0 = 0
            current_sum_fee1 = 0
    # position_df = position_df.drop(columns=["lp_net_value_begin", "lp_net_value_end", "lp_net_value_this_hour"])
    position_df["return_rate"] = position_df["net_value_end"] / position_df["net_value_begin"]
    position_df.to_csv(os.path.join(config["save_path"], f"position_fee/{position_id}.csv"))


init_fee = False
freq = "1H"
multi_thread = True

if __name__ == "__main__":
    start_time = time.time()
    start = date(2021, 12, 21)
    end = date(2023, 6, 20)
    day = start
    print("loading fees")
    # 每天的手续费合并起来, 便于调试的时候快速加载
    if init_fee:
        day_fee_df_0 = load_fees("0")
        with open(os.path.join(config["save_path"], "all_daily_fee0.pkl"), "wb") as f:
            pickle.dump(day_fee_df_0, f)
        day_fee_df_1 = load_fees("1")
        with open(os.path.join(config["save_path"], "all_daily_fee1.pkl"), "wb") as f:
            pickle.dump(day_fee_df_1, f)
    else:
        with open(os.path.join(config["save_path"], "all_daily_fee0.pkl"), "rb") as f:
            day_fee_df_0 = pickle.load(f)
        with open(os.path.join(config["save_path"], "all_daily_fee1.pkl"), "rb") as f:
            day_fee_df_1 = pickle.load(f)
    tick_range_df = pd.read_csv(os.path.join(config["save_path"], "tick_fee/swap_tick_range.csv"), parse_dates=["day_str"])
    tick_range_df["day_str"] = tick_range_df["day_str"].apply(lambda a: a.date())  # quick search

    tick_range_df = tick_range_df.set_index(["day_str"])
    positions_df = pd.read_csv(os.path.join(config["save_path"], "position_liquidity.csv"),
                               dtype={"final_amount0": float, "final_amount1": float, "liquidity": float, "id": str},
                               parse_dates=["blk_time"])
    prices_df = pd.read_csv(os.path.join(config["save_path"], "price.csv"), parse_dates=["block_timestamp"])
    prices_df = prices_df.set_index(["block_timestamp"])
    prices_df = prices_df.resample(freq).first().ffill()
    print("start calc")
    ids = positions_df.groupby("id")
    current_time = datetime.combine(end + timedelta(days=1), datetime.min.time()) - timedelta(minutes=1)
    if multi_thread:
        ids = list(ids)
        with Pool(28) as p:
            res = list(tqdm(p.imap(process_one_position, ids), ncols=120, total=len(ids)))
    else:
        with tqdm(total=len(ids), ncols=150) as pbar:
            for position_id, rel_actions in ids:
                if position_id != "0x00a1df29047dc005c192e50ec87798dbab816f39-202520-202530":
                    pbar.update()
                    continue

                process_one_position((str(position_id), rel_actions))
                pbar.update()
    print("exec time", time.time() - start_time)
