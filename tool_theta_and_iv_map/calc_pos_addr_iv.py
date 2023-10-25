import os
from datetime import date, datetime, timedelta

import pandas as pd
import numpy as np
from typing import List


# active position 范围内持有仓位位活动， 比如时间范围内4月1日到2日，持仓数量范围，流动性大小
# 加载所有的地址
# 时间        地址                                        仓位id   流动性
# 2023-04-02 0xfffee8d9d34efa314418227cc4ae1117612498c3  814708  30
# 2023-04-01 0xfffee8d9d34efa314418227cc4ae1117612498c3  814708  100
# 2023-04-01 0xfffb5f35c7f179ffda3dca1add78007a150146a9  814710  50
# 2022-11-10 0xfffbc61b80115d1c617b4fa82b70eedf0f515b6a  469393  20

# 计算2023-04-02

# 第五步
# 获取2023-04-03 tick 第一条swap数据
# block_number,block_timestamp,tx_type,transaction_hash,pool_tx_index,pool_log_index,proxy_log_index,sender,receipt,amount0,amount1,total_liquidity,total_liquidity_delta,sqrtPriceX96,current_tick,position_id,tick_lower,tick_upper,liquidity
# 41074938,2023-04-03 00:00:41,SWAP,0xaa09511ba5340342481a38c7fe184511d7f556a098fd636bebd3d5e53ce90b90,37,157,,0x4c60051384bd2d3c01bfc845cf5f4b44bcbe9de5,0x769935f11b092723bda47ea04bc4325737182540,-300000000,167184340057430637,1859762728228853022,0,1869859520424625902221565458775477,201390.0,,,,
#

# 第六步
# weighted = position_liquidity / sum all_position

def format_dt(dt: date):
    return dt.strftime("%Y-%m-%d")


def fmt_full_dt(dt: date):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def load_address_info(path):
    # get all address from file name
    file_lst = os.listdir(path)
    return [file.split('.')[0] for file in file_lst]


def load_positions_info():
    # 加载所有的position 地址信息
    pass


def get_active_positions_by_time(period=100):
    pass


def get_active_positions_by_count(count=100):
    pass


def padding_nan(max_data, min_data):
    if pd.isna(max_data):
        max_data = min_data
    elif pd.isna(min_data):
        min_data = max_data
    return max_data, min_data


def fetch_iv(iv_df: pd.DataFrame, lower_tick: int, upper_tick: int):
    iv_df["upper_diff"] = iv_df["upper_tick"] - upper_tick
    iv_df["lower_diff"] = iv_df["lower_tick"] - lower_tick
    upper_max = iv_df["upper_diff"].loc[lambda x: x >= 0].min() + upper_tick
    upper_min = iv_df["upper_diff"].loc[lambda x: x <= 0].max() + upper_tick
    upper_max, upper_min = padding_nan(upper_max, upper_min)
    lower_max = iv_df["lower_diff"].loc[lambda x: x >= 0].min() + lower_tick
    lower_min = iv_df["lower_diff"].loc[lambda x: x <= 0].max() + lower_tick
    lower_max, lower_min = padding_nan(lower_max, lower_min)
    iv_lst = []

    try:
        if upper_max != lower_max:
            iv_lst.append(iv_df.loc[lambda x: (x.upper_tick == upper_max) & (x.lower_tick == lower_max)].iv.iloc[0])
        if upper_max != lower_min:
            iv_lst.append(iv_df.loc[lambda x: (x.upper_tick == upper_max) & (x.lower_tick == lower_min)].iv.iloc[0])
        if upper_min != lower_max:
            iv_lst.append(iv_df.loc[lambda x: (x.upper_tick == upper_min) & (x.lower_tick == lower_max)].iv.iloc[0])
        if upper_min != lower_min:
            iv_lst.append(iv_df.loc[lambda x: (x.upper_tick == upper_min) & (x.lower_tick == lower_min)].iv.iloc[0])

    except Exception as e:
        print(e)
    iv_lst = list(filter(lambda x: not pd.isna(x), iv_lst))
    result = sum(iv_lst) / len(iv_lst) if len(iv_lst) > 0 else 0
    if pd.isna(result):
        print(lower_tick, upper_tick)
    return result


def fetch_weighted_data(iv_liq_df: pd.DataFrame):
    sum_liquidity = 0
    for index, row in iv_liq_df.iterrows():
        sum_liquidity += row["liquidity"]
    # sum_liquidity = iv_liq_df["liquidity"].sum()
    iv_liq_df["prob"] = iv_liq_df["liquidity"] / sum_liquidity
    weighted = (iv_liq_df["prob"] * iv_liq_df["iv"]).sum()
    return weighted


def dict2dataframe(dic=None) -> pd.DataFrame:
    result = []
    for key, value in dic.items():
        result.append((key, value))
    result.sort(key=lambda x: x[0])
    return pd.DataFrame(result, columns=["iv", "liquidity"])


def main(
        start_date: date,
        end_date: date,
        active_period: int = 10,
        address_lst: List = None,
        tick_path: str = './tick_data/',
        iv_path: str = './iv_data/',
        result_path: str = './result/',
        pool_addr: str = '0x45dda9cb7c25131df268515131f647d726f50608'
        ):
    """
    active_period活跃范围的数据
    """
    addr_pos_df = pd.read_csv('./position_data/position_address.csv')
    if address_lst:
        # filter addr_pos_df by address_lst
        addr_pos_df = addr_pos_df[addr_pos_df['address'] in address_lst]

    position_df = pd.read_csv('./position_data/position_liquidity.csv')
    position_df = position_df[['id', 'lower_tick', 'upper_tick', 'tx_type', 'blk_time', 'liquidity']]
    position_df = position_df[position_df.tx_type == 'MINT']
    position_df.sort_values('blk_time', inplace=True)

    current_day = start_date  # start date
    mean_lst = []
    while current_day <= end_date:  # end date
        dt_str = format_dt(current_day)

        # 获取时间范围内的仓位数据
        start_filter_date = format_dt(current_day + timedelta(days=-active_period))
        period_df = position_df[(position_df.blk_time >= f'{start_filter_date} 00:00:00') & (position_df.blk_time <= f'{dt_str} 23:59:59')]
        # position 数据过去10天，包含今天数据

        # 加载tick数据
        next_tick_file = tick_path + f'polygon-{pool_addr}-' + format_dt(
            current_day) + '.tick.csv'
        next_tick_df = pd.read_csv(next_tick_file)
        next_tick_first_swap = next_tick_df[next_tick_df.tx_type == "SWAP"].head(1)
        next_tick_current_tick = next_tick_first_swap["current_tick"].iloc[0]

        iv_df = pd.read_csv(f"{iv_path}{dt_str}.csv")
        iv_liq_dict = {}
        for _, row in period_df.iterrows():
            # check id in addr_pos_df
            if addr_pos_df[addr_pos_df['position'] == row.id].empty:
                continue
            if not row.lower_tick <= next_tick_current_tick <= row.upper_tick:
                continue
            iv = fetch_iv(iv_df, row.lower_tick, row.upper_tick)
            if iv not in iv_liq_dict:
                iv_liq_dict[iv] = 0
            iv_liq_dict[iv] += int(row.liquidity)
        iv_liq_df = dict2dataframe(iv_liq_dict)
        mean = fetch_weighted_data(iv_liq_df)  # 计算
        print(mean)
        mean_lst.append((dt_str, mean))

        print(current_day)
        current_day += timedelta(days=1)
    final_df = pd.DataFrame(mean_lst, columns=["date", "mean"])
    final_df.to_csv(f"{result_path}final_daily_mean.csv", index=False)


if __name__ == '__main__':
    start_date, end_date = date(2023, 1, 1), date(2023, 5, 30)
    address_lst = []
    main(start_date=start_date, end_date=end_date, address_lst=address_lst)
