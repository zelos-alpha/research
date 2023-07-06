import os.path
import time
from _decimal import Decimal
from datetime import timedelta, date
from typing import List, Tuple

import pandas as pd
from tqdm import tqdm

from utils import format_date, config, to_decimal, PositionLiquidity, get_pos_key

"""
step 5: 构建position的操作列表, 包括mint, burn, collect. 然后按照position, block number的顺序来保存
"""


# @dataclass
# class LivePosition:
#     id: str
#     lower_tick: int
#     upper_tick: int
#     liquidity: Decimal
#     !!!注意!!!: 对于这个数据结构, amount0是collect_fee_0,
#     只记录最终collect的时候, 分了多少手续费, 和swap没关系, 只是校对结果用的
#     amount0: Decimal = Decimal(0)
#     amount1: Decimal = Decimal(0)

def find_pos_from_list(
        pos_list: List[PositionLiquidity], key_word
) -> Tuple[int, PositionLiquidity | None]:
    idx = 0
    for pos in pos_list:
        if pos.id == key_word:
            return idx, pos
        idx += 1
    return -1, None


def get_tick_key(tx_row: pd.Series) -> Tuple[int, int]:
    return int(tx_row["tick_lower"]), int(tx_row["tick_upper"])


def process_day(day_tx: pd.DataFrame):
    for tx_index, tx in day_tx.iterrows():
        pos_key_str = get_pos_key(tx)
        tick_key = get_tick_key(tx)
        amount0 = amount1 = liquidity = Decimal(0)

        match tx["tx_type"]:
            case "MINT":
                liquidity = tx["liquidity"]
                amount0 = tx["amount0"]
                amount1 = tx["amount1"]
            case "BURN":
                liquidity = Decimal(0) - tx["liquidity"]
                amount0 = tx["amount0"]
                amount1 = tx["amount1"]
            case "COLLECT":
                amount0 = tx["amount0"]
                amount1 = tx["amount1"]

        position_history.append(PositionLiquidity(
            pos_key_str,
            tick_key[0],
            tick_key[1],
            tx["tx_type"],
            tx["block_number"],
            tx["transaction_hash"],
            tx["pool_log_index"],
            tx["block_timestamp"],
            liquidity,
            amount0,
            amount1))


# file_name = "position_liquidity.pkl"
if __name__ == "__main__":
    # result_path = os.path.join(config["save_path"], file_name)
    start = date(2021, 12, 20)
    end = date(2023, 6, 20)
    day = start

    # if os.path.exists(result_path):
    #     with open(result_path, "rb") as f:
    #         current_position = pickle.load(f)
    # else:
    position_history: List[PositionLiquidity] = []

    with tqdm(total=(end - start).days + 1, ncols=150) as pbar:
        while day <= end:
            timer_start = time.time()
            day_str = format_date(day)

            file = os.path.join(
                config["path"],
                f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv",
            )

            # if day > date(2021, 12, 31):
            #     day += timedelta(days=1)
            #     continue

            df_tx_day: pd.DataFrame = pd.read_csv(
                file,
                converters={
                    "amount0": to_decimal,
                    "amount1": to_decimal,
                    "total_liquidity": to_decimal,
                    "liquidity": to_decimal,
                },
                dtype={"block_timestamp": object},
            )
            df_tx_day["block_timestamp"] = pd.to_datetime(
                df_tx_day["block_timestamp"], format="%Y-%m-%d %H:%M:%S"
            )
            df_tx_day = df_tx_day[df_tx_day["tx_type"] != "SWAP"]
            process_day(df_tx_day)
            day += timedelta(days=1)
            # save_state(RuntimeVar(day))
            timer_end = time.time()
            # print(
            #     day_str,
            #     f"{timer_end - timer_start}s",
            #     "current postions key count",
            #     len(current_position.keys()),
            # )
            pbar.update()
    print("generate result, please wait")
    result_df = pd.DataFrame(position_history)
    result_df = result_df.sort_values(["id", "block_number", "log_index"])
    result_df.to_csv(os.path.join(config["save_path"], "position_liquidity.csv"), index=False)
    pass
