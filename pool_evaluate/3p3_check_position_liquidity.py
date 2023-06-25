import os.path
import pickle
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import List, Tuple, Dict

import pandas as pd
from tqdm import tqdm

from utils import LivePosition, format_date, config, to_decimal

"""
step 3.3: 构建当前所有的position_id, 检查mint和burn的数据是否合格. 是否有缺失positon_id的情况
"""


def find_pos_from_list(
        pos_list: List[LivePosition], key_word
) -> Tuple[int, LivePosition | None]:
    idx = 0
    for pos in pos_list:
        if pos.id == key_word:
            return idx, pos
        idx += 1
    return -1, None


def get_pos_key(tx: pd.Series) -> str:
    if tx["position_id"] and not pd.isna(tx["position_id"]):
        return str(int(tx["position_id"]))
    return f'{tx["sender"]}-{int(tx["tick_lower"])}-{int(tx["tick_upper"])}'


def get_tick_key(tx_row: pd.Series) -> Tuple[int, int]:
    return int(tx_row["tick_lower"]), int(tx_row["tick_upper"])


# def distribute_swap_fee(swap_tx: pd.Series):
#     for same_range_pos in current_position.values():
#         for pos in same_range_pos:
#             if not (pos.upper_tick > swap_tx["current_tick"] > pos.lower_tick):
#                 continue
#             # if the simulating liquidity is above the actual liquidity, we will consider share=1
#             share = (
#                 pos.liquidity / swap_tx["total_liquidity"]
#                 if pos.liquidity < swap_tx["total_liquidity"]
#                 else Decimal(1)
#             )
#             fee0 = Decimal(0)
#             fee1 = Decimal(0)

#             if swap_tx["amount0"] > 0:
#                 fee0 = swap_tx["amount0"] * share * config["pool_fee_rate"]
#             else:
#                 fee1 = swap_tx["amount1"] * share * config["pool_fee_rate"]
#             pos.amount0 += fee0
#             pos.amount1 += fee1

def remove_if_empty(pos_in_list_index: int, pos_in_list: LivePosition, tick_key):
    if (
            pos_in_list.liquidity < 1000
            # 由于这个amount不包含手续费, 所以肯定比链上collect的值少.
            and pos_in_list.amount0 < config["ignore_threshold_0"]
            and pos_in_list.amount1 < config["ignore_threshold_1"]
    ):
        del current_position[tick_key][pos_in_list_index]

    if len(current_position[tick_key]) == 0:
        del current_position[tick_key]


def process_day(day_tx: pd.DataFrame):
    for tx_index, tx in day_tx.iterrows():
        match tx["tx_type"]:
            case "MINT":
                tick_key = get_tick_key(tx)
                if tick_key not in current_position:
                    current_position[tick_key] = []
                pos_key_str = get_pos_key(tx)
                idx, pos_in_list = find_pos_from_list(
                    current_position[tick_key], pos_key_str
                )
                if pos_in_list:
                    pos_in_list.liquidity += tx["liquidity"]
                else:
                    current_position[tick_key].append(
                        LivePosition(
                            pos_key_str, tick_key[0], tick_key[1], tx["liquidity"]
                        )
                    )

            case "BURN":
                tick_key = get_tick_key(tx)
                if tx["liquidity"] < config["ignore_threshold_L"]:
                    continue
                if tick_key not in current_position:
                    raise RuntimeError(
                        f"Burn:{day_str} {tick_key}, {tx['transaction_hash']} not exist"
                    )

                pos_key_str = get_pos_key(tx)
                idx, pos_in_list = find_pos_from_list(
                    current_position[tick_key], pos_key_str
                )
                if pos_in_list:
                    pos_in_list.liquidity -= tx["liquidity"]
                    pos_in_list.amount0 += tx["amount0"]
                    pos_in_list.amount1 += tx["amount1"]
                else:
                    raise RuntimeError(
                        f"Burn:{day_str} {tick_key},{pos_key_str},{tx['transaction_hash']} not exist"
                    )

            case "COLLECT":
                tick_key = get_tick_key(tx)
                if (
                        tx["amount0"] < config["ignore_threshold_0"]
                        and tx["amount1"] < config["ignore_threshold_1"]
                ):
                    continue
                if tick_key not in current_position:
                    raise RuntimeError(
                        f"Burn:{day_str} {tick_key},{tx['transaction_hash']} not exist"
                    )
                pos_key_str = get_pos_key(tx)
                idx, pos_in_list = find_pos_from_list(
                    current_position[tick_key], pos_key_str
                )
                if pos_in_list:
                    pos_in_list.amount0 -= tx["amount0"]
                    pos_in_list.amount1 -= tx["amount1"]
                else:
                    raise RuntimeError(
                        f"Collect:{day_str} {tick_key},{pos_key_str},{tx['transaction_hash']} not exist"
                    )
                # remove_if_empty(idx, pos_in_list, tick_key)
            case "SWAP":
                pass  # distribute_swap_fee(swap_tx=tx)






file_name = "position_liquidity.pkl"
if __name__ == "__main__":
    result_path = os.path.join(config["save_path"], file_name)
    start = date(2023, 6, 20)
    end = date(2023, 6, 20)
    day = start

    if os.path.exists(result_path):
        with open(result_path, "rb") as f:
            current_position = pickle.load(f)
    else:
        current_position: Dict[Tuple[int, int], List[LivePosition]] = {}

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
            with open(result_path, "wb") as f:
                pickle.dump(current_position, f)
            pbar.update()
    # with open(result_path, "wb") as f:
    #     pickle.dump(current_position, f)
    pass