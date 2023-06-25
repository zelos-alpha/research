import pandas as pd
import os.path
from _decimal import Decimal
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import NamedTuple, List, Tuple, Dict
from utils import Position, PositionAction, PositionManager, format_date, config
from tqdm import tqdm
import time
import sys
import pickle

"""
step 4: 计算所有swap, 会给当前的仓位带来多少手续费
放弃, 这个方法计算量太大, 没有办法做采样化简, 算了30天的大概就有2.2G的历史数据以及2G的current数据了
"""



@dataclass
class LivePosition:
    id: str
    lower_tick: int
    upper_tick: int
    liquidity: Decimal
    amount0: Decimal = Decimal(0)
    amount1: Decimal = Decimal(0)


def to_decimal(value):
    return Decimal(value) if value else Decimal(0)


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
    return f'{ tx["sender"]}-{int( tx["tick_lower"])}-{int( tx["tick_upper"])}'


def get_tick_key(tx_row: pd.Series) -> Tuple[int, int]:
    return (int(tx_row["tick_lower"]), int(tx_row["tick_upper"]))




def distribute_swap_fee(swap_tx: pd.Series):
    for same_range_pos in current_position.values():
        for pos in same_range_pos:
            if not (pos.upper_tick > swap_tx["current_tick"] > pos.lower_tick):
                continue
            # if the simulating liquidity is above the actual liquidity, we will consider share=1
            share = (
                pos.liquidity / swap_tx["total_liquidity"]
                if pos.liquidity < swap_tx["total_liquidity"]
                else Decimal(1)
            )
            fee0 = Decimal(0)
            fee1 = Decimal(0)

            if swap_tx["amount0"] > 0:
                fee0 = swap_tx["amount0"] * share * config["pool_fee_rate"]
            else:
                fee1 = swap_tx["amount1"] * share * config["pool_fee_rate"]
            pos_manager.add_history(
                pos.id, swap_tx["block_timestamp"], fee0, fee1, PositionAction.SWAP
            )
            pos.amount0 += fee0
            pos.amount1 += fee1

def remove_if_empty(pos_in_list_index: int, pos_in_list: LivePosition, tick_key):
    if (
        pos_in_list.liquidity < 1000
        # 由于这个amount不包含手续费, 所以肯定比链上collect的值少.
        and pos_in_list.amount0 < config["ignore_threshold_0"]
        and pos_in_list.amount1 < config["ignore_threshold_1"]
    ):
        del current_position[tick_key][pos_in_list_index]
        pos_manager.dump_and_remove(
            os.path.join(config["save_path"], "positions"), pos_in_list.id
        )

    if len(current_position[tick_key]) == 0:
        del current_position[tick_key]


def process_day(day_tx: pd.DataFrame):
    with tqdm(total=len(day_tx.index), ncols=150) as pbar:
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
                    total_liquidity = 0
                    if pos_in_list:
                        pos_in_list.liquidity += tx["liquidity"]
                        total_liquidity = pos_in_list.liquidity
                    else:
                        current_position[tick_key].append(
                            LivePosition(
                                pos_key_str, tick_key[0], tick_key[1], tx["liquidity"]
                            )
                        )
                        total_liquidity = tx["liquidity"]
                    # history
                    pos_manager.add_position(pos_key_str, tick_key[0], tick_key[1])
                    pos_manager.add_history(
                        pos_key_str,
                        tx["block_timestamp"],
                        0,
                        0,
                        PositionAction.MINT,
                        total_liquidity,
                        address=tx["sender"],
                    )

                case "BURN":
                    tick_key = get_tick_key(tx)
                    if tx["liquidity"] < config["ignore_threshold_L"]:
                        pbar.update()
                        continue
                    if tick_key not in current_position:
                        raise RuntimeError(f"Burn: {tick_key}, {tx['transaction_hash']} not exist")
                    
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
                            f"Burn: {tick_key},{pos_key_str},{tx['transaction_hash']} not exist"
                        )
                    pos_manager.add_history(
                        pos_key_str,
                        tx["block_timestamp"],
                        tx["amount0"],
                        tx["amount1"],
                        PositionAction.BURN,
                        Decimal(0),
                    )
                case "COLLECT":
                    tick_key = get_tick_key(tx)
                    if tx["amount0"] < config["ignore_threshold_0"] and tx["amount1"] < config["ignore_threshold_1"]:
                        pbar.update()
                        continue
                    if tick_key not in current_position:
                        raise RuntimeError(
                            f"Burn: {tick_key},{tx['transaction_hash']} not exist"
                        )
                    pos_key_str = get_pos_key(tx)
                    idx, pos_in_list = find_pos_from_list(
                        current_position[tick_key], pos_key_str
                    )
                    if pos_in_list:
                        pos_in_list.amount0 -= tx["amount0"]
                        pos_in_list.amount1 -= tx["amount1"]
                        # TODO : 和自己计算的手续费比较, 如果超过太多要均摊
                    else:
                        raise RuntimeError(
                            f"Collect: {tick_key},{pos_key_str},{tx['transaction_hash']} not exist"
                        )

                    pos_manager.add_history(
                        pos_key_str,
                        tx["block_timestamp"],
                        tx["amount0"],  # TODO: 注意: 这里和fee是真实值, 和预估值有差距, 应该处理一下.
                        tx["amount1"],
                        PositionAction.COLLECT,
                        Decimal(0),
                        fee_will_append=False,
                    )
                    remove_if_empty(idx, pos_in_list, tick_key)

                case "SWAP":
                    distribute_swap_fee(swap_tx=tx)
            pbar.update()


@dataclass
class RuntimeVar:
    current_day: datetime


def save_state(runtime_var):
    with open(os.path.join(config["save_path"], "runtime_var.pkl"), "wb") as f:
        pickle.dump(runtime_var, f)
    with open(os.path.join(config["save_path"], "current_positions.pkl"), "wb") as f:
        pickle.dump(current_position, f)
    with open(os.path.join(config["save_path"], "history.pkl"), "wb") as f:
        pickle.dump(pos_manager._content, f)


def load_state():
    var_path = os.path.join(config["save_path"], "runtime_var.pkl")
    if not os.path.exists(var_path):
        return None, {}, PositionManager()
    with open(var_path, "rb") as f:
        runtime_var: RuntimeVar = pickle.load(f)
    with open(os.path.join(config["save_path"], "current_positions.pkl"), "rb") as f:
        current_position = pickle.load(f)
    with open(os.path.join(config["save_path"], "history.pkl"), "rb") as f:
        pos_manager = PositionManager()
        pos_manager._content = pickle.load(f)
    return runtime_var, current_position, pos_manager


if __name__ == "__main__":
    start = date(2021, 12, 20)
    end = date(2023, 5, 30)
    day = start

    current_position: Dict[Tuple[int, int], List[LivePosition]] = {}
    pos_manager: PositionManager = PositionManager()
    runtime_var, current_position, pos_manager = load_state()
    if runtime_var != None:
        day = runtime_var.current_day
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
        save_state(RuntimeVar(day))
        timer_end = time.time()
        print(
            day_str,
            f"{timer_end - timer_start}s",
            "current postions key count",
            len(current_position.keys()),
            "pos manager size",
            pos_manager.get_size_str(),
        )

    print("")
    # print(current_position)

    pass