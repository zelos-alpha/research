import pandas as pd
import os.path
from _decimal import Decimal
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import NamedTuple, List, Tuple, Dict
from utils import Position, PostionAction, PostionManager, format_date
from tqdm import tqdm
import time
import sys

"""
step3: 计算所有swap, 会给当前的仓位带来多少手续费
"""


config = {
    # "path": "/data/demeter-data/matic-usdc-weth-005-tick",
    "path": "~/data/polygon/usdc_weth-updated",
}


@dataclass
class LivePosition:
    id: str
    upper_tick: int
    lower_tick: int
    liquidity: int
    amount0: int = 0
    amount1: int = 0


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
    if tx["position_id"]:
        return str(int(tx["position_id"]))
    return f'{ tx["sender"]}-{int( tx[0])}-{int( tx[1])}'


def get_tick_key(tx_row: pd.Series) -> Tuple[int, int]:
    return (int(tx_row["tick_lower"]), int(tx_row["tick_upper"]))


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
                    PostionAction.mint,
                    total_liquidity,
                    address=tx["sender"],
                )

            case "BURN":
                tick_key = get_tick_key(tx)
                if tick_key not in current_position:
                    raise RuntimeError(f"Burn: {tick_key} not exist")
                pos_key_str = get_pos_key(tx)
                idx, pos_in_list = find_pos_from_list(
                    current_position[tick_key], pos_key_str
                )
                if pos_in_list:
                    pos_in_list.liquidity -= tx["liquidity"]
                    pos_in_list.amount0 += tx["amount0"]
                    pos_in_list.amount1 += tx["amount1"]
                else:
                    raise RuntimeError(f"Burn: {tick_key},{pos_key_str} not exist")

                pos_manager.add_history(
                    pos_key_str,
                    tx["block_timestamp"],
                    tx["amount0"],
                    tx["amount1"],
                    PostionAction.burn,
                    Decimal(0),
                )
            case "COLLECT":
                tick_key = get_tick_key(tx)
                if tick_key not in current_position:
                    raise RuntimeError(f"Burn: {tick_key} not exist")
                pos_key_str = get_pos_key(tx)
                idx, pos_in_list = find_pos_from_list(
                    current_position[tick_key], pos_key_str
                )
                if pos_in_list:
                    pos_in_list.amount0 -= tx["amount0"]
                    pos_in_list.amount1 -= tx["amount1"]
                    # TODO : 和自己计算的手续费比较, 如果超过太多要均摊
                    if (
                        pos_in_list.liquidity == 0
                        # 由于这个amount不包含手续费, 所以肯定比链上collect的值少.
                        and pos_in_list.amount0 < 0
                        and pos_in_list.amount1 < 0
                    ):
                        del current_position[tick_key][idx]
                    if len(current_position[tick_key]) == 0:
                        del current_position[tick_key]
                else:
                    raise RuntimeError(f"Burn: {tick_key},{pos_key_str} not exist")
                
                pos_manager.add_history(
                    pos_key_str,
                    tx["block_timestamp"],
                    tx["amount0"], # TODO: 注意: 这里和fee是真实值, 和预估值有差距, 应该处理一下. 
                    tx["amount1"],
                    PostionAction.collect,
                    Decimal(0),
                    fee_will_append=False
                )
            case "SWAP":
                pass


if __name__ == "__main__":
    start = date(2021, 12, 20)
    end = date(2023, 5, 30)
    day = start

    current_position: Dict[Tuple[int, int], List[LivePosition]] = {}
    pos_manager: PostionManager = PostionManager()

    # with tqdm(total=(end - start).days, ncols=150) as pbar:
    while day <= end:
        day_str = format_date(day)

        file = os.path.join(
            config["path"],
            f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv",
        )

        if day > date(2021, 12, 21):
            day += timedelta(days=1)
            # pbar.update()
            continue

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
        print(day_str, sys.getsizeof(current_position), pos_manager.get_size_str())
        day += timedelta(days=1)
        # pbar.update()

    print("")
    print(current_position)

    pass
