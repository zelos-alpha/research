from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import os
import pickle
from typing import List, NamedTuple, Tuple, Dict, List

config = {
    # "path": "/data/demeter-data/usdc_weth_1",
    # "save_path": "/data/demeter-data/uni_replay",
    "path": "~/data/polygon/usdc_weth_1",
    "save_path": "/home/sun/workspace/data/uni_pool_evaluation",
    "pool_fee_rate": Decimal(0.0005),
    "is_0_base": True,
    "decimal0": 6,
    "decimal1": 18,
    "ignore_threshold_0": Decimal(1000000),  # 1u
    "ignore_threshold_1": Decimal(1000000000000000),  #  0.001 eth
    "ignore_threshold_L": Decimal(1000000),
}

def to_decimal(value):
    return Decimal(value) if value else Decimal(0)

def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


def time2timestamp(t: datetime) -> int:
    return int(t.timestamp() * 1000)


class PostionAction(Enum):
    mint = 1
    burn = 2
    collect = 3
    swap = 4


class PositionHistory(NamedTuple):
    timestamp: int  # 为了提高性能
    blk_time: datetime  # 时间戳, 因为是用tick计算的, 所以每个swap, 会有一个timestamp
    fee0: Decimal
    fee1: Decimal
    liquidity: Decimal  # 本来应该用int, 但是为了转换pandas的时候不转换为float, 用Decimal存储
    action: PostionAction


class Position(NamedTuple):
    id: str
    lower_tick: int
    upper_tick: int
    history: List[PositionHistory]
    # 地址的变化列表, Tuple(start,end, address), start, end是左闭右开
    addr_history: List[Tuple[int, int, str]]


@dataclass
class LivePosition:
    id: str
    lower_tick: int
    upper_tick: int
    liquidity: Decimal
    amount0: Decimal = Decimal(0)
    amount1: Decimal = Decimal(0)


class PostionManager(object):
    def __init__(self) -> None:
        self._content: Dict[str, Position] = {}

    def get_size_str(self) -> str:
        key_count = 0
        history_count = 0
        for v in self._content.values():
            key_count += 1
            history_count += len(v.history)

        return f"key count: {key_count}, value count: {history_count}"

    def add_position(self, id: str, lower: int, upper: int):
        if id not in self._content:
            position = Position(id, lower, upper, [], [])
            self._content[id] = position

    def add_history(
        self,
        id: str,
        blk_time: datetime,
        fee0: Decimal,
        fee1: Decimal,
        action: PostionAction,
        liquidity: Decimal = Decimal(0),
        address: str = None,
        fee_will_append=True,  # fee的两个字段, 是否要叠加之前的fee
    ):
        if id not in self._content:
            raise RuntimeError(f"{id} not found, call add_position first")
        pos = self._content[id]

        if not fee_will_append or len(pos.history) < 1:
            fee0 = fee0
            fee1 = fee1
        else:
            last_pos: PositionHistory = pos.history[len(pos.history) - 1]
            fee0 = fee0 + last_pos.fee0
            fee1 = fee1 + last_pos.fee1
        history = PositionHistory(
            time2timestamp(blk_time), blk_time, fee0, fee1, liquidity, action
        )
        pos.history.append(history)
        current_index = len(pos.history) - 1
        if address:
            last_idx = -1 if len(pos.addr_history) < 1 else len(pos.addr_history) - 1
            if last_idx > -1:
                pos.addr_history[last_idx] = current_index
            pos.addr_history.append((current_index, 999999999999, address))

    def dump_and_remove(self, save_path: str, key: str):
        file_name = os.path.join(save_path, key + ".pkl")
        with open(file_name, "wb") as f:
            pickle.dump(self._content[key], f)
        del self._content[key]
