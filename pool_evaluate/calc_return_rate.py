from dataclasses import dataclass
from datetime import datetime
from typing import NamedTuple, List, Tuple, Dict
import pandas as pd

"""
step3: 并行计算手续费分成, 因此跳过

step4: 通过Positions, 计算Balance
"""


class PositionHistory(NamedTuple):
    timestamp: int  # 为了提高性能
    blk_time: datetime  # 时间戳, 因为是用tick计算的, 所以每个swap, 会有一个timestamp
    fee0: int
    fee1: int
    liquidity: int


class Position(NamedTuple):
    id: int | str
    lower_tick: int
    upper_tick: int
    history: List[PositionHistory]
    addr_history: List[Tuple[int | datetime, int | datetime, str]]  # 地址的变化列表, Tuple(start,end, address), start, end是左闭右开


class UniPosition(NamedTuple):
    fee_amount0: int
    fee_amount1: int
    lower_tick: int
    upper_tick: int
    liquidity: int


class Pool(NamedTuple):
    decimal0: int
    decimal1: int
    is_0_base: bool


def datetime2timestamp(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def timestamp2datetime(tp: int) -> datetime:
    return datetime.fromtimestamp(tp / 1000)


@dataclass
class Balance:
    """
    每分钟
    """
    timestamp: datetime  # 每分钟, 需要对原始数据resample.
    # 原始数据
    positions: List[UniPosition]
    # 额外数据
    price: float = 0.0
    # 计算值
    net_value: float = 0.0
    amount0: float = 0.0
    amount1: float = 0.0  # amount0 + amount1* price
    net_value_delta: float = 0.0


def generate_test_data() -> List[Position]:
    addrs = [(0, 7, "0x1"),
             (7, 10, "0x2")]
    phhhh = [
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 0)), datetime(2023, 6, 8, 15, 0), 1, 1, 1000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 1)), datetime(2023, 6, 8, 15, 1), 2, 1, 1000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 2)), datetime(2023, 6, 8, 15, 2), 3, 1, 1000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 3)), datetime(2023, 6, 8, 15, 3), 4, 1, 1000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 10)), datetime(2023, 6, 8, 15, 10), 5, 1, 1000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 11)), datetime(2023, 6, 8, 15, 11), 6, 1, 1000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 12)), datetime(2023, 6, 8, 15, 12), 7, 1, 1000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 20)), datetime(2023, 6, 8, 15, 20), 8, 1, 1000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 21)), datetime(2023, 6, 8, 15, 21), 9, 1, 1000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 6, 8, 15, 22)), datetime(2023, 6, 8, 15, 22), 10, 1, 1000000000000),

    ]
    p = Position(12345, 123, 456, phhhh, addrs)
    return [p]


def convert_position_to_address(positions: List[Position]):
    addresses = set()
    for position in positions:
        for addr in position.addr_history:
            addresses.add(addr[2])
    addr_positions: Dict[str, Dict[int, List[UniPosition]]] = {}
    for addr in addresses:
        addr_positions[addr] = {}
    for position in positions:
        for pos_addr in position.addr_history:
            pos_histories: List[PositionHistory] = position.history[pos_addr[0]:pos_addr[1]]
            for ph in pos_histories:
                if ph.timestamp not in addr_positions[pos_addr[2]]:
                    addr_positions[pos_addr[2]][ph.timestamp] = []
                addr_positions[pos_addr[2]][ph.timestamp].append(
                    UniPosition(ph.fee0, ph.fee1, position.lower_tick, position.upper_tick, ph.liquidity)
                )
    addr_balances: Dict[str, List[Balance]] = {}
    for addr in addr_balances:
        addr_balances[addr] = []
    for addr, time_position_dict in addr_positions.items():
        time_position_list = [(timestamp, pos_list) for timestamp, pos_list in time_position_dict.items()]
        time_position_list.sort(key=lambda x: x[0])
        addr_balances[addr] = [Balance(timestamp=timestamp2datetime(x[0]), positions=x[1]) for x in time_position_list]
    return addr_balances


if __name__ == "__main__":
    positions = generate_test_data()
    address_balances = convert_position_to_address(positions)
    print(address_balances)
