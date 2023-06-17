from datetime import date, datetime
from typing import List, NamedTuple, Tuple, Dict, List


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


class PositionHistory(NamedTuple):
    timestamp: int  # 为了提高性能
    blk_time: datetime  # 时间戳, 因为是用tick计算的, 所以每个swap, 会有一个timestamp
    fee0: int
    fee1: int
    liquidity: int


class Position(NamedTuple):
    id: str
    lower_tick: int
    upper_tick: int
    history: List[PositionHistory]
    addr_history: List[
        Tuple[int | datetime, int | datetime, str]
    ]  # 地址的变化列表, Tuple(start,end, address), start, end是左闭右开


class PostionManager(object):
    def __init__(self) -> None:
        self.__content: Dict[str, Position] = {}

    def get_size_str(self) -> str:
        key_count = 0
        history_count = 0
        for v in self.__content:
            key_count += 1
            history_count += len(v.addr_history)

        return f"key count: {key_count}, value count: {history_count}"
