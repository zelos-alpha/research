import os.path
from _decimal import Decimal
from dataclasses import dataclass
from datetime import datetime
from typing import NamedTuple, List, Tuple, Dict
import pandas as pd
from demeter.uniswap.helper import quote_price_to_sqrt
from demeter.uniswap.liquitidy_math import get_sqrt_ratio_at_tick, get_amounts

pd.options.display.max_columns = None
pd.options.display.max_rows = 99999
pd.set_option('display.width', 5000)
pd.options.display.max_colwidth = 100

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


def to_minute(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute)


@dataclass
class Balance:
    """
    每分钟
    """
    blk_time: datetime  # 每分钟, 需要对原始数据resample.
    timestamp: int
    # 原始数据
    positions: List[UniPosition]
    # 额外数据
    price: float = 0.0
    # 计算值
    net_value: float = 0.0
    amount0: float = 0.0
    amount1: float = 0.0  # amount0 + amount1* price
    net_value_with_pre_l: float = 0.0


def generate_test_data() -> List[Position]:
    addrs = [(0, 7, "0x1"),
             (7, 10, "0x2")]
    phhhh = [
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 0)), datetime(2023, 5, 8, 15, 0), 1000000, 1000000000000000, 100000000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 1)), datetime(2023, 5, 8, 15, 1), 2000000, 2000000000000000, 110000000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 2)), datetime(2023, 5, 8, 15, 2), 3000000, 3000000000000000, 100000000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 3)), datetime(2023, 5, 8, 15, 3), 4000000, 4000000000000000, 100000000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 10)), datetime(2023, 5, 8, 15, 10), 5000000, 5000000000000000, 100000000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 11)), datetime(2023, 5, 8, 15, 11), 6000000, 6000000000000000, 100000000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 11, 30)), datetime(2023, 5, 8, 15, 12), 7000000, 7000000000000000, 100000000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 20)), datetime(2023, 5, 8, 15, 20), 8000000, 8000000000000000, 100000000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 21)), datetime(2023, 5, 8, 15, 21), 9000000, 9000000000000000, 100000000000000000),
        PositionHistory(datetime2timestamp(datetime(2023, 5, 8, 15, 22)), datetime(2023, 5, 8, 15, 22), 10000000, 10000000000000000, 100000000000000000),

    ]
    p = Position(12345, 200000, 202000, phhhh, addrs)
    return [p]


def convert_position_to_address(positions: List[Position]) -> Dict[str, List[Balance]]:
    # TODO : 加入逻辑: 如果tick重复, 合并流动性
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
        addr_balances[addr] = [Balance(timestamp=x[0], blk_time=timestamp2datetime(x[0]), positions=x[1]) for x in time_position_list]
    return addr_balances


def fill_price(df: pd.DataFrame | List[Balance]):
    for index, row in df.iterrows():
        t = to_minute(row["blk_time"])
        df.loc[index, "price"] = price_df.loc[t]["price"]
    return df


def get_net_value(amount0, amount1, price):
    if config["is_0_base"]:
        return amount0 + price * amount1
    else:
        return amount0 * price + amount1


def find_pos(tick_range: Tuple[int, int], rows: List[UniPosition]):
    for pos in rows:
        if pos.lower_tick == tick_range[0] and pos.upper_tick == tick_range[1]:
            return pos
    return None


def fill_net_value(df: pd.DataFrame) -> pd.DataFrame:
    for index, row in df.iterrows():
        amount0_sum = 0
        amount1_sum = 0
        sqrt_price = int(quote_price_to_sqrt(Decimal(row["price"]), config["decimal0"], config["decimal1"], config["is_0_base"]))
        # 计算净值.
        for pos_of_tick in row["positions"]:
            pos_of_tick: UniPosition = pos_of_tick
            amount0_sum += pos_of_tick.fee_amount0 / 10 ** config["decimal0"]
            amount1_sum += pos_of_tick.fee_amount1 / 10 ** config["decimal1"]

            position_amount0, position_amount1 = get_amounts(sqrt_price,
                                                             pos_of_tick.lower_tick,
                                                             pos_of_tick.upper_tick,
                                                             pos_of_tick.liquidity,
                                                             config["decimal0"],
                                                             config["decimal1"])
            amount0_sum += float(position_amount0)
            amount1_sum += float(position_amount1)

        df.loc[index, "amount0"] = amount0_sum
        df.loc[index, "amount1"] = amount1_sum
        df.loc[index, "net_value"] = get_net_value(amount0_sum, amount1_sum, row["price"])
        # 计算除去流动性增减考虑的净值, 用于计算收益率
        # 算法: "前一个tick"的流动性, 和"这个tick"的价格以及手续费收入来计算"净值NVp",
        # 计算收益率的时候用NVp除以前一个tick的净值NV得到收益率.
        if index == 0:
            df.loc[index, "net_value_with_pre_l"] = df.loc[index, "net_value"]
        else:
            pre_amount0_sum = 0
            pre_amount1_sum = 0
            for pre_pos_of_tick in df.loc[index - 1]["positions"]:
                new_pos: UniPosition = find_pos(pre_pos_of_tick, row["positions"])  # 找到前一行, 在这一行中, 相同的postion
                if new_pos is None:  # position is burned
                    new_pos = pre_pos_of_tick
                pre_amount0_sum += new_pos.fee_amount0 / 10 ** config["decimal0"]
                pre_amount1_sum += new_pos.fee_amount1 / 10 ** config["decimal1"]
                pre_position_amount0, pre_position_amount1 = get_amounts(sqrt_price,
                                                                         pre_pos_of_tick.lower_tick,
                                                                         pre_pos_of_tick.upper_tick,
                                                                         pre_pos_of_tick.liquidity,
                                                                         config["decimal0"],
                                                                         config["decimal1"])
                pre_amount0_sum += float(pre_position_amount0)
                pre_amount1_sum += float(pre_position_amount1)

            df.loc[index, "net_value_with_pre_l"] = get_net_value(pre_amount0_sum, pre_amount1_sum, row["price"])

    return df


def get_return_return(df: pd.DataFrame) -> pd.DataFrame:
    df["net_value_with_pre_l"] = df.net_value_with_pre_l.shift(-1)
    df["return_rate"] = df["net_value_with_pre_l"] / df["net_value"]
    df = df.drop(columns=["positions"])
    return df


config = {
    "is_0_base": True,
    "decimal0": 6,
    "decimal1": 18
}

if __name__ == "__main__":
    data_path = "/data/research_data/uni/polygon/usdc_weth-updated"
    price_df = pd.read_csv(os.path.join(data_path, "price.csv"), parse_dates=['block_timestamp'])
    price_df = price_df.set_index(["block_timestamp"])
    positions = generate_test_data()
    address_balances = convert_position_to_address(positions)
    # print(address_balances)

    for addr, balance in address_balances.items():
        df = pd.DataFrame(balance)
        df = fill_price(df)
        df = fill_net_value(df)
        df = get_return_return(df)
        print(df)
