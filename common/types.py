import datetime
from collections import namedtuple
from enum import Enum
from typing import Union, NamedTuple
from decimal import Decimal

CurrentState = namedtuple("CurrentState", ["sqrtPriceX96", "tick", "observationIndex",
                                           "observationCardinality", "observationCardinalityNext",
                                           "feeProtocol", "unlocked", "liqulidity"])


class OnchainTxType(Enum):
    MINT = 0
    SWAP = 2
    BURN = 1
    COLLECT = 3


class ChainEvent(NamedTuple):
    block_number: int
    block_timestamp: datetime.datetime
    transaction_index: int
    log_index: int
    tx_type: OnchainTxType
    amount0: int
    amount1: int
    current_liquidity: int
    current_tick: int


class PoolBaseInfo(NamedTuple):
    token0: str
    token1: str
    pool: str
    fee: int
    tickSpacing: int
    token0_decimal: int
    token1_decimal: int
    token0_is_usdlike: bool


class Position(NamedTuple):
    lower_tick: int
    upper_tick: int
    liquidity: int
    uncollectedfee_token0: int
    uncollectedfee_token1: int


class StrategyStatus(NamedTuple):
    timestamp: datetime.datetime
    usd_in_wallet: float
    othertoken_in_wallet: float
    usd_price: float
    usd_in_pool: float
    othertoken_in_pool: float
    uncollected_usd: float
    uncollected_othertoken: float
