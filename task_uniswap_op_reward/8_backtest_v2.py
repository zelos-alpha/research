import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Union

import demeter
import numpy as np
import pandas as pd
from demeter.broker.liquitidymath import get_sqrt_ratio_at_tick
from demeter.broker.v3_core import V3CoreLib
from pandas import Timedelta

from demeter import Strategy, TokenInfo, PoolBaseInfo, RowData, Actuator, ChainType, PositionInfo, Position, Broker

pd.options.display.max_columns = None
pd.options.display.max_rows = 12
pd.set_option('display.width', 5000)
pd.options.display.max_colwidth = 100


@dataclass
class Results:
    return_rate: Decimal
    return_rate_with_op: Decimal
    net_value_with_op: Decimal
    share: Decimal
    liquidity: int
    uncollect: Decimal
    balance: Decimal

    @staticmethod
    def names():
        return ["return_rate", "return_rate_with_op", "net_value_with_op", "share", "liquidity", "uncollect", "balance"]

    def to_array(self):
        return [self.return_rate, self.return_rate_with_op, self.net_value_with_op,
                self.share, self.liquidity, self.uncollect, self.balance]


def decimal_from_value(value):
    return Decimal(value) if value != "" else Decimal(0)


def from_unit(value, token: TokenInfo):
    return Decimal(value) * Decimal(10 ** (-token.decimal))


def to_minute(time: datetime) -> datetime:
    return datetime(time.year, time.month, time.day, time.hour, time.minute)


def safe_div(a, b):
    if a == 0 and b == 0:
        return 1
    elif b == 0:
        return 1
    else:
        return a / b


class ReplayStrategy(Strategy):
    def __init__(self, strategy_actions):
        super().__init__()
        self._actions: pd.DataFrame = strategy_actions  # .head(1)
        self._actions["block_timestamp"] = self._actions.block_timestamp.apply(lambda x: to_minute(x))
        self._actions = self._actions.set_index('block_timestamp')
        self.results: [Results] = []
        self.net_value_before = Decimal(0)
        self.net_value_with_op_before = Decimal(0)

    def before_bar(self, row_data: Union[RowData, pd.Series]):
        if row_data.row_id < 1:
            price = row_data.price
            reward = 0
        else:
            price = self.data.get_by_cursor(-1).price
            reward = op_reward.loc[row_data.timestamp - timedelta(minutes=1)].reward
        self.net_value_before = self.broker.get_account_status(price).net_value
        self.net_value_with_op_before = self.net_value_before + Decimal(reward)

    def after_bar(self, row_data: Union[RowData, pd.Series]):
        status_after = self.broker.get_account_status(row_data.price)
        return_rate = safe_div(status_after.net_value, self.net_value_before)

        self.operate(row_data)

        total_liq = 0
        for position in self.broker.positions.values():
            total_liq += position.liquidity
        net_value_with_op = status_after.net_value + Decimal(op_reward.loc[row_data.timestamp].reward)
        self.results.append(
            Results(return_rate, safe_div(net_value_with_op, self.net_value_with_op_before), net_value_with_op,
                    total_liq / row_data.currentLiquidity, total_liq,
                    status_after.base_uncollected + row_data.price * status_after.quote_uncollected,
                    status_after.base_balance + row_data.price * status_after.quote_balance))

    def modify_balance(self, position, price, removed):
        """
        在collect之后, 修正.
        1. 账户余额设置为0.
        2. 比较移除金额和头寸剩余金额, 如果剩余金额很少. 认为这些余额是手续费计算带来的误差. 头寸剩余金额变为0
        """
        self.broker.asset0.balance = Decimal(0)
        self.broker.asset1.balance = Decimal(0)

        if position in self.broker.positions:
            pos: Position = self.broker.positions[position]
            if self.broker._is_token0_base:
                base_amount, quote_amount = (pos.pending_amount0, pos.pending_amount1)
            else:
                base_amount, quote_amount = (pos.pending_amount1, pos.pending_amount0)
            # 比较剩余金额和移除金额.
            if removed == 0:
                return
            if (base_amount + quote_amount * price) / removed < 0.005:
                # 金额都设置为0, 去除误差对收益率的影响
                self.broker.positions[position].pending_amount0 = Decimal(0)
                self.broker.positions[position].pending_amount1 = Decimal(0)
                # if pos.liquidity < Decimal(10000):
                #     del self.broker.positions[position]
        pass

    def operate(self, row_data: RowData):
        if row_data.timestamp not in self._actions.index:
            return
        current_action: pd.DataFrame = self._actions.loc[[row_data.timestamp]]
        if row_data.timestamp == datetime(2023, 1, 4, 17, 28):
            current_action.to_csv(f"{config['work_folder']}xxx.csv")
        for index, action in current_action.iterrows():
            amount0_delta = from_unit(action.amount0, asset0)
            amount1_delta = from_unit(action.amount1, asset1)
            if self.broker._is_token0_base:
                base_amount, quote_amount = (amount0_delta, amount1_delta)
            else:
                base_amount, quote_amount = (amount1_delta, amount0_delta)
            try:
                match action.tx_type:
                    case "MINT":
                        self.broker.asset0.add(amount0_delta)
                        self.broker.asset1.add(amount1_delta)
                        ret = self.broker.add_liquidity_by_tick(action.tick_lower,
                                                                action.tick_upper,
                                                                base_amount,
                                                                quote_amount,
                                                                int(action.sqrtPriceX96))
                    case "COLLECT":
                        if amount0_delta == 0 and amount1_delta == 0:
                            continue
                        pos = PositionInfo(int(action.tick_lower), int(action.tick_upper))
                        ret = self.broker.collect_fee(pos,
                                                      max_collect_amount0=amount0_delta,
                                                      max_collect_amount1=amount1_delta,
                                                      remove_dry_pool=False)
                        self.broker.asset0.sub(amount0_delta, True)
                        self.broker.asset1.sub(amount1_delta, True)
                        removed_amount = base_amount + quote_amount * row_data.price
                        self.modify_balance(pos, row_data.price, removed_amount)

                    case "BURN":
                        if amount0_delta == 0 and amount1_delta == 0:
                            continue
                        if action.liquidity == 0:
                            continue
                        ret = self.broker.remove_liquidity(PositionInfo(int(action.tick_lower), int(action.tick_upper)),
                                                           liquidity=abs(action.liquidity),
                                                           collect=False,
                                                           sqrt_price_x96=int(action.sqrtPriceX96))
            except KeyError as e:
                traceback.print_exc()
                print(row_data.timestamp)
                exit(1)

        pass


def get_annual_return(duration_in_day, rate):
    return (365 / Decimal(duration_in_day)) * rate  # 加入如果大于-1就强制为-1的逻辑?


def print_return(data: pd.DataFrame):
    return_rate: pd.Series = data.return_rate
    multiple_return = 1
    return_rate_mul_list = [None] * len(data.index)
    price_rate_list = [None] * len(data.index)
    price_first = data.price.head(1).iloc[0]
    i = 0
    for index, row in data.iterrows():
        multiple_return *= row.return_rate
        return_rate_mul_list[i] = multiple_return
        price_rate_list[i] = row.price / price_first
        i += 1
    multiple_return -= 1
    time_span: Timedelta = return_rate.index[len(return_rate.index) - 1] - return_rate.index[0]
    time_span_day = time_span.total_seconds() / (60 * 60 * 24)
    annual_return = get_annual_return(time_span_day, multiple_return)
    standard_return = (data.price.tail(1).iloc[0] - data.price.head(1).iloc[0]) / data.price.head(1).iloc[0]
    annual_standard_return = get_annual_return(time_span_day, standard_return)
    print(f"收益率{multiple_return}, 年化收益率{annual_return}, "
          f"基准收益率{standard_return}, 基准年化收益率{annual_standard_return}")
    data["return_rate_mul"] = return_rate_mul_list
    data["price_rate"] = price_rate_list
    return data


def add_mint_for_null_position(actions: pd.DataFrame, broker: Broker):
    start: pd.Timestamp = actions["block_timestamp"].head(1).iloc[0]

    path = f"{data_path}/{ChainType.Optimism.name}-{config['pool']}-{start.strftime('%Y-%m-%d')}.csv"
    market_df: pd.DataFrame = pd.read_csv(path, converters={'inAmount0': decimal_from_value,
                                                            'inAmount1': decimal_from_value,
                                                            'netAmount0': decimal_from_value,
                                                            'netAmount1': decimal_from_value,
                                                            "currentLiquidity": decimal_from_value})
    market_first = market_df.iloc[0]
    first_actions = actions.groupby(by=["tick_lower", "tick_upper"]).first()
    no_mint_actions: pd.DataFrame = first_actions.loc[first_actions.tx_type != "MINT"]

    for index, row in no_mint_actions.iterrows():
        all_burn = actions.loc[
            (actions.tick_lower == index[0]) & (actions.tick_upper == index[1]) & (actions.tx_type == "BURN")]
        sum_liquidity = all_burn.liquidity.sum()
        all_mint = actions.loc[
            (actions.tick_lower == index[0]) & (actions.tick_upper == index[1]) & (actions.tx_type == "MINT")]
        sum_liquidity -= all_mint.liquidity.sum()
        current_tick = market_first.openTick
        current_sqrt_price = get_sqrt_ratio_at_tick(current_tick)
        token0_get, token1_get = V3CoreLib.close_position(pool, PositionInfo(index[0], index[1]), sum_liquidity,
                                                          current_sqrt_price)

        df1 = pd.DataFrame({
            "position_id": row.position_id,
            "tx_type": "MINT",
            "block_number": 0,
            "block_timestamp": pd.Timestamp(market_first.timestamp),
            "pool_log_index": 0,
            "tick_lower": index[0],
            "tick_upper": index[1],
            "liquidity": sum_liquidity,
            "current_tick": current_tick,
            "amount0": int(token0_get * (10 ** asset0.decimal)),
            "amount1": int(token1_get * (10 ** asset1.decimal)),
            "sqrtPriceX96": current_sqrt_price,
        }, index=[0])
        actions = actions.append(df1)
    return actions.sort_values(["block_number", "pool_log_index"])


def add_column(data: pd.DataFrame):
    return_rate: pd.Series = data.return_rate
    multiple_return = 1
    multiple_return_with_op = 1
    return_rate_mul_list = [None] * len(data.index)
    return_rate_with_op_mul_list = [None] * len(data.index)
    price_rate_list = [None] * len(data.index)
    price_first = data.price.head(1).iloc[0]
    i = 0
    for index, row in data.iterrows():
        multiple_return *= row.return_rate
        multiple_return_with_op *= row.return_rate_with_op
        return_rate_mul_list[i] = multiple_return
        return_rate_with_op_mul_list[i] = multiple_return_with_op
        price_rate_list[i] = row.price / price_first
        i += 1
    data["return_rate_mul"] = return_rate_mul_list
    data["price_rate"] = price_rate_list
    data["return_rate_with_op_mul"] = return_rate_with_op_mul_list
    return data


def run_backtest(actuator: Actuator):
    actual_actions: pd.DataFrame = \
        pd.read_csv(f'{config["work_folder"]}{config["name"]}.csv',
                    parse_dates=['block_timestamp'], dtype={'amount0': np.float64, 'amount1': np.float64},
                    converters={'sqrtPriceX96': decimal_from_value, 'liquidity': decimal_from_value})

    actual_actions = actual_actions.sort_values(["block_number", "pool_log_index"])

    actual_actions = add_mint_for_null_position(actual_actions, actuator.broker)
    actuator.strategy = ReplayStrategy(actual_actions)
    actuator.data_path = data_path
    start: pd.Timestamp = actual_actions["block_timestamp"].head(1).iloc[0]
    end: pd.Timestamp = actual_actions.block_timestamp.tail(1).iloc[0]

    actuator.load_data(ChainType.Optimism.name,
                       config["pool"],
                       start.to_pydatetime().date(),
                       end.to_pydatetime().date())
    actuator.run(enable_notify=False, evaluator=[])
    actuator.output()
    output_pd = pd.DataFrame(columns=Results.names(),
                             index=actuator.data.index,
                             data=map(lambda d: d.to_array(), actuator.strategy.results))
    output_pd["net_values"] = actuator.account_status.net_value
    output_pd["price"] = actuator.data.price
    # output_pd = pd.DataFrame(index=actuator.data.index,
    #                          data={"net_values": actuator.account_status.net_value,
    #                                "return_rate": actuator.data.return_rate,
    #                                "liquidity_share": actuator.data.share,
    #                                "liquidity": actuator.data.liquidity
    #
    #                                })
    output_pd = add_column(output_pd)
    output_pd.to_csv(f'{config["work_folder"]}{config["name"]}.result.csv')
    return output_pd


tokens = {
    "weth": TokenInfo(name="WETH", decimal=18),
    "dai": TokenInfo(name="DAI", decimal=18),
    "usdc": TokenInfo(name="USDC", decimal=6),
    "op": TokenInfo(name="OP", decimal=18)
}
pool_list = {
    "WETH-DAI": "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31",  # 0.3
    "USDC-DAI": "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6",  # 0.01
    # this pool is omitted, because there only a few of swap actions in this pool, so price is not accurate.
    # plus, this is the only pool without a stable coin, so calculate it's net_value will involve external  price list.
    "wstETH-ETH": "0x4a5a2a152e985078e1a4aa9c3362c412b7dd0a86",  # 0.05
    # "OP-USDC": "0x1c3140ab59d6caf9fa7459c6f83d4b52ba881d36",  # 0.3
    "OP-USDC": "0x1d751bc1a723accf1942122ca9aa82d49d08d2ae",  # 0.05
}

config = {
    "fund": "defiedge",
    "asset0": tokens["op"],
    "asset1": tokens["usdc"],
    "rate": 0.05,
    "is_0_base": False,
    "work_folder": "~/AA-labs-FE/05_op_reward_phase2/data-result/",
    "pool": pool_list[f'{tokens["op"].name}-{tokens["usdc"].name}']
}
config["name"] = f"{config['fund']}-{config['asset0'].name}-{config['asset1'].name}"
data_path = "~/AA-labs-FE/05_op_reward_phase2/data_minute"

reward_info = {
    "total": (25000 / 4),
    "start": datetime(2023, 1, 19, 0, 0, 0),
    "end": datetime(2023, 2, 9, 0, 0, 0)
}

if __name__ == "__main__":
    # global various
    if config["is_0_base"]:
        base = config["asset0"]
    else:
        base = config["asset1"]
    asset0 = config["asset0"]
    asset1 = config["asset1"]
    pool = PoolBaseInfo(config["asset0"], config["asset1"], config["rate"], base)
    op_reward: pd.DataFrame = pd.read_csv("~/AA-labs-FE/05_op_reward_phase2/data-aid/op_price_list.csv")
    op_reward["timestamp"] = pd.to_datetime(op_reward["timestamp"])
    op_reward.set_index("timestamp", inplace=True)
    op_reward["reward"] = 0
    op_reward.loc[reward_info["start"]:reward_info["end"], "reward"] = reward_info["total"] / (
            (reward_info["end"] - reward_info["start"]).total_seconds() / 60)
    actuator = Actuator(pool, allow_negative_balance=True)

    # do different work

    output_pd = run_backtest(actuator)

    pass

# 第三期的时候, 做一下op奖励,和手续费收入的占比