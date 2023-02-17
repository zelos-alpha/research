from datetime import date, datetime
from decimal import Decimal
from typing import Union

import numpy as np
import pandas as pd

from demeter import Strategy, TokenInfo, PoolBaseInfo, AtTimeTrigger, RowData, \
    Position, ActionTypeEnum, Actuator, Asset, ChainType, AccountStatus, PositionInfo

pd.options.display.max_columns = None
# pd.options.display.max_rows = None
pd.set_option('display.width', 5000)
pd.options.display.max_colwidth = 100


def decimal_from_value(value):
    return Decimal(value) if value != "" else Decimal(0)


def from_unit(value, token: TokenInfo):
    return Decimal(value) * Decimal(10 ** (-token.decimal))


class ReplayStrategy(Strategy):
    def __init__(self, strategy_actions):
        super().__init__()
        self._actions: pd.DataFrame = strategy_actions  # .head(17)

    def initialize(self):
        for index, action in self._actions.iterrows():
            match action.tx_type:
                case "MINT":
                    self.triggers.append(AtTimeTrigger(action.block_timestamp, self.add_at, action))
                case "COLLECT":
                    self.triggers.append(AtTimeTrigger(action.block_timestamp, self.collect_at, action))
                case "BURN":
                    # if action.liquidity == 0:
                    #     continue
                    self.triggers.append(AtTimeTrigger(action.block_timestamp, self.remove_at, action))

    def next(self, row_data: Union[RowData, pd.Series]):
        # 注意: 这个时候的净值, 没有经过mint/burn的影响
        if row_data.row_id - 1 < 0:
            return_rate = 1
        else:
            if actuator.account_status_list[row_data.row_id - 1].net_value == 0:
                return_rate = 1
            else:
                return_rate = self.broker.get_account_status(row_data.price).net_value / actuator.account_status_list[
                    row_data.row_id - 1].net_value
        self.data.loc[row_data.timestamp, "return_rate"] = return_rate
        keys = list(self.broker.positions.keys())
        total_liq = 0
        for position_key in keys:
            total_liq += self.broker.positions[position_key].liquidity
        self.data.loc[row_data.timestamp, "share"] = total_liq / row_data.currentLiquidity
        self.data.loc[row_data.timestamp, "liquidity"] = total_liq

    def add_at(self, row_data: RowData, *args, **kwargs):
        action = args[0]
        self.broker.asset0.add(from_unit(action.amount0, asset0))
        self.broker.asset1.add(from_unit(action.amount1, asset1))

        if self.broker._is_token0_base:
            base, quote = (from_unit(action.amount0, asset0), from_unit(action.amount1, asset1))
        else:
            base, quote = (from_unit(action.amount1, asset1), from_unit(action.amount0, asset0))
        ret = self.broker.add_liquidity_by_tick(action.tick_lower,
                                                action.tick_upper,
                                                base,
                                                quote,
                                                int(action.sqrtPriceX96))
        pass

    def remove_at(self, row_data: RowData, *args, **kwargs):
        action = args[0]
        if int(action.liquidity) == 0:
            return
        ret = self.broker.remove_liquidity(PositionInfo(int(action.tick_lower), int(action.tick_upper)),
                                           liquidity=abs(action.liquidity),
                                           collect=False,
                                           sqrt_price_x96=int(action.sqrtPriceX96))

    def collect_at(self, row_data: RowData, *args, **kwargs):
        action = args[0]
        amount0 = from_unit(action.amount0, asset0)
        amount1 = from_unit(action.amount1, asset1)
        ret = self.broker.collect_fee(PositionInfo(int(action.tick_lower), int(action.tick_upper)),
                                      max_collect_amount0=amount0,
                                      max_collect_amount1=amount1,
                                      remove_dry_pool=False)
        self.broker.asset0.sub(from_unit(action.amount0, asset0))
        self.broker.asset1.sub(from_unit(action.amount1, asset1))


akk_weth_usdc = {"name": "akk-weth_usdc",
                 "asset0": TokenInfo(name="weth", decimal=18),
                 "asset1": TokenInfo(name="usdc", decimal=6),
                 "rate": 0.05,
                 "is_0_base": False,
                 "from_file": "/home/sun/AA-labs-FE/code/research/data-uni-op/akk-weth_usdc.csv",
                 "pool": "0x85149247691df622eaf1a8bd0cafd40bc45154a9",
                 "start": date(2022, 10, 25),
                 "end": date(2022, 11, 14),
                 "op_each_day": 396.8}
akk_weth_dai = {"name": "akk-weth_dai",
                "asset0": TokenInfo(name="weth", decimal=18),
                "asset1": TokenInfo(name="dai", decimal=18),
                "rate": 0.3,
                "is_0_base": False,
                "from_file": "/home/sun/AA-labs-FE/code/research/data-uni-op/akk-weth_dai.csv",
                "pool": "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31",
                "start": date(2022, 10, 26),
                "end": date(2022, 11, 14),
                "op_each_day": 396.8}
xtoken_weth_usdc = {"name": "xtoken-weth_usdc",
                    "asset0": TokenInfo(name="weth", decimal=18),
                    "asset1": TokenInfo(name="usdc", decimal=6),
                    "rate": 0.05,
                    "is_0_base": False,
                    "from_file": "/home/sun/AA-labs-FE/code/research/data-uni-op/xtoken-weth_usdc.csv",
                    "pool": "0x85149247691df622eaf1a8bd0cafd40bc45154a9",
                    "start": date(2022, 10, 26),
                    "end": date(2022, 11, 14),
                    "op_each_day": 396.8}
gamma_weth_usdc = {"name": "gamma-weth_usdc",
                   "asset0": TokenInfo(name="weth", decimal=18),
                   "asset1": TokenInfo(name="usdc", decimal=6),
                   "rate": 0.05,
                   "is_0_base": False,
                   "from_file": "/home/sun/AA-labs-FE/code/research/data-uni-op/gamma-weth_usdc_1101.csv",
                   "pool": "0x85149247691df622eaf1a8bd0cafd40bc45154a9",
                   "start": date(2022, 11, 1),
                   "end": date(2022, 11, 14),
                   "op_each_day": 396.8}
config = akk_weth_dai


def load_result_and_calc():
    output_pd = pd.read_csv(f"/home/sun/AA-labs-FE/code/research/data-uni-op/{config['name']}-backtest.csv")
    total_rate = Decimal(1)
    for idx, row in output_pd.iterrows():
        total_rate *= Decimal(row.return_value)
    print("total return rate", config["name"], total_rate)


if __name__ == "__main__":
    actual_actions = pd.read_csv(config["from_file"],
                                 parse_dates=['block_timestamp'], dtype={
            'amount0': np.float64, 'amount1': np.float64,
        }, converters={'sqrtPriceX96': decimal_from_value, 'liquidity': decimal_from_value})

    if config["is_0_base"]:
        base = config["asset0"]
    else:
        base = config["asset1"]
    asset0 = config["asset0"]
    asset1 = config["asset1"]
    pool = PoolBaseInfo(config["asset0"], config["asset1"], config["rate"], base)

    actuator = Actuator(pool)
    actuator.strategy = ReplayStrategy(actual_actions)
    actuator.data_path = "/home/sun/AA-labs-FE/code/research/data-op-minute"
    actuator.load_data(ChainType.Optimism.name.lower(), config["pool"],
                       config["start"],
                       config["end"])
    actuator.run(enable_notify=True, enable_evaluating=False)
    actuator.output()

    output_pd = pd.DataFrame(index=actuator.data.index,
                             data={"net_values": actuator.account_status.net_value,
                                   "return_value": actuator.data.return_rate,
                                   "liquidity_share": actuator.data.share,
                                   "liquidity": actuator.data.liquidity
                                   })

    output_pd["op_reward"] = config["op_each_day"] / 1440 * 0.9
    print(output_pd)
    output_pd.to_csv(f"/home/sun/AA-labs-FE/code/research/data-uni-op/{config['name']}-backtest.csv")
    # load_result_and_calc()
    pass
