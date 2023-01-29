import traceback
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Union

import numpy as np
import pandas as pd
from pandas import Timedelta

from demeter import Strategy, TokenInfo, PoolBaseInfo, RowData, \
    Actuator, ChainType, PositionInfo, Position

pd.options.display.max_columns = None
pd.options.display.max_rows = 12
pd.set_option('display.width', 5000)
pd.options.display.max_colwidth = 100


@dataclass
class Results:
    return_rate: Decimal
    share: Decimal
    liquidity: Decimal
    uncollect: Decimal
    balance: Decimal

    @staticmethod
    def names():
        return ["return_rate", "share", "liquidity", "uncollect", "balance"]

    def to_array(self):
        return [self.return_rate, self.share, self.liquidity, self.uncollect, self.balance]


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

    def before_bar(self, row_data: Union[RowData, pd.Series]):
        if row_data.row_id < 1:
            price = row_data.price
        else:
            price = self.data.get_by_cursor(-1).price
        self.net_value_before = self.broker.get_account_status(price).net_value

    def after_bar(self, row_data: Union[RowData, pd.Series]):
        status_after = self.broker.get_account_status(row_data.price)
        return_rate = safe_div(status_after.net_value, self.net_value_before)

        self.operate(row_data)

        total_liq = 0
        for position in self.broker.positions.values():
            total_liq += position.liquidity
        self.results.append(
            Results(return_rate, total_liq / row_data.currentLiquidity, total_liq,
                    status_after.base_uncollected + row_data.price * status_after.quote_uncollected,
                    status_after.base_balance + row_data.price * status_after.quote_balance))

    def modify_balance(self, position, price, removed):
        """
        After collect, modify balance.
        1. set user account balance to zero, this means user withdraw their money from borker after collect. 
        2. compare withdraw amount and uncollected balance, if uncollected balance is much less than withdraw amount, we consider those uncollected amount is error of fee. so we set uncollect amount to zero. We do this because we don't want fee error affect net value.
        """
        self.broker.asset0.balance = Decimal(0)
        self.broker.asset1.balance = Decimal(0)

        if position in self.broker.positions:
            pos: Position = self.broker.positions[position]
            if self.broker._is_token0_base:
                base_amount, quote_amount = (pos.pending_amount0, pos.pending_amount1)
            else:
                base_amount, quote_amount = (pos.pending_amount1, pos.pending_amount0)
            # compare withdraw amount and uncollect balance
            if removed == 0:
                return
            if (base_amount + quote_amount * price) / removed < 0.005:
                self.broker.positions[position].pending_amount0 = Decimal(0)
                self.broker.positions[position].pending_amount1 = Decimal(0)
                # if you want to speed up backtest, uncommect this. this will remove positions whose liquidity is very low. this is introduced by fee error
                # if pos.liquidity < Decimal(10000):
                #     del self.broker.positions[position]
        pass

    def operate(self, row_data: RowData):
        if row_data.timestamp not in self._actions.index:
            return
        current_action: pd.DataFrame = self._actions.loc[[row_data.timestamp]]
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
                                                      remove_dry_pool=True) # delete positions whose liquidity is zero
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
    return (365 / Decimal(duration_in_day)) * rate  


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
    print(f"return rate: {multiple_return}, annual return rate{annual_return}, "
          f"benchmark return rate{standard_return}, annual benchmark return rate{annual_standard_return}")
    data["return_rate_mul"] = return_rate_mul_list
    data["price_rate"] = price_rate_list
    return data


def run_backtest(actuator: Actuator):
    actual_actions = choose_tx_from_csv()

    actual_actions = actual_actions.sort_values(["block_number", "pool_log_index"])

    actuator.strategy = ReplayStrategy(actual_actions)
    start: pd.Timestamp = actual_actions["block_timestamp"].head(1).iloc[0]
    end: pd.Timestamp = actual_actions.block_timestamp.tail(1).iloc[0]
    actuator.data_path = "~/AA-labs-FE/code/demeter/data"
    actuator.load_data(ChainType.Polygon.name,
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
    # save result, if you want
    # output_pd = pd.DataFrame(index=actuator.data.index,
    #                          data={"net_values": actuator.account_status.net_value,
    #                                "return_rate": actuator.data.return_rate,
    #                                "liquidity_share": actuator.data.share,
    #                                "liquidity": actuator.data.liquidity
    #                                })
    first = True
    for index, row in output_pd.iterrows():
        if first and row.liquidity != 0:
            start_index = index
            break
    for index, row in output_pd.reindex(index=output_pd.index[::-1]).iterrows():
        if first and row.liquidity != 0:
            end_index = index
            break


    output_pd = output_pd.loc[start_index:end_index]
    output_pd.to_csv(f'{config["work_folder"]}{config["user"]}.result.csv')
    return output_pd


def choose_tx_from_csv():
    """
    read action from origial csv file. because when import csv to mysql, big integer will be converted to double. such as sqrtpricex96
    """
    choosen_tx: pd.DataFrame = pd.read_csv(f"{config['work_folder']}{config['user']}.csv")
    choosen_tx["key"] = choosen_tx.apply(lambda x: x.transaction_hash + "-" + str(x.pool_log_index), axis=1)
    choosen_tx: pd.Series = choosen_tx["key"]
    all_tx: pd.DataFrame = pd.read_pickle("~/AA-labs-FE/code/research/data_tick/merge1.pkl")
    all_tx = all_tx.loc[all_tx['key'].isin(choosen_tx)]
    all_tx["block_timestamp"]= pd.to_datetime(all_tx['block_timestamp'])
    return all_tx


config = {"user": "0x28d1c71f83c1a138336f98b929bfd44e54114a8b",  # 0x0b0901e9cef9eaed5753519177e3c7cfd0ef96ef
          "asset0": TokenInfo(name="usdc", decimal=6),
          "asset1": TokenInfo(name="weth", decimal=18),
          "rate": 0.05,
          "is_0_base": True,
          "work_folder": "~/AA-labs-FE/03_fund_replay/01_init/",
          "pool": "0x45dda9cb7c25131df268515131f647d726f50608"}

if __name__ == "__main__":
    # global various
    if config["is_0_base"]:
        base = config["asset0"]
    else:
        base = config["asset1"]
    asset0 = config["asset0"]
    asset1 = config["asset1"]
    pool = PoolBaseInfo(config["asset0"], config["asset1"], config["rate"], base)

    actuator = Actuator(pool, allow_negative_balance=True)

    # do different work

    output_pd = run_backtest(actuator)

    # bk_result = pd.read_csv(
    #     f'{config["work_folder"]}{config["user"]}.result.csv',
    #     parse_dates=['Unnamed: 0'])
    # output_pd = bk_result.set_index("Unnamed: 0")

    process_result: pd.DataFrame = print_return(output_pd)

    process_result.to_csv(f'{config["work_folder"]}{config["user"]}.result.csv')


    pass
