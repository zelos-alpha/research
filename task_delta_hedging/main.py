import importlib.util
import sys
from datetime import date, datetime
from decimal import Decimal

import numpy as np
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.pylab import plt

from demeter.uniswap.helper import quote_price_to_sqrt
from task_delta_hedging.utils import optimize_delta_netural

spec = importlib.util.spec_from_file_location("demeter", "/home/sun/AA-labs-FE/code/demeter/demeter/__init__.py")
foo = importlib.util.module_from_spec(spec)
sys.modules["demeter"] = foo
spec.loader.exec_module(foo)

from demeter.aave import AaveV3Market
from demeter.uniswap import UniV3Pool, UniLpMarket, V3CoreLib
from demeter import ChainType, Strategy, TokenInfo, Actuator, MarketInfo, RowData, AtTimeTrigger
import pandas as pd

pd.options.display.max_columns = None
pd.set_option("display.width", 5000)

H = Decimal("1.2")
L = Decimal("0.8")
AAVE_POLYGON_USDC_ALPHA = Decimal("0.8")  # polygon usdc可以贷出资金的比例, 最大是0.825. 为了防止清算, 调整成0,8


class DeltaHedgingStrategy(Strategy):
    def __init__(self):
        super().__init__()
        optimize_res = optimize_delta_netural(float(H), float(L), float(AAVE_POLYGON_USDC_ALPHA))
        V_U_A, V_U_init, V_U_uni, V_E_uni, V_E_lend, V_U_lend = optimize_res[1]

        self.usdc_aave_supply = Decimal(V_U_A)
        self.usdc_uni_init = Decimal(V_U_init)
        self.usdc_uni_lp = Decimal(V_U_uni)
        self.eth_uni_lp = Decimal(V_E_uni)
        self.eth_aave_borrow = Decimal(V_E_lend)
        self.usdc_aave_borrow = Decimal(V_U_lend)
        print("V_U_A: ", self.usdc_aave_supply)
        print("V_U_init: ", self.usdc_uni_init)
        print("V_U_uni: ", self.usdc_uni_lp)
        print("V_E_uni: ", self.eth_uni_lp)
        print("V_E_lend: ", self.eth_aave_borrow)
        print("V_U_lend: ", self.usdc_aave_borrow)
        self.fee0 = []
        self.fee1 = []
        self.last_collect_fee0 = 0
        self.last_collect_fee1 = 0
        self.change_history = pd.Series()

    def initialize(self):
        new_trigger = AtTimeTrigger(time=datetime.combine(start_date, datetime.min.time()), do=self.change_position)
        self.triggers.append(new_trigger)

    def reset_funds(self):
        # withdraw all positions

        mb = market_uni.get_market_balance()

        self.last_collect_fee0 += mb.base_uncollected
        self.last_collect_fee1 += mb.quote_uncollected
        market_uni.remove_all_liquidity()
        for b_key in market_aave.borrow_keys:
            swap_amount = market_aave.get_borrow(b_key).amount - broker.assets[eth].balance
            if swap_amount > 0:
                market_uni.buy(swap_amount * (1 + market_uni.pool_info.fee_rate))
            market_aave.repay(b_key)
        for s_key in market_aave.supply_keys:
            market_aave.withdraw(s_key)
        market_uni.sell(broker.assets[eth].balance)

    def change_position(self, row_data: RowData):
        self.reset_funds()

        pos_h = H * row_data.prices[eth.name]
        pos_l = L * row_data.prices[eth.name]

        total_cash = self.get_cash_net_value(row_data.prices)

        # work
        self.last_aave_supply_value = total_cash * self.usdc_aave_supply
        self.last_aave_borrow_value = self.last_aave_supply_value * AAVE_POLYGON_USDC_ALPHA

        market_aave.supply(usdc, self.last_aave_supply_value)
        market_aave.borrow(eth, self.last_aave_borrow_value / row_data.prices[eth.name])

        self.last_net_value = total_cash

        market_uni.sell(self.usdc_aave_borrow * total_cash / row_data.prices[eth.name])  # eth => usdc

        market_uni.add_liquidity(pos_l, pos_h)

        # result monitor
        print("!!!!", row_data.timestamp)
        self.change_history.loc[row_data.timestamp] = "changed"
        pass

    def get_cash_net_value(self, price: pd.Series):
        return Decimal(sum([asset.balance * price[asset.name] for asset in broker.assets.values()]))

    def get_current_net_value(self, price):
        cash = self.get_cash_net_value(price)
        lp_value = 0
        sqrt_price = quote_price_to_sqrt(price[eth.name], market_uni.pool_info.token0.decimal, market_uni.pool_info.token1.decimal, market_uni.pool_info.is_token0_base)
        for pos_key, pos in market_uni.positions.items():
            amount0, amount1 = V3CoreLib.get_token_amounts(market_uni.pool_info, pos_key, sqrt_price, pos.liquidity)
            lp_value += amount0 * price[usdc.name] + amount1 * price[eth.name]

        return cash + self.last_aave_supply_value - self.last_aave_borrow_value + lp_value

    def on_bar(self, row_data: RowData):
        if not self.last_net_value * Decimal("0.98") < self.get_current_net_value(row_data.prices) < self.last_net_value * Decimal(
                "1.02"):  # 如果本金的价值(不考虑uni和aave的手续费), 变动到原来的0.98, 调仓重来.
            self.change_position(row_data)
        pass

    def after_bar(self, row_data: RowData):
        mb = market_uni.get_market_balance()
        self.fee0.append(mb.base_uncollected + self.last_collect_fee0)
        self.fee1.append(mb.quote_uncollected + self.last_collect_fee1)


def plot(balance_history_df: pd.DataFrame, net_value_columns=[]):
    fig: Figure = plt.figure()
    ax_value: Axes = fig.add_subplot()
    for nvc in net_value_columns:
        ax_value.plot(balance_history_df.index, balance_history_df[nvc], '-', label=nvc)

    ax_value.set_xlabel("Time")
    balance_history_df["num_index"] = np.arange(0, len(balance_history_df.index))
    change_timestamp = df[df["change"].notnull()]
    ax_price: Axes = ax_value.twinx()
    ax_price.plot(balance_history_df.index, balance_history_df["price"], '-rd', label="price", markevery=change_timestamp["num_index"])

    ax_value.legend(loc=0)
    ax_price.legend(loc=0)

    ax_price.set_ylim(0, 1900)

    plt.show()
    pass


if __name__ == "__main__":
    """
    主要流程
    1. 准备配置, 开始循环. 
    2. 在测试开始, 开始个流程.
    3. 如果本金的价值(不考虑uni和aave的手续费), 变动到原来的0.98, 调仓重来. 
    4. 如果发生了清算, 重来.
    
    每次投资的流程. 
    1. 给定上下界, 求解最优化公式, 得到资金比例
    2. 按照比例分配资金, 存钱, 借钱, 然后swap, 投lp
    
    注意: 
    * 上下界选择当时价格的20%(搞个可配置)
    * 调仓太频繁了怎么办
    * 添加lp的时候, 看一下预估资金和实际的差值, 如果太大, 报警退出, 调整模型 
    * swap的手续费暂且忽略, 不过好像忽略不成.
    """
    RUN_BACKTEST = False
    if RUN_BACKTEST:
        start_date = date(2023, 8, 1)
        end_date = date(2023, 10, 1)
        # start_date = date(2023, 8, 14)
        # end_date = date(2023, 8, 17)
        market_key_uni = MarketInfo("uni")
        market_key_aave = MarketInfo("aave")

        usdc = TokenInfo(name="usdc", decimal=6, address="0x2791bca1f2de4661ed88a30c99a7a9449aa84174")  # TokenInfo(name='usdc', decimal=6)
        eth = TokenInfo(name="weth", decimal=18, address="0x7ceb23fd6bc0add59e62ac25578270cff1b9f619")  # TokenInfo(name='eth', decimal=18)
        pool = UniV3Pool(usdc, eth, 0.05, usdc)
        actuator = Actuator()
        broker = actuator.broker

        market_uni = UniLpMarket(market_key_uni, pool)  # uni_market:UniLpMarket, positions: 1, total liquidity: 376273903830523
        market_uni.data_path = "/data/uniswap/polygon/usdc_weth_005"
        market_uni.load_data(ChainType.polygon.name, "0x45dda9cb7c25131df268515131f647d726f50608", start_date, end_date)
        broker.add_market(market_uni)  # add market

        market_aave = AaveV3Market(market_key_aave, "/home/sun/AA-labs-FE/code/demeter/tests/aave_risk_parameters/polygon.csv", [usdc, eth])
        market_aave.data_path = "/data/aave/polygon"
        market_aave.load_data(ChainType.polygon, [usdc, eth], start_date, end_date)
        broker.add_market(market_aave)  # add market

        broker.set_balance(usdc, 1000)  # set balance
        broker.set_balance(eth, 0)  # set balance

        actuator.strategy = DeltaHedgingStrategy()
        actuator.set_price(market_uni.get_price_from_data())

        actuator.run()
        df = actuator.get_account_status_dataframe()
        df["change"] = actuator.strategy.change_history
        df["price"] = actuator.token_prices[eth.name]
        df["accum_fee0"] = actuator.strategy.fee0
        df["accum_fee1"] = actuator.strategy.fee1
        df.to_csv("./account.csv")
        actuator.save_result(path="./", account=False, actions=True)

    df = pd.read_csv("./account.csv", index_col=0, parse_dates=True)
    df["uncollected_sum"] = df["accum_fee1"] * df["price"] + df["accum_fee0"]

    plot(df, ["net_value", "uni_net_value", "aave_borrows_value", "aave_supplies_value"])
    plot(df, ["uncollected_sum"])
'''
绘图
* 时间横轴
* 纵轴包括总净值, 价格, 
* 事件打上点. 包括调仓, 清算. 
'''
