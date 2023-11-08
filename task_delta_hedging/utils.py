import matplotlib
from scipy.optimize import minimize
import numpy as np
from math import sqrt

matplotlib.use('TkAgg')
import matplotlib.pyplot as plt


def optimize_delta_netural(ph, pl, alpha, delta=0.0):
    uni_amount_con = (1 - 1 / ph ** 0.5) / (1 - pl ** 0.5)

    liq = 1 / (2 - 1 / sqrt(ph) - sqrt(pl))
    # solution x
    # V_U_A: value go into AAVE 0
    # V_U_init: init usdc , not go into AAVE 1
    # V_U_uni: usdc in uniswap 2
    # V_E_uni: eth in uniswap 3
    # V_E_lend: eth lend from aave 4
    # V_U_lend: some eth lend from aave exchange to usdc 5

    # V_U_A = x[0]
    # V_U_init = x[1]
    # V_U_uni = x[2]
    # V_E_uni = x[3]
    # V_E_lend = x[4]
    # V_U_lend = x[5]

    # add constrains
    cons = ({"type": "eq", "fun": lambda x: x[0] + x[1] - 1},  # V_U_A + V_U_init = 1
            {"type": "eq", "fun": lambda x: x[2] * uni_amount_con - x[3]},  # uniswap providing constrain
            {"type": "eq", "fun": lambda x: (1 - 1 / ph ** 0.5) * (x[2] + x[3]) * liq - (x[5] + x[3]) - delta},  # delta netural
            # ineq
            {"type": "ineq", "fun": lambda x: x[1] + x[5] - x[2]},  # amount relation for usdc
            {"type": "ineq", "fun": lambda x: x[4] - x[3]},  # amount relation for eth
            {"type": "ineq", "fun": lambda x: alpha * x[0] - x[5] - x[4]},  # relation for aave
            # all x >= 0
            {"type": "ineq", "fun": lambda x: x[0]},
            {"type": "ineq", "fun": lambda x: x[1]},
            {"type": "ineq", "fun": lambda x: x[2]},
            {"type": "ineq", "fun": lambda x: x[3]},
            {"type": "ineq", "fun": lambda x: x[4]},
            {"type": "ineq", "fun": lambda x: x[5]},
            )

    init_x = np.array((0, 0, 0, 0, 0, 0))
    # # Method Nelder-Mead cannot handle constraints.
    res = minimize(lambda x: -(x[2] + x[3]), init_x, method='SLSQP', constraints=cons)

    return res.fun, res.x


def unit_lp_value(ph, pl, p):
    liq = 1 / (2 - 1 / sqrt(ph) - sqrt(pl))
    if p < pl:
        return liq * p * (1 / sqrt(pl) - 1 / sqrt(ph))

    elif p > ph:
        return liq * (sqrt(ph) - sqrt(pl))

    return liq * (2 * sqrt(p) - sqrt(pl) - p / sqrt(ph))


def profolio_value(price, unishare, debtshare, eth_cash, collateral, ph, pl):
    return unishare * unit_lp_value(ph, pl, price) - (debtshare - eth_cash) * price + collateral


def proflio_pnl_plot(ph, pl, unishare, debtshare, eth_cash, collateral):
    price_ts = np.linspace(0.8, 1.2, 100)
    pnl_ts = [profolio_value(price, unishare, debtshare, eth_cash, collateral, ph, pl) for price in price_ts]

    # plot
    plt.plot(price_ts, pnl_ts)
    plt.show()


# polt pnl for different delta
def different_pnl_plot(ph, pl, alpha):
    # delta_list = np.linspace(-0.05,0.05,10)
    delta_list = [0]
    price_list = np.linspace(0.8, 1.2, 100)

    profolio_list = [optimize_delta_netural(ph, pl, alpha, delta) for delta in delta_list]

    for profolio in profolio_list:
        V_U_A, V_U_init, V_U_uni, V_E_uni, V_E_lend, V_U_lend = profolio[1]
        unishare = V_U_uni + V_E_uni
        debtshare = V_E_lend + V_U_lend
        eth_cash = V_E_lend - V_E_uni
        collateral = V_U_A
        pnl_ts = [profolio_value(price, unishare, debtshare, eth_cash, collateral, ph, pl) for price in price_list]
        plt.plot(price_list, pnl_ts)
    # plot lp value
    lp_value_ts = [unit_lp_value(ph, pl, price) for price in price_list]
    plt.plot(price_list, lp_value_ts)
    # transfer delta list to string list
    delta_list = [str(delta) for delta in delta_list]
    plt.legend(delta_list + ["lp value"])
    plt.show()


# plot one delta pnl with debt,v3lpvalue, separtely
def pnl_with_delta(ph, pl, alpha, delta):
    price_list = np.linspace(ph, pl, 100)

    proflio = optimize_delta_netural(ph, pl, alpha, delta)
    V_U_A, V_U_init, V_U_uni, V_E_uni, V_E_lend, V_U_lend = proflio[1]
    unishare = V_U_uni + V_E_uni
    debtshare = V_E_lend + V_U_lend
    eth_cash = V_E_lend - V_E_uni
    collateral = V_U_A

    debt_series = [-(debtshare - eth_cash) * price for price in price_list]
    lp_value_series = [unishare * unit_lp_value(ph, pl, price) for price in price_list]

    # pnl equals to debt + lp_value+collateral
    pnl_series = [debt + lp_value + collateral for debt, lp_value in zip(debt_series, lp_value_series)]

    # plot 3 series in three plot in one figure
    fig, axs = plt.subplots(3, 1)
    axs[0].plot(price_list, debt_series)
    axs[0].set_title('debt')
    axs[1].plot(price_list, lp_value_series, 'tab:orange')
    axs[1].set_title('lp value')
    axs[2].plot(price_list, pnl_series, 'tab:green')
    axs[2].set_title('pnl with delta = ' + str(delta))

    # show
    plt.show()


if __name__ == "__main__":
    #
    ph = 1.2
    pl = 0.8
    alpha = 0.825

    res = optimize_delta_netural(ph, pl, alpha)

    #
    liq = -res[0]
    V_U_A, V_U_init, V_U_uni, V_E_uni, V_E_lend, V_U_lend = res[1]
    print("V_U_A: ", V_U_A)
    print("V_U_init: ", V_U_init)
    print("V_U_uni: ", V_U_uni)
    print("V_E_uni: ", V_E_uni)
    print("V_E_lend: ", V_E_lend)
    print("V_U_lend: ", V_U_lend)
    print("value in uniswap: ", liq)

    # delta
    # different_pnl_plot(1.2, 0.8, 0.7)

    pnl_with_delta(1.2, 0.8, 0.7, 0)
