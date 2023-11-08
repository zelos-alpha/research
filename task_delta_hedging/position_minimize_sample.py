from scipy.optimize import minimize
import numpy as np
import matplotlib.pyplot as plt

"""
没用了, 公式推导有问题, 当个demo看看就行了. 
"""

H = 1.25 ** 2
L = 0.8 ** 2

r_U = 0.08  # return rate of uniswap
r_A = 0.033  # return rate of support usdc
r_LE = 0.022  # return rate of lend eth
r_E = 0.0051  # return rate of support eth


def func(x) -> float:
    V_UE = x[0]
    V_USDC = x[1]
    V_A = x[2]
    V_LE = x[3]
    return -(r_U * (V_UE + V_USDC) + r_A * V_A - r_LE * V_LE + r_E * (V_LE - V_UE))  # if max, use -func()


def get_best_param():
    uni_amount_con = (1 - 1 / H ** 0.5) / (1 - L ** 0.5)

    cons = ({"type": "eq", "fun": lambda x: x[1] + x[2] - 1},  # func() == 0
            {"type": "eq", "fun": lambda x: ((2 - 1 / H ** 0.5) * (x[0] + x[1]) - x[3])},
            {"type": "eq", "fun": lambda x: x[0] - uni_amount_con * x[1]},
            {"type": "ineq", "fun": lambda x: x[3] - x[0]},  # func() >= 0
            {"type": "ineq", "fun": lambda x: 0.825 * x[2] - x[3]})
    init_x = np.array((0, 0, 0, 0))
    # Method Nelder-Mead cannot handle constraints.
    res = minimize(func, init_x, method='SLSQP', constraints=cons)
    return res


def get_price_value(V_UE, V_USDC, V_A, V_LE):
    liq = V_USDC / (H ** 0.5 - L ** 0.5)
    p = np.arange(1 / 3, 3, 0.01)
    ret = r_U * (p * liq * (1 / p ** 0.5 - 1 / H ** 0.5) + liq * (p ** 0.5 - L ** 0.5)) + r_A * V_A + p * (r_E * (V_LE - V_UE) - r_LE * V_LE)
    return p, ret


def get_uniswap_value(p):
    if p < L:
        return p * (1 / L ** 0.5 - 1 / H ** 0.5)
    elif L <= p <= H:
        return 2 * p ** 0.5 - L ** 0.5 - p / H ** 0.5
    else:
        return H ** 0.5 - L ** 0.5


if __name__ == "__main__":
    res = get_best_param()
    print(res)
    prices, return_rates = get_price_value(res.x[0], res.x[1], res.x[2], res.x[3])
    print(return_rates)
    plt.plot(prices, return_rates)
    plt.show()
