from scipy.optimize import minimize
import numpy as np


def func(x) -> float:
    V_UE = x[0]
    V_USDC = x[1]
    V_A = x[2]
    V_LE = x[3]
    return 0.08 * (V_UE + V_USDC) + 0.033 * V_A - 0.022 * V_LE # if max, use -func()


if __name__ == "__main__":
    H = 1.25 ** 2
    L = 0.8 ** 2
    uni_amount_con = (1 - 1 / H ** 0.5) / (1 - L ** 0.5)
    """
    cons = ({'type': 'eq', 'fun': lambda x: x[0] * x[1] * x[2] - 1}, # xyz=1
        {'type': 'ineq', 'fun': lambda x: x[0] - e}, # x>=e，即 x > 0
        {'type': 'ineq', 'fun': lambda x: x[1] - e},
        {'type': 'ineq', 'fun': lambda x: x[2] - e}
       )
       """
    cons = ({"type": "eq", "fun": lambda x: x[1] + x[2] == 1},
            {"type": "eq", "fun": lambda x: ((2 - 1 / H ** 0.5) * (x[0] + x[1]) - x[3]) == 0},
            {"type": "eq", "fun": lambda x: x[0] == uni_amount_con * x[1]},
            {"type": "ineq", "fun": lambda x: x[0] <= x[3] <= 0.825 * x[2]})
    init_x = np.array((0, 0, 0, 0))
    # Method Nelder-Mead cannot handle constraints.
    res = minimize(func, init_x, method='SLSQP', constraints=cons)
