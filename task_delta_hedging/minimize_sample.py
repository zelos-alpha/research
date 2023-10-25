from scipy.optimize import minimize
import numpy as np


def func(x) -> float:
    return -(5 * (x[0] + 1) ** 2 + 8)



if __name__ == "__main__":
    """
    cons = ({'type': 'eq', 'fun': lambda x: x[0] * x[1] * x[2] - 1}, # xyz=1
        {'type': 'ineq', 'fun': lambda x: x[0] - e}, # x>=e，即 x > 0
        {'type': 'ineq', 'fun': lambda x: x[1] - e},
        {'type': 'ineq', 'fun': lambda x: x[2] - e}
       )
       """
    cons = ({"type": "ineq", "fun": lambda x: x[0] - (-100)}, {"type": "ineq", "fun": lambda x: -x[0] + 100})
    init_x = np.array((0,))
    # Method Nelder-Mead cannot handle constraints.
    res = minimize(func, init_x, method='SLSQP', constraints=cons)
    print(res)
