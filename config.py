from decimal import Decimal

import pandas as pd
import math


def _x96_to_decimal(number: int):
    return Decimal(number) / 2 ** 96


def decimal_to_x96(number: Decimal):
    return int(Decimal(number) * 2 ** 96)


def tick_to_sqrt_price_x96(ticker) -> int:
    return int(decimal_to_x96(Decimal((Decimal.sqrt(Decimal(1.0001))) ** ticker)))


def tick_to_quote_price(tick: int, token_0_decimal, token_1_decimal, is_token0_base: bool):
    sqrt_price = tick_to_sqrt_price_x96(tick)
    decimal_price = _x96_to_decimal(sqrt_price) ** 2
    pool_price = decimal_price * Decimal(10 ** (token_0_decimal - token_1_decimal))
    return Decimal(1 / pool_price) if is_token0_base else pool_price


def fun(tick):
    if pd.isna(tick):
        return tick
    else:
        return float(tick_to_quote_price(tick, decimal0, decimal1, base0))


def compute_slip(df, x="amount0", y="amount1"):
    # print(df, sep='\n')
    # if pd.isnull(df['past_price']):
    #     df['ept_slip'] = pd.NA
    #     print(df, end='\n')
    # else:
    #     if base0:
    #         x = 'amount0'
    #         y = 'amount1'
    #     else:
    #         x = 'amount1'
    #         y = 'amount0'
        if df[y] < 0:
            p = math.sqrt(Decimal(df['past_liquidity'])*df['past_price'])
            p = (p + df[y])**2 / df['past_liquidity']
        else:
            p = math.sqrt(Decimal(df['past_liquidity']) / df['past_price'])
            p = df['past_liquidity'] /(p + df[x]) ** 2
        # print(type(df['current_price']),type(p))
        df['ept_slip'] = (Decimal(p) - df['past_price']) / df['past_price']
        return df


def compute_mean_ratio(df):
    return (math.sqrt(df['price_lower']*df['price_upper']) / df["current_price"]) - 1




# Use 'get_liquidity' function to calculate liquidity as a function of amounts and price range
def get_liquidity0(sqrtA: Decimal, sqrtB: Decimal, amount0: Decimal, decimals: int) -> Decimal:
    if sqrtA > sqrtB:
        (sqrtA, sqrtB) = (sqrtB, sqrtA)

    liquidity = amount0 / ((2 ** 96 * (sqrtB - sqrtA) / sqrtB / sqrtA) / 10 ** decimals)
    return float(liquidity)


def get_liquidity1(sqrtA: Decimal, sqrtB: Decimal, amount1: Decimal, decimals: int) -> Decimal:
    if sqrtA > sqrtB:
        (sqrtA, sqrtB) = (sqrtB, sqrtA)

    liquidity = amount1 / ((sqrtB - sqrtA) / 2 ** 96 / 10 ** decimals)
    return float(liquidity)


def get_liquidity(tick: int, tickA: int, tickB: int, amount0: float, amount1: float) -> Decimal:
    global decimal0, decimal1
    amount0 = Decimal(amount0)
    amount1 = Decimal(amount1)
    sqrt = Decimal(1.0001 ** (tick / 2) * (2 ** 96))
    sqrtA = Decimal(1.0001 ** (tickA / 2) * (2 ** 96))
    sqrtB = Decimal(1.0001 ** (tickB / 2) * (2 ** 96))

    if sqrtA > sqrtB:
        (sqrtA, sqrtB) = (sqrtB, sqrtA)

    if sqrt <= sqrtA:
        liquidity0 = get_liquidity0(sqrtA, sqrtB, amount0, decimal0)
        return liquidity0
    elif sqrtB > sqrt > sqrtA:
        liquidity0 = get_liquidity0(sqrt, sqrtB, amount0, decimal0)
        liquidity1 = get_liquidity1(sqrtA, sqrt, amount1, decimal1)

        liquidity = liquidity0 if liquidity0 < liquidity1 else liquidity1
        return liquidity
    else:
        liquidity1 = get_liquidity1(sqrtA, sqrtB, amount1, decimal1)
        return liquidity1

def wavg(group, avg_name, weight_name):
    d = group[avg_name]
    w = group[weight_name]
    return (d*w).sum() / w.sum() 

def fun_size(log_lq):
    if log_lq <= 15:
        return 0
    elif log_lq <= 27:
        return 1
    else:
        return 2
    
# set info!!
decimal0 = 6  # USDC
decimal1 = 18
base0 = True  # tick is base on usdc?
