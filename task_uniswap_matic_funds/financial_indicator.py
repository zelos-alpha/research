from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

import config

eth = pd.read_csv('eth.csv', sep=',', index_col=[0], dtype=object, header=0)
eth[['block_timestamp']] = eth[['block_timestamp']].astype(str)
eth[['price']] = eth[['price']].astype(float)
year_eth = (eth.loc[525599, 'price'] / eth.loc[0, 'price']) - 1

eth.loc[:, 'price_rate'] = eth.loc[:, 'price'].pct_change()
eth['price_rate'][0] = 0
eth_fluctuation = config.flut(eth, 'price_rate')
print(eth_fluctuation)

info = pd.read_csv('result1.csv', sep=',', index_col=[], dtype=object, header=0)
info.index = info['name']

# for name in ['a7']:
for name in info.index:
    receipt = info.loc[name, 'receipt']
    print(name)

    if info.loc[name, 'type'] == 'fund':
        path = '01_init/' + receipt + '.' + name + '.result.csv'
    else:
        path = '01_init/' + receipt + '.result.csv'
    result = pd.read_csv(path, sep=',', index_col=[], dtype=object, header=0)
    result.rename(columns={'Unnamed: 0': 'block_timestamp'}, inplace=True)
    result.loc[:, "block_timestamp"] = result.loc[:, "block_timestamp"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    result = result[result.block_timestamp >= '2022-01-01']
    result.index = result.iloc[:, 0]
    result[['return_rate', 'liquidity', 'net_values', 'price', 'return_rate_mul', 'price_rate']] = result[
        ['return_rate', 'liquidity', 'net_values', 'price', 'return_rate_mul', 'price_rate']].astype(float)
    result.loc[:, 'return_rate'] = result.loc[:, 'return_rate'] - 1
    result.loc[:, 'eth_return'] = result.loc[:, 'price'].pct_change()
    result.loc[:, 'eth_return'][0] = 0
    print(result.shape)
    print(result.index[0], "----", result.index[-1])
    # print(pd.isna(result).sum())

    # beta
    model = LinearRegression().fit(np.array(result['eth_return']).reshape(-1, 1), (result['return_rate']))
    beta = model.coef_

    # alpha
    day = (result['block_timestamp'][-1] - result['block_timestamp'][0]).days
    year_asset = (result['return_rate_mul'][-1] - 1) * 365 / day
    alpha = year_asset - (beta * year_eth)

    # fluctuation
    fluctuation = config.flut(result, 'return_rate')

    info.loc[name, 'day'] = day
    info.loc[name, 'end value'] = result['net_values'][-1]
    info.loc[name, 'liquidity change'] = result['liquidity'][-1] - result['liquidity'][0]
    info.loc[name, 'period return'] = result['return_rate_mul'][-1] - 1
    info.loc[name, 'yearly return'] = year_asset
    info.loc[name, 'eth yearly return'] = year_eth
    info.loc[name, 'beta'] = beta
    info.loc[name, 'alpha'] = alpha
    info.loc[name, 'fluctuation'] = fluctuation
    info.loc[name, 'eth fluctuation'] = eth_fluctuation

info.to_csv('result2.csv')
