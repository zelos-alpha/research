from datetime import datetime

import pandas as pd
from matplotlib import pyplot as plt, ticker

eth = pd.read_csv('eth_day.csv', sep=',', index_col=[0], dtype=object, header=0)
eth[['eth_net']] = eth[['eth_net']].astype(float)

plt.figure(figsize=(20, 10))
plt.title("Net value all")
plt.plot(eth.index, eth['eth_net'], label='ETH', linestyle=':')

info = pd.read_csv('info.csv', sep=',', index_col=[], dtype=object, header=0)
info.index = info['name']
for name in ['a0', 'a1']:
    # for name in info.index:
    receipt = info.loc[name, 'receipt']
    print(name)
    if info.loc[name, 'type'] == 'fund':
        path = '01_init/' + receipt + '.' + name + '.result.csv'
    else:
        path = '01_init/' + receipt + '.result.csv'
        # continue
    result = pd.read_csv(path, sep=',', index_col=[], dtype=object, header=0)
    result.rename(columns={'Unnamed: 0': 'block_timestamp'}, inplace=True)
    result.index = result.iloc[:, 0]

    # Convert minute-level data to daily data
    try:
        result.loc[:, "block_timestamp"] = result.loc[:, "block_timestamp"].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    except:
        result.loc[:, "block_timestamp"] = result.loc[:, "block_timestamp"].apply(
            lambda x: datetime.strptime(x, "%Y/%m/%d %H:%M"))
    result.loc[:, 'day'] = result.loc[:, "block_timestamp"].apply(lambda x: x.strftime("%Y-%m-%d"))
    result = result[result.block_timestamp >= '2022/01/01']
    result = result.groupby('day')[['block_timestamp', 'return_rate_mul']].last()

    if name == 'a5':
        result = result[result.index > '2022-07-20']

    # Align initial value and ETH net value
    start_day = result.index[0]
    eth_index_start = eth.loc[start_day, 'eth_net']
    result[['return_rate_mul']] = result[['return_rate_mul']].astype(float)
    result.loc[:, 'return_rate_mul'] = result.loc[:, 'return_rate_mul'] * eth_index_start

    plt.plot(result.index, result['return_rate_mul'], label=name)

plt.xticks(rotation=30)
ax0 = plt.gca()
ax0.xaxis.set_major_locator(ticker.MultipleLocator(30))
plt.legend()
plt.show()
