import math
from datetime import datetime
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

### Run this file after "Position_id.py"

# There are some abnormalities in the data of some addresses, such as holding positions for too long.
# Abnormal positions provide very low liquidity, so remove individual abnormal positions for clear images.
# Manually remove the exception when necessary, see the comments in the code.

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

eth = pd.read_csv('eth.csv', sep=',', index_col=[0], dtype=object, header=0)
eth[['block_timestamp']] = eth[['block_timestamp']].astype(str)
eth[['price', '0']] = eth[['price', '0']].astype(float)
ro = 10
eth.loc[:, 'price2'] = eth.loc[:, 'price'].rolling(ro).mean()
eth.loc[0:ro, 'price2'] = eth.loc[0:ro, 'price']
# eth.loc[1,'price2'] = eth.loc[1,'price']
eth.loc[:, 'price_up'] = eth.loc[:, 'price2'] * 1.1
eth.loc[:, 'price_low'] = eth.loc[:, 'price2'] * 0.9
eth.loc[:, "block_timestamp"] = eth.loc[:, "block_timestamp"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))

info = pd.read_csv('info.csv', sep=',', index_col=[], dtype=object, header=0)
info.index = info['name']

# print(info.index)
# for name in ['polycat']:
for name in info.index:
# for name in ['Popsicle','Unipilot','dHEDGE', 'polycat', 'a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9']:

    receipt = info.loc[name, 'receipt']
    print(name)
    if info.loc[name, 'type'] == 'fund':
        path = '01_init/' + name + '.csv'
        path_result = '01_init/' + receipt + "." + name + '.result.csv'
    else:
        path = '01_init/' + name + '.csv'
        path_result = '01_init/' + receipt + '.result.csv'

    action = pd.read_csv(path, sep=',', index_col=[], dtype=object, header=0)
    action = action[action.tx_type != 'COLLECT']
    action.loc[:, "block_timestamp"] = action.loc[:, "block_timestamp"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    action = action[action.block_timestamp >= datetime.strptime('2022-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")]
    action.loc[:, "block_timestamp"] = action.loc[:, "block_timestamp"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M"))
    action = action.sort_values(['block_timestamp', 'tx_type'], ascending=[True, False])
    action[['price_lower', 'price_upper', 'liquidity']] = action[['price_lower', 'price_upper', 'liquidity']].astype(
        float)

    result = pd.read_csv(path_result, sep=',', index_col=[], dtype=object, header=0)
    result.rename(columns={'Unnamed: 0': 'block_timestamp'}, inplace=True)
    result.loc[:, "block_timestamp"] = result.loc[:, "block_timestamp"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    result = result[result.block_timestamp > '2022-01-01']
    result[['liquidity']] = result[['liquidity']].astype(float)
    result["log_lq"] = result["liquidity"].apply(lambda x: math.log((x + 1), 10))

    fig, axs = plt.subplots(3, 1, sharex=True, height_ratios=[0.7, 0.15, 0.15], figsize=(20, 10))
    fig.subplots_adjust(hspace=0)
    fig.suptitle(name + ' action', size=30)
    axs[0].set_ylabel('price')
    eth_temp = eth[(eth.block_timestamp >= result['block_timestamp'].min()) & (
                eth.block_timestamp <= result['block_timestamp'].max())]
    axs[0].plot(eth_temp['block_timestamp'], eth_temp['price2'], label='ETH Price', alpha=0.5)

    if (name != 'a2') & (name != 'polycat'):
        axs[0].plot(eth_temp['block_timestamp'], eth_temp['price_low'], label='10% range', color='darkgrey', alpha=.25)
        axs[0].plot(eth_temp['block_timestamp'], eth_temp['price_up'], label='10% range', color='darkgrey', alpha=.25)
        axs[0].legend()

    action_id = action.groupby('tick_id')
    for id, df in action_id:
        print()
        if (id == '349660-349980-0') & (name == 'a8'):
            continue
        elif (id == '202470-202880') & (name == 'a8'):
            continue
        elif (id == '200940-200950-0') & (name == 'polycat'):
            continue
        # elif (id == '202440-202640-0') & (name == 'a8'):
        #     continue
        # if id == '201790.0-205240.0':
        #     continue

        a = 0
        try:
            idx = df[df.tx_type == "MINT"].index[0]
            start_hour = action.loc[idx, 'block_timestamp']
        except:
            print(name, id, "no mint")
            start_hour = result['block_timestamp'].min()

        try:
            idx = df[df.tx_type == "BURN"].index[0]
            end_hour = action.loc[idx, 'block_timestamp']
        except:
            print(name, id, "no burn")
            end_hour = result['block_timestamp'].max()
            # end_hour = '2022-12-31 23:59'

        try:
            price_up = df.loc[df[df.tx_type == "MINT"].index[0], 'price_upper']
            price_low = df.loc[df[df.tx_type == "MINT"].index[0], 'price_lower']
        except:
            price_up = df.loc[df[df.tx_type == "BURN"].index[0], 'price_upper']
            price_low = df.loc[df[df.tx_type == "BURN"].index[0], 'price_lower']

        dates = pd.date_range(start_hour, end_hour, freq='1T').to_list()
        num = len(dates)

        # if price_up > 24000:
        #     up_array = np.repeat(24000,num)
        if price_up > 5000:
            up_array = np.repeat(5000, num)
            print(name, id, price_up, "out of range")
        else:
            up_array = np.repeat(price_up, num)
        low_array = np.repeat(price_low, num)

        print(dates[0], dates[-1], price_up, price_low)
        # for positions with a small market making range and short time period.
        if len(up_array) == 1:
            axs[0].vlines(dates, low_array, up_array, linewidth=3, colors='r', alpha=1)
        elif (price_up - price_low) <= 2:
            axs[0].hlines(np.array([price_up, price_low]), dates[0], dates[-1], linewidth=5, colors='r', alpha=1)
        else:
            axs[0].fill_between(dates, up_array, low_array, interpolate=True, step='mid', alpha=.25)

    axs[1].plot(eth_temp['block_timestamp'], eth_temp['0'])
    axs[1].set_ylabel('fluctuation')
    axs[1].set_xlabel('time')

    axs[2].plot(result['block_timestamp'], result['log_lq'])
    axs[2].set_ylabel('log liquidity')
    axs[2].set_xlabel('time')

    plt.xticks(rotation=30)

    plt.savefig('strategy plot/' + name + ' strategy.png', bbox_inches='tight')
    # plt.show()
