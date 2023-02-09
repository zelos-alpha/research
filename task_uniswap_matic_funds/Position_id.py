from datetime import datetime
import pandas as pd
import numpy as np
import config

### Run this file before "action_plot0.py"

# There are some abnormalities in the data of some addresses, such as holding positions for too long.
# Abnormal positions provide very low liquidity, so remove individual abnormal positions for clear images.
# Manually remove the exception when necessary, see the comments in the code.

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
# print(eth)
info = pd.read_csv('info.csv', sep=',', index_col=[], dtype=object, header=0)
info.index = info['name']


print(info.index)
# for name in ['a8']:
for name in info.index:

    receipt = info.loc[name, 'receipt']
    print('--------------------------------')
    print(name)

    if info.loc[name, 'type'] == 'fund':
        path = '01_init/'+receipt+'.csv'
        path_result = '01_init/'+receipt+"."+name+'.result.csv'
    else:
        path = '01_init/' + receipt + '.csv'
        path_result = '01_init/' + receipt + '.result.csv'
        # continue

    # read and reformat the tick data
    action = pd.read_csv(path, sep=',', index_col=[0], dtype=object, header=0)
    action = action[action.tx_type != 'COLLECT']
    action.loc[:, "block_timestamp"] = action.loc[:, "block_timestamp"].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    action['tick_id'] = action['tick_lower']+ '-' + action['tick_upper']
    action[['amount0', 'amount1', 'current_tick', "tick_lower", "tick_upper",'price','liquidity']] = action[
        ['amount0', 'amount1', 'current_tick', "tick_lower", "tick_upper",'price','liquidity']].astype(float)
    action[['current_tick', "tick_lower", "tick_upper"]] = action[['current_tick',"tick_lower", "tick_upper"]].astype('Int64')

    idx = action[(action.tx_type == 'BURN')&(action.liquidity == 0)].index.values
    action = action.drop(idx)

    action.loc[action['tx_type'].str.contains('BURN'), 'liquidity'] = action.loc[action['tx_type'].str.contains('BURN'), 'liquidity'] * (-1)

    action = action.sort_values(['block_timestamp','tx_type'], ascending=[True,False])
    action = action.reset_index()
    action_id = action.groupby('tick_id')

    idx = np.array([])
    for id, df in action_id:
        df['cum_liq'] = df['liquidity'].cumsum()
        print(id)
        # print(df.loc[:,['block_timestamp', 'tx_type', 'liquidity', 'cum_liq']])

        mint = np.array(df[df.tx_type == 'MINT'].index.values)

        if len(mint) == 0: # 0 mint: error
            print(name, id, 'no mint')
            continue
        #
        # if mint[0] == 2676:
        #     continue

        # try:
        #     df.loc[1329, 'cum_liq'] = 0
        #     burn0 = np.array(df[(df.tx_type == 'BURN') & (df.cum_liq <= 200)].index.values)
        # except:
        #     burn0 = np.array(df[(df.tx_type == 'BURN') & (df.cum_liq <=200)].index.values)

        burn0 = np.array(df[(df.tx_type == 'BURN') & (df.cum_liq <= 200)].index.values)
        burn0 = burn0[burn0>mint[0]]
        idx = np.append(idx, burn0)

        if (name == 'a8') & (id == '202470-202880'):
            burn0=[266]
        # elif (name == 'a3') & (id == '200840-200950'):
        #     burn0=[542]
        # elif (name == 'a3') & (id == '200850-200960'):
        #     burn0 = [507]
        if (name == 'Popsicle'):
            # if (id == '193770.0-196530.0'):
            #     burn0 = [160]
            if (id == '197130.0-205130.0') | (id == '201790.0-205240.0'):
                continue
        # elif name =='gamma':
        #     judge = df.loc[:,'liquidity'] < np.repeat(200, len(df))
        #     if judge.all():
        #         print('too small')
        #         continue


        if len(burn0) == 0: # 0 burn
            idx = np.append(idx, mint[0])
        else:
            count = 0
            for i in burn0:
                idx = np.append(idx, mint[0])
                action.loc[[mint[0], i], 'tick_id'] = action.loc[[mint[0], i], 'tick_id'] +\
                                                             '-' + str(count)
                mint = mint[mint>i]
                count += 1
            if mint.size > 0:
                idx = np.append(idx, mint[0])
                action.loc[mint[0], 'tick_id'] = action.loc[mint[0], 'tick_id'] + \
                                                             '-' + str(count)



    action = action.loc[idx,:]
    action.loc[:, 'price_upper'] = action.loc[:, 'tick_lower'].map(config.fun)
    action.loc[:, 'price_lower'] = action.loc[:, 'tick_upper'].map(config.fun)
    action.to_csv('01_init/'+name+'.csv')







