import json
from datetime import date, timedelta

import pandas as pd

import common.constants as constants
import common.types as comm_type
from tool_process.v2.preprocess import compare_burn_data

folder = "/home/sun/AA-labs-FE/05_op_reward_phase2/data/"
to_folder = "/home/sun/AA-labs-FE/05_op_reward_phase2/data-bq/"

topic_dict = {
    "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde": "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f",
    # mint
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67": None,  # swap
    "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c": "0x26f6a048e9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4",
    # burn
    "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0": "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01"
    # collect
}


def format_date(ddd: date):
    return ddd.strftime("%Y-%m-%d")


def process_topic(df):
    df["topic_array"] = df.apply(lambda x: json.loads(x.topics), axis=1)
    df["topic_name"] = df.apply(lambda x: x.topic_array[0], axis=1)
    return df


def load_proxy_data():
    #  headers : block_number,tx_hash,tx_index,log_index,data,topics
    # df = pd.read_csv("../data-op/lint.csv")
    df = pd.read_csv("/home/sun/AA-labs-FE/05_op_reward_phase2/data/0xc36442b4a4522e871399cd717abdd847ab11fe88.csv")
    df = df.set_index("tx_hash")
    df = process_topic(df)
    return df
    # return pd.read_pickle("../data-op/0xc36442b4a4522e871399cd717abdd847ab11fe88.pkl")


def add_proxy_log(df, index, proxy_row):
    df.loc[index, "proxy_data"] = proxy_row.data
    df.loc[index, "proxy_topics"] = proxy_row.topics
    df.loc[index, "proxy_log_index"] = proxy_row.log_index


def match_proxy_log(df: pd.DataFrame):
    for index, row in df.iterrows():
        if row.tx_type == comm_type.OnchainTxType.SWAP:
            continue
        if row.tx_hash not in proxy_data.index:
            continue
        proxy_tx: pd.DataFrame = proxy_data.loc[[row.tx_hash]]
        proxy_tx_matched: pd.DataFrame = proxy_tx.loc[proxy_tx.topic_name == topic_dict[row.topic_name]]

        for pindex, possible_match in proxy_tx_matched.iterrows():
            if row.tx_type == comm_type.OnchainTxType.MINT:
                if row.data[66:] == possible_match.data[2:]:
                    add_proxy_log(df, index, possible_match)
                    break
            elif row.tx_type == comm_type.OnchainTxType.COLLECT or row.tx_type == comm_type.OnchainTxType.BURN:
                if compare_burn_data(row.data, possible_match.data):
                    add_proxy_log(df, index, possible_match)
                    break
            else:
                raise ValueError("not support tx type")

    if "proxy_topics" not in df.columns:
        df["proxy_data"] = None
        df["proxy_topics"] = None
        df["proxy_log_index"] = None


pass


def format_df(df: pd.DataFrame):
    df = df.drop(["tx_type", "topics", "topic_name"], axis=1)
    df["proxy_topics"] = df.proxy_topics.apply(lambda x: json.loads(x) if isinstance(x, str) else x)

    df = df.rename(columns={
        "tx_hash": "transaction_hash",
        "timestamp": "block_timestamp",
        "log_index": "pool_log_index",
        "tx_index": "pool_tx_index",
        "data": "pool_data",
        "topic_array": "pool_topics",
    })

    return df


if __name__ == "__main__":
    contract = "0x4a5a2a152e985078e1a4aa9c3362c412b7dd0a86"
    start = date(2023, 1, 13)
    day = start
    proxy_data = load_proxy_data()
    print("load proxy finish")
    #         with tqdm(total=len(self._data.index), ncols=150) as pbar:
    while day < date(2023, 2, 9):
        logs = pd.read_csv(f"{folder}{contract}-{format_date(day)}.csv")
        logs = process_topic(logs)
        logs["tx_type"] = logs.apply(lambda x: constants.type_dict[x.topic_array[0]], axis=1)
        logs["timestamp"] = pd.to_datetime(logs["timestamp"])
        match_proxy_log(logs)
        logs = format_df(logs)
        logs.to_csv(f"{to_folder}{contract}-{format_date(day)}.csv", index=False)
        print(format_date(day))
        day = day + timedelta(days=1)
