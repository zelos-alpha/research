import json
import os
import pickle

import pandas as pd

"""
step0: 合并uni proxy的transfer(通过demeter-fetch的rpc下载的). 并解析成csv
"""


def str2addr(v: str):
    return "0x" + v[26:]


if __name__ == "__main__":
    path = "/data/research_data/uni/polygon/matic-proxy-transfer"
    files = os.listdir(path)
    files = list(filter(lambda x: x.endswith("tmp.pkl"), files))
    all_logs = []
    for f in files:
        with open(os.path.join(path, f), "rb") as fp:
            logs = pickle.load(fp)
            all_logs.extend(logs)
    for tl in all_logs:
        if "topics" in tl:
            topics = json.loads(tl["topics"])
        else:
            continue

        all_logs.append({
            'block_number': tl["block_number"],
            'transaction_hash': tl["transaction_hash"],
            'transaction_index': tl["transaction_index"],
            'log_index': tl["log_index"],
            'from': str2addr(topics[1]),
            'to': str2addr(topics[2]),
            'position_id': int(topics[3], 16)
        })
    df = pd.DataFrame(all_logs)
    df = df.sort_values(["block_number", "transaction_index", "log_index"])
    df.to_csv(os.path.join(path, "joined.csv"), index=False)
