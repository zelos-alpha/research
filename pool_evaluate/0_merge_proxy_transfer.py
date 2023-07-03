import json
import os
import pickle
from tqdm import tqdm
import pandas as pd

"""
step0: 合并uniswap proxy的transfer(通过demeter-fetch的rpc下载的). 并解析成csv, 
得到uniswap nft token转账的列表(所有pool的)
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
    all_txes = []
    for tl in all_logs:
        if "topics" in tl:
            topics = json.loads(tl["topics"])
        else:
            # print(tl["transaction_hash"],tl["log_index"])
            continue # 由于原始文件被破坏造成的脏数据. 按理说不应该出现.

        all_txes.append({
            'block_number': tl["block_number"],
            'transaction_hash': tl["transaction_hash"],
            'transaction_index': tl["transaction_index"],
            'log_index': tl["log_index"],
            'from': str2addr(topics[1]),
            'to': str2addr(topics[2]),
            'position_id': int(topics[3], 16)
        })
    df = pd.DataFrame(all_txes)
    df = df.sort_values(["block_number", "transaction_index", "log_index"])
    df.to_csv(os.path.join(path, "joined.csv"), index=False)
