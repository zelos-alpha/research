import random
from enum import Enum
from typing import NamedTuple, List
from tqdm import tqdm
import requests

import pandas as pd

""" tag people transfer"""


class SpecialAddress(Enum):
    black_hole = "0x0000000000000000000000000000000000000000"


uni_pool_list = [
    "0x83abecf7204d5afc1bea5df734f085f2535a9976",
    "0x5d7b4563f794f52423181c8320cdd3bf8fdf55d7",
    "0xdf1cadd06a22b0a2680504fb6859c6aeb7df09d8",
    "0xe4beacc96c6f9261ed93027e67f07f22bebd5650",
    "0xe6d460f695f64d0fb84325c6aaeeb8e537796515",
    "0x82ab1958f58d667e0c9b06e7104bf42ecade0ee7",
]


class EventTopic(Enum):
    transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    uni_swap = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    uni_v2_swap = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
    uni_mint = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
    uni_burn = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"

    @staticmethod
    def uni() -> List[str]:
        return [EventTopic.uni_swap.value,
                EventTopic.uni_mint.value,
                EventTopic.uni_v2_swap.value,
                EventTopic.uni_burn.value]


def rule_to_addr(row: pd.Series) -> str:
    if len(rel_tx.index) < 0:
        return ""
    address = rel_tx.iloc[0]["to"]
    match_addr = address_tag_df[address_tag_df["address"] == address]

    return match_addr.iloc[0]["type"] if len(match_addr.index > 0) else ""


def rule_uniswap(row: pd.Series) -> str:
    uni_logs = rel_log[rel_log["topic0"].isin(uni_event_list)]
    if len(uni_logs) > 0:
        uni_options = uni_logs["topic0"].apply(lambda x: EventTopic(x).name)
        if len(uni_options) > 1:
            # return ";".join(list(uni_options))
            return "MEVBot"
        elif len(uni_options) > 0:
            return list(uni_options)[0]
        else:
            return ""
        # return ";".join(list(uni_options))
    else:
        return ""


class Rule(NamedTuple):
    level: int
    name: str
    matcher: lambda row: ""


rules_level0: List[Rule] = [
    Rule(0, "mint", lambda row: "mint" if row["from"] == SpecialAddress.black_hole.value else ""),
    Rule(0, "burn", lambda row: "burn" if row["to"] == SpecialAddress.black_hole.value else ""),
]
rules: List[Rule] = [
    # len(log)==1
    Rule(1, "transfer", lambda row: "transfer" if len(rel_log.index) == 1 and rel_log.iloc[0]["topic0"] == EventTopic.transfer.value else ""),
    Rule(2, "other", lambda row: "other" if len(rel_log.index) == 1 else ""),
    Rule(11, "by to address", rule_to_addr),  # by to address
    Rule(21, "uniswap", rule_uniswap),  # uniswap
    Rule(99999, "other", lambda row: "other")  # other
]


def decide_level0_type(row: pd.Series) -> str:
    for rule in rules_level0:
        result = rule.matcher(row)
        if result != "":
            return result
    return ""


def decide_type(row: pd.Series) -> str:
    rules.sort(key=lambda x: x.level)
    for rule in rules:
        result = rule.matcher(row)
        if result != "":
            return result
    return "other"


def simplify_tx_df(transfer_df: pd.DataFrame):
    transfer_df["to_del"] = False
    grouped = transfer_df.groupby("transactionHash", group_keys=False)
    to_del = []
    to_addr = {}
    with tqdm(total=grouped.ngroups, ncols=150) as pbar1:
        for tx_hash, tx_df in grouped:
            if len(tx_df) == 1:
                pbar1.update()
                continue
            # if tx_hash != "0x7e1df30db14a088d49f99c94c91d928cca31c5e65356892667a636bd2dbfee08":
            #     continue
            i0 = 0
            i1 = 1
            while i0 < len(tx_df.index):
                is_continue = False

                if tx_df.iloc[i0]["to_del"]:
                    i0 += 1
                    continue
                while i1 < len(tx_df.index):
                    if i0 == i1:
                        i1 += 1
                        continue
                    if (not tx_df.iloc[i1]["to_del"]) and tx_df.iloc[i0]["to"] == tx_df.iloc[i1]["from"] and tx_df.iloc[i0]["value"] == tx_df.iloc[i1]["value"]:
                        tx_df.loc[tx_df.index[i1], "to_del"] = True
                        tx_df.loc[tx_df.index[i0], "to"] = tx_df.iloc[i1]["to"]
                        to_del.append(tx_df.index[i1])  # as set value in dataframe directly will cause some issue, store value to external var
                        to_addr[tx_df.index[i0]] = tx_df.iloc[i1]["to"]
                        i1 = 0  # 从头搜索
                        is_continue = True
                        break
                    i1 += 1
                if is_continue:
                    i0 = 0
                    continue
                i0 += 1
            pbar1.update()
    transfer_df = transfer_df.drop(to_del)
    for k, v in to_addr.items():
        transfer_df.loc[k, "to"] = v
    transfer_df = transfer_df.drop(columns=["to_del"])
    return transfer_df


# 修改: 做一个规则引擎,  规则改成可配置, 可以分级别


if __name__ == "__main__":
    path = "/home/sun/AA-labs-FE/12_meme_research/0x7a58c0be72be218b41c608b7fe7c5bb630736c71.transfer.csv"
    print("loading transfer")
    transfers = pd.read_csv(path)
    print("loading logs")
    full_log_df = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/token_0x7a58c0be72be218b41c608b7fe7c5bb630736c71.log.full.csv")
    print("loading tx")
    full_tx_df = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/token_0x7a58c0be72be218b41c608b7fe7c5bb630736c71.tx.csv")

    address_tag_df = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/contract_addr_tag.csv")

    print("simplify tx")
    # if transfer in a tx is a->b, b->c, with same amount, will simplify to a-c
    # transfers = simplify_tx_df(transfers)
    # transfers.to_csv("/home/sun/AA-labs-FE/12_meme_research/0x7a58c0be72be218b41c608b7fe7c5bb630736c71.transfer.tmp.csv", index=False)
    transfers = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/0x7a58c0be72be218b41c608b7fe7c5bb630736c71.transfer.tmp.csv")
    tx_type_list = []
    uni_event_list = EventTopic.uni()
    with tqdm(total=len(transfers.index), ncols=150) as pbar:
        for index, transfer_row in transfers.iterrows():
            row_type = decide_level0_type(transfer_row)
            if row_type == "":
                same_block_log: pd.DataFrame = full_log_df[full_log_df["blockNumber"] == transfer_row["blockNumber"]]
                rel_log: pd.DataFrame = same_block_log[same_block_log["transactionIndex"] == transfer_row["transactionIndex"]]
                same_block_tx: pd.DataFrame = full_tx_df[full_tx_df["blockNumber"] == transfer_row["blockNumber"]]
                rel_tx: pd.DataFrame = same_block_tx[same_block_tx["transactionIndex"] == transfer_row["transactionIndex"]]
                row_type = decide_type(transfer_row)
            tx_type_list.append(row_type)
            pbar.update()
    transfers["type"] = tx_type_list
    transfers.to_csv("/home/sun/AA-labs-FE/12_meme_research/0x7a58c0be72be218b41c608b7fe7c5bb630736c71.transfer.type.csv", index=False)
