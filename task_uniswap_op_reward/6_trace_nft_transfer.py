import pandas as pd
import toy_common

# useless, as trace nft transfer is not a good idea,
# it only useful for the first mint, add append mint doesn't have nft transfer

def find_uniswap_nft_transfer():
    """
    分析uniswap erc721 transfer to, 找到头寸id和用户地址的对应关系

    :return:
    :rtype:
    """
    logs = pd.read_csv("../data-op/0xc36442b4a4522e871399cd717abdd847ab11fe88-transfer.csv")
    logs["topic_array"] = logs.apply(lambda x: toy_common.split_topic(x.topics), axis=1)
    logs["from_addr"] = logs.apply(lambda x: "0x" + x.topic_array[1][26:], axis=1)
    logs["to_addr"] = logs.apply(lambda x: "0x" + x.topic_array[2][26:], axis=1)
    logs["position_id"] = logs.apply(lambda x: int(x.topic_array[3], 16), axis=1)
    mapping: pd.DataFrame = logs.loc[:, ["position_id", "from_addr", "to_addr", "block_number", "tx_hash", "log_index"]]
    mapping = mapping.sort_values(["position_id"])
    mapping.to_csv("../data-op-bq-tick/address-position.csv", index=False)


def merge_mint_and_nft_transfer():
    """
    把find_uniswap_nft_transfer中,找到的erc721 transfer, 和mint交易对应起来.
    :return:
    :rtype:
    """
    # contract = "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31"
    # contract = "0x85149247691df622eaf1a8bd0cafd40bc45154a9"
    contract = "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6"
    transfers = pd.read_csv("../data-op-bq-tick/address-position.csv")
    pool_op: pd.DataFrame = pd.read_csv(f"../data-op-bq-tick/{contract}-merged.csv")
    for idx, pool_row in pool_op.iterrows():
        if pool_row["tx_type"] != "MINT":
            continue
        mint_transfer = transfers.loc[transfers["tx_hash"] == pool_row["transaction_hash"]]
        if len(mint_transfer.index) == 1:
            pool_op.loc[idx, "receipt"] = mint_transfer.iloc[0]["to_addr"]
            # print(len(mint_transfer.index), mint_transfer.iloc[0]["position_id"], mint_transfer.iloc[0]["to_addr"])
    pool_op.to_csv(f"../data-op-bq-tick/{contract}-merged1.csv", index=False)


if __name__ == "__main__":
    # find_uniswap_nft_transfer()
    merge_mint_and_nft_transfer()
