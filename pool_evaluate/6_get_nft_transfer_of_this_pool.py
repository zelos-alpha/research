import pandas as pd
from tqdm import tqdm

"""

6 查找uniswap nft转账的情况, 这个转账, 会对地址净值的计算有影响(因为流动性给其他人了)
一共才72个. 决定不处理这些情况了. 

"""


if __name__ ==   "__main__":
    path = "/home/sun/AA-labs-FE/research_data/uni/polygon/uni_nft_transfer/uni-nft-transfer.csv"
    nft_transfers = pd.read_csv(path)

    this_pool_transfer_idx = []
    pos_lq = pd.read_csv("/home/sun/AA-labs-FE/14_uni_pool_evaluate/data/position_liquidity.csv",  dtype = {'id': str})
    pos_lq1 = pos_lq[pos_lq["id"].str.len() < 40]
    pos_lq1["id"] = pos_lq1["id"].apply(lambda x: int(x))
    all_pos_id = pos_lq1["id"].unique().tolist()
    with tqdm(total=len(nft_transfers.index), ncols=120) as pbar:
        for index, row in nft_transfers.iterrows():
            if row["position_id"] in all_pos_id and row["from"] != "0x0000000000000000000000000000000000000000" and row["to"] != "0x0000000000000000000000000000000000000000":
                this_pool_transfer_idx.append(index)

            pbar.update()

    this_pool_nft_transfer: pd.DataFrame = nft_transfers.loc[nft_transfers.index[this_pool_transfer_idx]]
    this_pool_nft_transfer.to_csv("/home/sun/AA-labs-FE/research_data/uni/polygon/uni_nft_transfer/uni-nft-transfer.usdc-weth-005.csv")
