import pandas as pd
from tqdm import tqdm

'''
merge to price files, from price a-b,b-c, get a-c
usage step:
1. generate 2 price file by chifra_swap_logs_to_price.py
2. use this script, merge price to a-c
Note:This script find the most close block number when mergeing
'''

if __name__ == "__main__":
    df1_path = "/home/sun/AA-labs-FE/12_meme_research/0x83abecf7204d5afc1bea5df734f085f2535a9976.swap.logs.price.csv"
    df1 = pd.read_csv(df1_path)
    df2 = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640.swap.logs.price.csv")
    final_price = []

    with tqdm(total=len(df1.index), ncols=150) as pbar:
        for index, row in df1.iterrows():
            height_match = df2[df2["blockNumber"] == row["blockNumber"]]
            if len(height_match.index) < 1:
                small_df2 = df2[(df2["blockNumber"] > row["blockNumber"] - 100) & (df2["blockNumber"] < row["blockNumber"] + 100)]
                height_match = small_df2.iloc[(small_df2["blockNumber"] - row["blockNumber"]).abs().argsort()[:1]]
            final_price.append(height_match.iloc[0]["price"] * row["price"])
            pbar.update()
    df1["final_price"] = final_price
    df1.to_csv(df1_path, index=False)
