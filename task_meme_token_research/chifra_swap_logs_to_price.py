# 从chifra中导出uniswap pool的logs, 然后从中提取中价格信息


import pandas as pd
import os
import pathlib

# pool_config = {
#     "address": "0x83abecf7204d5afc1bea5df734f085f2535a9976",
#     "decimal0": 18,
#     "decimal1": 18,
#     "is_token0_base": False
# }
pool_config = {
    "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
    "decimal0": 6,
    "decimal1": 18,
    "is_token0_base": True
}

def signed_int(h):
    """
    Converts hex values to signed integers.
    """
    s = bytes.fromhex(h[2:])
    i = int.from_bytes(s, 'big', signed=True)
    return i


def sqrt_to_price(sqrt_price):
    decimal_val = sqrt_price / 2 ** 96
    decimal_price = decimal_val ** 2
    pool_price = decimal_price * (10 ** (pool_config["decimal0"] - pool_config["decimal1"]))
    return 1 / pool_price if pool_config["is_token0_base"] else pool_price


if __name__ == "__main__":
    from_path = "/home/sun/AA-labs-FE/12_meme_research/0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640.swap.logs.csv"
    df_logs = pd.read_csv(from_path)
    df_logs = df_logs[df_logs["address"] == pool_config["address"]]

    result = []
    for index, row in df_logs.iterrows():
        data_hex = row["data"]
        no_0x_data = data_hex[2:]
        chunk_size = 64
        chunks = len(no_0x_data)
        no_0x_data = data_hex[2:]

        split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
        amount0, amount1, sqrtPriceX96, current_liquidity, current_tick = [signed_int(onedata) for onedata in split_data]

        result.append({
            "timestamp": row["timestamp"],
            "blockNumber": row["blockNumber"],
            "date": row["date"],
            # "transactionHash": row["transactionHash"],
            "price": sqrt_to_price(sqrtPriceX96)
        })
        pass

    df_result = pd.DataFrame(result)
    file_name = pathlib.Path(from_path).stem
    to_path = os.path.join(os.path.dirname(from_path), file_name + ".price.csv")
    df_result.to_csv(to_path, index=False)
