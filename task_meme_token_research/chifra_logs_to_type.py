# 从chifra中导出uniswap pool的logs, 然后从中提取中价格信息
from enum import Enum

import pandas as pd
import os
import pathlib

pool_config = {
    "address": "0x83abecf7204d5afc1bea5df734f085f2535a9976",
    "decimal0": 18,
    "decimal1": 18,
    "is_token0_base": False
}

MINT_KECCAK = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
SWAP_KECCAK = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
BURN_KECCAK = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
COLLECT_KECCAK = "0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0"


class OnchainTxType(str, Enum):
    MINT = "MINT"
    SWAP = "SWAP"
    BURN = "BURN"
    COLLECT = "COLLECT"
    OTHER = "OTHER"


type_dict = {
    MINT_KECCAK: OnchainTxType.MINT,
    SWAP_KECCAK: OnchainTxType.SWAP,
    BURN_KECCAK: OnchainTxType.BURN,
    COLLECT_KECCAK: OnchainTxType.COLLECT
}


def signed_int(h):
    """
    Converts hex values to signed integers.
    """
    s = bytes.fromhex(h[2:])
    i = int.from_bytes(s, 'big', signed=True)
    return i


def hex_to_address(topic_str):
    return "0x" + topic_str[26:]


def sqrt_to_price(sqrt_price):
    decimal_val = sqrt_price / 2 ** 96
    decimal_price = decimal_val ** 2
    pool_price = decimal_price * (10 ** (pool_config["decimal0"] - pool_config["decimal1"]))
    return 1 / pool_price if pool_config["is_token0_base"] else pool_price


def get_tx_type(topic0):
    if topic0 in type_dict:
        tx_type = type_dict[topic0]
    else:
        tx_type = OnchainTxType.OTHER
    return tx_type


def handle_event(tx_type, topic1, topic2, topic3, data_hex):
    # proprocess topics string ->topic list
    # topics_str = topics.values[0]
    sender = liquidity = sqrtPriceX96 = receipt = amount0 = amount1 = current_liquidity = current_tick = tick_lower = tick_upper = delta_liquidity = None

    # data_hex = data.values[0]

    no_0x_data = data_hex[2:]
    chunk_size = 64
    chunks = len(no_0x_data)
    match tx_type:
        case OnchainTxType.SWAP:
            sender = hex_to_address(topic1)
            receipt = hex_to_address(topic2)
            split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
            amount0, amount1, sqrtPriceX96, current_liquidity, current_tick = [signed_int(onedata) for onedata in split_data]

        case OnchainTxType.BURN:
            sender = hex_to_address(topic1)
            tick_lower = signed_int(topic2)
            tick_upper = signed_int(topic3)
            split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
            liquidity, amount0, amount1 = [signed_int(onedata) for onedata in split_data]
            delta_liquidity = -liquidity
        case OnchainTxType.MINT:
            # sender = topic_str_to_address(topic1)
            owner = hex_to_address(topic1)
            tick_lower = signed_int(topic2)
            tick_upper = signed_int(topic3)
            split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
            sender = hex_to_address(split_data[0])
            liquidity, amount0, amount1 = [signed_int(onedata) for onedata in split_data[1:]]
            delta_liquidity = liquidity
        case OnchainTxType.COLLECT:
            tick_lower = signed_int(topic2)
            tick_upper = signed_int(topic3)
            split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
            sender = hex_to_address(split_data[0])
            amount0, amount1 = [signed_int(onedata) for onedata in split_data[1:]]

        case _:
            # raise ValueError("not support tx type")
            pass
    return sender, receipt, amount0, amount1, sqrtPriceX96, current_liquidity, current_tick, tick_lower, tick_upper, liquidity, delta_liquidity


if __name__ == "__main__":
    from_path = "/home/sun/AA-labs-FE/12_meme_research/0x83abecf7204d5afc1bea5df734f085f2535a9976.logs.csv"
    df_logs = pd.read_csv(from_path)
    df_logs = df_logs[df_logs["address"] == pool_config["address"]]

    result = []
    df_logs["tx_type"] = df_logs.apply(lambda x: get_tx_type(x["topic0"]), axis=1)
    df_logs = df_logs[df_logs["tx_type"] != OnchainTxType.OTHER]

    df_logs[["sender", "receipt", "amount0", "amount1",
             "sqrtPriceX96", "total_liquidity", "current_tick", "tick_lower", "tick_upper", "liquidity",
             "total_liquidity_delta"]] = df_logs.apply(
        lambda x: handle_event(x.tx_type, x.topic1, x.topic2, x.topic3, x.data), axis=1, result_type="expand")
    # for index, row in df_logs.iterrows():
    #     data_hex = row["data"]
    #     no_0x_data = data_hex[2:]
    #     chunk_size = 64
    #     chunks = len(no_0x_data)
    #     no_0x_data = data_hex[2:]
    #
    #     split_data = ["0x" + no_0x_data[i:i + chunk_size] for i in range(0, chunks, chunk_size)]
    #     amount0, amount1, sqrtPriceX96, current_liquidity, current_tick = [signed_int(onedata) for onedata in split_data]
    #
    #     result.append({
    #         "timestamp": row["timestamp"],
    #         "blockNumber": row["blockNumber"],
    #         "date": row["date"],
    #         "transactionHash": row["transactionHash"],
    #         "price": sqrt_to_price(sqrtPriceX96)
    #     })
    #     pass
    df_result = df_logs.drop(columns=["topic0", "topic1", "topic2", "topic3", "data", "compressedLog", "address"])
    # df_result = pd.DataFrame(result)
    file_name = pathlib.Path(from_path).stem
    to_path = os.path.join(os.path.dirname(from_path), file_name + ".tick.csv")
    df_result.to_csv(to_path, index=False)
