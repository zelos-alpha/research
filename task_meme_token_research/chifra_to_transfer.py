import pandas as pd
import sys


def to_addr(hex_str: str) -> str:
    return "0x" + hex_str[26:]


def to_value(hex_str: str) -> str:
    if len(hex_str) < 3:
        return 0
    return str(int(hex_str, 16))


if __name__ == "__main__":
    """
    把chifra导出的transfer log, 整理成transfer格式的文件
    要求必须在数据的文件夹使用
    使用方法: python chifra_to_transfer.py [地址]
    """
    log_addr = sys.argv[1]
    logs = pd.read_csv(f"./{log_addr}.log.csv")
    # logs: pd.DataFrame = logs.loc[logs.topic0 == '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef']
    logs["from"] = logs.apply(lambda x: to_addr(x["topic1"]), axis=1)
    logs["to"] = logs.apply(lambda x: to_addr(x["topic2"]), axis=1)
    logs["value"] = logs.apply(lambda x: to_value(x["data"]), axis=1)
    logs = logs.drop(columns=["topic0", "topic1", "topic2", "topic3", "data", "compressedLog", "address"])
    print(logs)
    logs.to_csv(f"./{log_addr}.transfer.csv", index=False)
