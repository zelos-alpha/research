import random
import pandas as pd
import requests
from tqdm import tqdm

endpoint = "https://eth-mainnet.g.alchemy.com/v2/uB5w7CMQOWk3iXB8BtJn3zh9AUvAvmYW"
proxy = "127.0.0.1:7890"


def __encode_json_rpc(method: str, params: list):
    return {"jsonrpc": "2.0", "method": method, "params": params, "id": random.randint(1, 2147483648)}


def __decode_json_rpc(response: requests.Response):
    content = response.json()
    if "error" in content:
        raise RuntimeError(content["error"]["code"], content["error"]["message"])
    return content["result"]


def do_post(param):
    return session.post(endpoint,
                        json=param,
                        proxies=proxies)


if __name__ == "__main__":
    """
    判断地址transfer转帐的from,to地址, 是否是合约, 并把"addr-是否是合约"单独保存到一个文件里 
    """
    df = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/0x7a58c0be72be218b41c608b7fe7c5bb630736c71.transfer.csv")
    addrs = df["from"]
    addrs = addrs.append(df["to"], ignore_index=True)
    print(len(addrs))
    addrs = pd.unique(addrs)
    print(addrs)
    df2 = pd.DataFrame(addrs, columns=["addr"])

    # df2.to_csv("/home/sun/AA-labs-FE/12_meme_research/0x7a58c0be72be218b41c608b7fe7c5bb630736c71.transfer.addrs.csv")
    # df2 = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/0x7a58c0be72be218b41c608b7fe7c5bb630736c71.transfer.addrs.csv")

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    proxies = {"http": proxy, "https": proxy, } if proxy else {}

    is_contract_list = []
    with tqdm(total=len(df2.index), ncols=150) as pbar:
        for index, value in df2.iterrows():
            response = do_post(__encode_json_rpc("eth_getCode", [value["addr"], "latest"]))
            response_val = __decode_json_rpc(response)
            is_contract_list.append(len(response_val) > 2)
            pbar.update()
    df2["is_contract"] = is_contract_list
    df2.to_csv("/home/sun/AA-labs-FE/12_meme_research/0x7a58c0be72be218b41c608b7fe7c5bb630736c71.transfer.addrs.csv", index=False)
