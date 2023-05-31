import requests
from datetime import datetime

"https://api.deepcoin.com/deepcoin/market/candles?instId=PEOPLE-USDT&bar=1D&limit=300&after=1659369600000"

proxies = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}


def to_timestamp(dt: datetime):
    return int(dt.timestamp() * 1000)


if __name__ == "__main__":
    start = datetime(2021, 11, 18)
    end = to_timestamp(datetime.now())
    size = 300
    cursor = to_timestamp(start)
    # 不可用, 价格最早是21.12.21的. 但是实际最早是21.11.18
    while cursor < end:
        resp = requests.get(f"https://api.deepcoin.com/deepcoin/market/candles?instId=PEOPLE-USDT&bar=1D&limit={size}&before={cursor}", proxies=proxies)
        value = resp.json()

    pass
