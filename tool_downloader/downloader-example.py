import os

import v1.big_query_downloader as big_query_downloader
from datetime import datetime
import time

if __name__ == "__main__":
    time_start = time.time()
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = "/home/sun/AA-labs-FE/code/demeter/auth/airy-sight-361003-d14b5ce41c48.json"
    big_query_downloader.download_bigquery_pool_event_matic(
        contract_address="0x45dda9cb7c25131df268515131f647d726f50608",  # 0x45dda9cb7c25131df268515131f647d726f50608
        date_begin=datetime(2022, 12, 13, 0, 0, 0),
        date_end=datetime(2023, 1, 1, 0, 0, 0),
        data_save_path="./data",
        chain_type=big_query_downloader.ChainType.Polygon)
    time_end = time.time()
    print('time cost', time_end - time_start, 's')
