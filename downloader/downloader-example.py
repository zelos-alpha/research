import v1.big_query_downloader as big_query_downloader
from datetime import datetime
import time

if __name__ == "__main__":
    time_start = time.time()
    big_query_downloader.download_bigquery_pool_event_matic(
        contract_address="0xbd934a7778771a7e2d9bf80596002a214d8c9304",
        date_begin=datetime(2021, 12, 21, 0, 0, 0),
        date_end=datetime(2022, 10, 19, 0, 0, 0),
        data_save_path="./data")
    time_end = time.time()
    print('time cost', time_end - time_start, 's')
