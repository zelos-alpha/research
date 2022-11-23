import json
from datetime import date, timedelta

import pandas as pd

import common.constants as constants
import common.types as comm_type
from tool_process.v2.preprocess import compare_burn_data
from toys.toy_common import format_date

folder = "../data-op-bq-tick/"
pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.set_option('display.width', 5000)
pd.options.display.max_colwidth = 100

if __name__ == "__main__":
    # contract = "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31"
    # contract = "0x85149247691df622eaf1a8bd0cafd40bc45154a9"
    contract = "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6"
    start = date(2022, 10, 10)
    day = start
    #         with tqdm(total=len(self._data.index), ncols=150) as pbar:
    total_df = None
    while day < date(2022, 11, 15):

        df = pd.read_csv(f"{folder}{contract}-{format_date(day)}.csv")
        if total_df is None:
            total_df = df
        else:
            total_df = pd.concat([total_df, df])
        day = day + timedelta(days=1)
    total_df: pd.DataFrame = total_df.loc[total_df.tx_type != "SWAP"]
    total_df = total_df.drop(
        total_df[(total_df.tx_type == "BURN") & (total_df.amount0 == 0) & (total_df.amount1 == 0)].index)
    total_df = total_df.sort_values(by=['position_id', "block_number", "pool_tx_index", "pool_log_index"])

    order = ["position_id", "tx_type", "block_number", "transaction_hash", "block_timestamp",
             "pool_log_index",
             "proxy_log_index", "tick_lower", "tick_upper", "liquidity", "current_tick", "amount0", "amount1",
             "sqrtPriceX96", "sender", "receipt"]
    total_df = total_df[order]

    total_df.to_csv(f"{folder}{contract}-merged.csv", index=False)
