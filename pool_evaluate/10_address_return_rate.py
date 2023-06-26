import pandas as pd
import os
from utils import LivePosition, format_date, config, to_decimal
from tqdm import tqdm
import math
from multiprocessing import Pool


def convert_one_address(param):
    address, positions = param
    # if address != '0x00534d0b37ab1dd1a967230f76d001e11abbd1ff':
    #     return
    address_total_nv_df = pd.DataFrame()
    address_total_rate_df = pd.DataFrame()

    for _, position_row in positions.iterrows():
        tmp_df = pd.read_csv(os.path.join(config["save_path"], "fee_result", f"{position_row['position']}.csv"), index_col=0, parse_dates=True)
        part_df_nv = tmp_df[["total_net_value"]]
        part_df_rate = tmp_df[["final_return_rate"]]
        part_df_nv = part_df_nv.rename(columns={"total_net_value": position_row['position']})
        part_df_rate = part_df_rate.rename(columns={"final_return_rate": position_row['position']})
        address_total_nv_df = pd.concat([address_total_nv_df, part_df_nv], axis=1)
        address_total_rate_df = pd.concat([address_total_rate_df, part_df_rate], axis=1)
        pass

    result_list = []

    for index, row in address_total_nv_df.iterrows():
        total_sum = 0
        net_value_sum = 0
        positions = []
        for c_name, c_value in row.items():
            if not math.isnan(c_value):
                total_sum += c_value * address_total_rate_df[c_name].loc[index]
                net_value_sum += c_value
                positions.append(c_name)
        result_list.append({
            "net_value": net_value_sum,
            "return_rate": total_sum / net_value_sum,
            "positions": positions,

        })
    result_df = pd.DataFrame(index=address_total_rate_df.index, data=result_list)
    result_df.to_csv(os.path.join(config["save_path"], "address_result", f"{address}.csv"))


if __name__ == "__main__":
    addr_pos = pd.read_csv(os.path.join(config["save_path"], "position_address.csv"))
    addresses = addr_pos.groupby("address")
    addresses = list(addresses)

    with Pool(20) as p:
        res = list(tqdm(p.imap(convert_one_address, addresses), ncols=120, total=len(addresses)))

