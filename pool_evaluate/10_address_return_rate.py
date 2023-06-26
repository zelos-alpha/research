import pandas as pd
import os
from utils import LivePosition, format_date, config, to_decimal
from tqdm import tqdm
import math

if __name__ == "__main__":
    addr_pos = pd.read_csv(os.path.join(config["save_path"], "position_address.csv"))
    addresses = addr_pos.groupby("address")
    for address, positions in addresses:
        if address != '0x00534d0b37ab1dd1a967230f76d001e11abbd1ff':
            continue
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

        for index, row in address_total_rate_df.iterrows():
            total_sum=0
            net_value_sum = 0
            for c_name, c_value in row.items():
                pass

    pass
    # with tqdm(total=len(files), ncols=150) as pbar:
    #     for file in files:
    #
    #         path = os.path.join(config["save_path"], f"fee_result", file)
    #         df = pd.read_csv(path)
    #         df["final_return_rate"] = 1
    #         val = 1
    #         for index, row in df.iterrows():
    #             val *= row["return_rate"]
    #             df.loc[index, "final_return_rate"] = val
    #         df.to_csv(path)
    #         pbar.update()
