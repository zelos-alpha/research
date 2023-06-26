import pandas as pd
import os
from utils import LivePosition, format_date, config, to_decimal
from tqdm import tqdm

if __name__ == "__main__":
    addr_pos = pd.read_csv(os.path.join(config["save_path"], "position_address.csv"))
    addresses = addr_pos.groupby("address")
    for address, positions in addresses:
        if address != "0x00009346eedf97c069b08c9bd1bb5d5a7feaf4c6":
            continue
        address_total_df = pd.DataFrame()
        for _, position_row in positions.iterrows():
            tmp_df = pd.read_csv(os.path.join(config["save_path"], "fee_result", f"{position_row['position']}.csv"))
            # address_total_df = pd.concat()
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
