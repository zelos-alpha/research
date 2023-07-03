import os.path
from datetime import timedelta, date

import pandas as pd
from tqdm import tqdm

from utils import format_date, config, get_pos_key
"""
7 建立地址和头寸的对应关系(利用mint)
"""
if __name__ == "__main__":
    start = date(2021, 12, 20)
    end = date(2023, 6, 20)
    day = start

    position_address_dict: dict[str, str] = {}

    with tqdm(total=(end - start).days + 1, ncols=150) as pbar:
        while day <= end:
            day_str = format_date(day)

            file = os.path.join(
                config["path"],
                f"polygon-0x45dda9cb7c25131df268515131f647d726f50608-{day_str}.tick.csv",
            )

            df = pd.read_csv(file)
            df = df[df["tx_type"] == "MINT"]

            for index, row in df.iterrows():
                pos_key = get_pos_key(row)
                if pos_key not in position_address_dict:
                    position_address_dict[pos_key] = row["sender"]

            day += timedelta(days=1)
            pbar.update()

    result = pd.DataFrame.from_dict({"position": position_address_dict.keys(), "address": position_address_dict.values()})
    result.to_csv(os.path.join(config["save_path"], "position_address.csv"), index=False)
