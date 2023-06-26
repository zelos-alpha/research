import pandas as pd
import os
from utils import LivePosition, format_date, config, to_decimal
from tqdm import tqdm

if __name__ == "__main__":
    files = os.listdir(os.path.join(config["save_path"], "fee_result"))
    with tqdm(total=len(files), ncols=150) as pbar:
        for file in files:

            path = os.path.join(config["save_path"], f"fee_result", file)
            df = pd.read_csv(path)
            df["final_return_rate"] = 1
            val = 1
            for index, row in df.iterrows():
                val *= row["return_rate"]
                df.loc[index, "final_return_rate"] = val
            df.to_csv(path)
            pbar.update()
