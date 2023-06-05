import random
import time
from typing import List

import pandas as pd
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.remote.webelement import WebElement
from webdriver_manager.microsoft import EdgeChromiumDriverManager

if __name__ == "__main__":
    # addrs_to_tag = ["0xc36442b4a4522e871399cd717abdd847ab11fe88"]
    # df = pd.DataFrame(data=addrs_to_tag, columns=["id"])
    # print(df)
    # df = pd.read_csv("peopleInDegree.csv")
    df = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/contract_addr_tag.csv")
    df_tx = pd.read_csv("/home/sun/AA-labs-FE/12_meme_research/token_0x7a58c0be72be218b41c608b7fe7c5bb630736c71.tx.csv")

    # pip install selenium
    # pip install webdriver-manager
    options = webdriver.EdgeOptions()
    options.add_argument("--proxy-server=127.0.0.1:7890")
    driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()), options=options)

    for index, row in df.iterrows():
        if row["type"] != "Other":
            continue
        if index < 143:
            continue
        txes = df_tx[df_tx["to"] == row["address"]]
        idx = 3 if len(txes.index) > 3 else (len(txes.index) - 1)
        try:
            hash = txes.iloc[idx]["hash"]
        except:
            continue
        driver.get(f"https://etherscan.io/tx/{hash.replace(' ', '')}")
        time.sleep(random.randint(3, 8))
        try:
            title_document: List[WebElement] = driver.find_elements(by=By.CSS_SELECTOR, value=".hash-tag.text-truncate")
            if len(list(filter(lambda x: "MEV Transaction" in x.text, title_document))) > 0:
                row["type"] = "MEVbot"
        except NoSuchElementException as e:
            pass

    driver.quit()

    print(df)
    df.to_csv("peopleInDegree_with_tag.csv", index=False)
    pass
