import pandas as pd

gamma_weth_usdc = {
    "name": "weth_usdc",
    "contract": "0x85149247691df622eaf1a8bd0cafd40bc45154a9",
    "wallet": "0xed17209ab7f9224e29cc9894fa14a011f37b6115"
}
gamma_weth_dai = {
    "name": "weth_dai",
    "contract": "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31",
    "wallet": "0x14e42871d90282ec0439f01347178d6331fc1873"
}
gamma_usdc_dai = {
    "name": "usdc_dai",
    "contract": "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6",
    "wallet": "0xaed05fdd471a4eecee48b34d38c59cc76681a6c8"
}
xtoken_weth_usdc = {
    "name": "weth_usdc",
    "contract": "0x85149247691df622eaf1a8bd0cafd40bc45154a9",
    "wallet": "0xc5f0237a2a2bb9dc60da73491ad39a1afc4c8b63"
}
xtoken_weth_dai = {
    "name": "weth_dai",
    "contract": "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31",
    "wallet": "0x7fc70abe76605d1ef1f7a5ddc5e2ad35a43a6949"
}
xtoken_usdc_dai = {
    "name": "usdc_dai",
    "contract": "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6",
    "wallet": "0x3f96c580436dd59404ba612bf6d8079dc10f6f7e"
}
akk_weth_usdc = {
    "name": "weth_usdc",
    "contract": "0x85149247691df622eaf1a8bd0cafd40bc45154a9",
    "wallet": "0x38b26d26e575b70ae37f7390a22711500773a00e"
}
akk_weth_dai = {
    "name": "weth_dai",
    "contract": "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31",
    "wallet": "0x96fecbdc4ff071779cd82da7ba23da76b0e37acb"
}
akk_usdc_dai = {
    "name": "usdc_dai",
    "contract": "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6",
    "wallet": "0x632336474f5bf11aebecd63b84a0a2800b99a490"
}
choosen_pair = akk_usdc_dai


def xtoken():
    source = pd.read_csv(f"../data-uni-op/xtoken-{choosen_pair['name']}_origin.csv", index_col=False)
    actions = pd.read_csv(f"../data-op-bq-tick/{choosen_pair['contract']}-merged1.csv")
    new_actions = None
    for index, source_row in source.iterrows():
        matched = actions.loc[actions.transaction_hash == source_row.Txhash]
        if new_actions is None:
            new_actions = matched
        else:
            new_actions = pd.concat([new_actions, matched])
        if len(matched.index) < 1:
            print(source_row.Txhash)
    new_actions.to_csv(f"../data-uni-op/xtoken-{choosen_pair['name']}.csv", index=False)


def gamma():
    actions = pd.read_csv(f"../data-op-bq-tick/{choosen_pair['contract']}-merged1.csv")
    for index, row in actions.iterrows():
        if row.tx_type == "COLLECT":
            rel_action: pd.DataFrame = actions.loc[actions.transaction_hash == row.transaction_hash]
            actions.loc[rel_action.index, "receipt"] = row.receipt
    actions: pd.DataFrame = actions.loc[actions.receipt == choosen_pair["wallet"]]
    actions.to_csv(f"../data-uni-op/gamma-{choosen_pair['name']}.csv", index=False)
    pass


def akk():
    actions = pd.read_csv(f"../data-op-bq-tick/{choosen_pair['contract']}-merged.csv")
    actions = actions.loc[actions["sender"] == choosen_pair["wallet"]]
    actions.to_csv(f"../data-uni-op/akk-{choosen_pair['name']}.csv", index=False)


if __name__ == "__main__":
    # gamma()
    # xtoken()
    akk()
