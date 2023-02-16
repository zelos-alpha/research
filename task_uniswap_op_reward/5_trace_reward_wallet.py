import pandas as pd

contracts = {
    "WETH-DAI": "0x03af20bdaaffb4cc0a521796a223f7d85e2aac31",
    "USDC-DAI": "0xbf16ef186e715668aa29cef57e2fd7f9d48adfe6",
    "wstETH-ETH": "0x4a5a2a152e985078e1a4aa9c3362c412b7dd0a86",
    "OP-USDC-03": "0x1c3140ab59d6caf9fa7459c6f83d4b52ba881d36",
    "OP-USDC-005": "0x1d751bc1a723accf1942122ca9aa82d49d08d2ae"
}
gamma = {
    "WETH-DAI": "0x14e42871d90282ec0439f01347178d6331fc1873",
    "USDC-DAI": "0xaed05fdd471a4eecee48b34d38c59cc76681a6c8",
    "wstETH-ETH": "0xa8524abc230725da48e61b677504fb5e8a409e38",
    "OP-USDC-03": "0x2102bef0d9727ea50ba844e7658e38480961835c"  # narrow
}
gamma2 = {
    "OP-USDC-03": "0x4f2b80d797bd1b93903cc84e5992586142c3ecd1"  # wide
}
xtoken = {
    "WETH-DAI": "0x7fc70ABE76605d1Ef1f7A5DdC5E2ad35A43a6949",
    "USDC-DAI": "0x3f96C580436dD59404Ba612bf6D8079Dc10f6f7E",
    "wstETH-ETH": "0x11AE2b89175792F57D320a020eaEa879E837fe6c",
    "OP-USDC-03": "0x9b36d76a7ae6C9AaAED5FB2248670612363f036e"
}
arrakis = {
    "WETH-DAI": "0x96fecbdc4ff071779cd82da7ba23da76b0e37acb",
    "USDC-DAI": "0x632336474f5bf11aebecd63b84a0a2800b99a490",
    "wstETH-ETH": "0x3dc6a6ef1836f02d9ff29212c22e71fa3b74d1a9",
    "OP-USDC-03": "0xa969a1bc401d27eccd1cf60bc53b78c27183e214"
}
defiedge = {
    "USDC-DAI": "0xf07c9e48a92de4d1229965935b4c70389d8a498a",
    "wstETH-ETH": "0xd19077f505ba21b970f88d7f1b81bf3a8a59bcc2",
    "OP-USDC-005": "0xf06e0c89749adb5976a114fde5b74e925531ee1b"
}

choosen_pool = "OP-USDC-005"
choosen_contract= contracts[choosen_pool]
choosen_wallet = defiedge[choosen_pool]


result_path="/home/sun/AA-labs-FE/05_op_reward_phase2/data-result/"
tick_path="/home/sun/AA-labs-FE/05_op_reward_phase2/data-tick/"
def xtoken():
    source = pd.read_csv(f"{result_path}xtoken-{choosen_pool}_origin.csv", index_col=False)
    actions = pd.read_csv(f"{tick_path}{choosen_contract}-merged.csv")
    new_actions = None
    for index, source_row in source.iterrows():
        matched = actions.loc[actions.transaction_hash == source_row.Txhash]
        if new_actions is None:
            new_actions = matched
        else:
            new_actions = pd.concat([new_actions, matched])
        if len(matched.index) < 1:
            print(source_row.Txhash)
    new_actions.to_csv(f"{result_path}xtoken-{choosen_pool}.csv", index=False)


def gamma():
    actions = pd.read_csv(f"{tick_path}{choosen_contract}-merged.csv")
    for index, row in actions.iterrows():
        rel_action: pd.DataFrame = actions.loc[actions.transaction_hash == row.transaction_hash]
        actions.loc[rel_action.index, "sender"] = row.sender
    actions: pd.DataFrame = actions.loc[ actions.sender == choosen_wallet]
    actions.to_csv(f"{result_path}gamma-{choosen_pool}.csv", index=False)
    pass


def akk():
    actions = pd.read_csv(f"{tick_path}{choosen_contract}-merged.csv")
    actions = actions.loc[actions["sender"] == choosen_wallet]
    # actions.to_csv(f"/home/sun/AA-labs-FE/05_op_reward_phase2/data-result/akk-{choosen_pool}.csv", index=False)
    actions.to_csv(f"{result_path}defiedge-{choosen_pool}.csv", index=False)


if __name__ == "__main__":
    # gamma()
    # xtoken()
    akk()
