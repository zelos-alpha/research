# data v2

## row definition

one row is an action in uniswap. such as add liquidity, swap
one transaction may contain multiple action.

for swap, a row is extract by on event log(swap)
for add/remove liquidity, collect, a row is extract by two event logs, one is thrown by pool contract and the other is thrown by proxy contract

## data column definition

for swap, a row is extract by on event log(swap)

* block_number: block number of this transaction
* block_timestamp: timestamp of block
* tx_type: mint(add liquidity)/burn(remove liquidity)/swap/collect, actions of uniswap
* transaction_hash
* pool_tx_index: index of transaction which interact will pool contract
* pool_log_index: index of log which thrown by pool contract
* proxy_log_index: index of log which thrown by proxy contract
* sender: tx sender
* receipt: receiver in swap
* amount0: amount 0 on in log
* amount1: amount 1 on in log
* total_liquidity: total liquidity of this pool, thrown by swap event. other event will keep the last value.
* total_liquidity_delta: liquidity change for current action, if price is in position price range, total_liquidity_delta== liuquidity, else total_liquidity_delta==0
* sqrtPriceX96: swap price
* current_tick: current swap tick, other event will keep the last value
* position_id: position to operate. if this position is not operated by proxy contract, no position id will be generate.
* tick_lower: position lower tick, for swap is null.
* tick_upper: position upper tick, for swap is null
* liquidity: position liquidity. read for log. for swap is null
