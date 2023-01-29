# README

## Purpose

Analysis on chain event of a Uniswap V3 pool. Find out active funds and address, then calculate their rate of return.

## Usage

1. Download on chain event logs via [tool_downloader](..%2Ftool_downloader), you should write a simple script according to the example, and set up several parameters, such as chain, pool address, google application credentials. After download, data will be saved in files split by day.
2. Process data via [tool_process](..%2Ftool_process), on chain trade actions will be extracted from event logs.
3. Call [merge_files_to_one_csv.py](merge_files_to_one_csv.py)to merge all mint/burn/collect actions to one CSV file.
4. To make the analysis easier, import the csv file to a database such as mysql. 
5. Now the following SQL to find out active address. We choose address by sum of collect amount and collect action counts.

```sql
# Statistics according to total collect amount,

select st.receipt, format(st.sum_amount, 2) amount, ct.cnt
from (select receipt, sum(amount0 / POWER(10, 6) + price * amount1 / POWER(10, 18)) sum_amount
      from polygon_usdc_weth
      where tx_type = 'collect'
      group by receipt
      order by sum_amount desc) st
         left outer join
     (select receipt, count(receipt) cnt
      from polygon_usdc_weth
      where tx_type = 'collect'
      group by receipt
      order by cnt desc) ct on st.receipt = ct.receipt

# Statistics according to collect times

select receipt, count(receipt) cnt
from polygon_usdc_weth
where tx_type = 'collect'
group by receipt
order by cnt desc
```

6. Once an address is picked. Find all relative transactions about this adress. For addresses who interact with uniswap proxy contact, we can use position id to find all relative actions. but for funds who wrote their own contract to interact with uniswap pool, their is no position id, so we have to find relative actions by transaction. 

```sql

select *
from polygon_usdc_weth
where position_id in (select position_id
                      from polygon_usdc_weth
                      where receipt = '0x28d1c71f83c1a138336f98b929bfd44e54114a8b')

select *
from polygon_usdc_weth
where transaction_hash in (select transaction_hash
                           from polygon_usdc_weth
                           where sender = '0x364d5077c4c6bce6280705fe657fc4c55fdac363')



# the following sql will help you analysis actions manually.

select position_id,
       block_timestamp,
       tx_type,
       transaction_hash,
       amount0 / pow(10, 6)                                 as amt0,
       amount1 / pow(10, 18)                                as amt1,
       amount0 / pow(10, 6) + amount1 / pow(10, 18) * price as net_value,
       current_tick,
       tick_lower,
       tick_upper,
       liquidity,
       price
from polygon_usdc_weth
where position_id in (select position_id
                      from polygon_usdc_weth
                      where receipt = '0xdfc52f8d8d3d056c91decc6788808c7e172d1ac2')
order by block_number;

```

7. Export the actions. then run backtest by [calc_return_rate.py](calc_return_rate.py), This script will replay the actions of a fund using demeter. 
8. Check rate of return, net values in generated csv file.

