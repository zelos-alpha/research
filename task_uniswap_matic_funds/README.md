# 说明

## 用途

计算选定的基金/地址的收益率

## 使用步骤

1. 使用[tool_downloader](..%2Ftool_downloader)下载数据, 按天为单位下载
2. 使用[tool_process](..%2Ftool_process), 将数据预处理,
3. 使用[merge_files_to_one_csv.py](merge_files_to_one_csv.py)将每天的数据, 合并到一个文件.
4. 将合并的csv文件, 导入到mysql数据库
5. 进行一些统计, 确定测试的目标,

```sql
# 按照质押资金量来统计
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
# 按照collect次数统计
select receipt, count(receipt) cnt
from polygon_usdc_weth
where tx_type = 'collect'
group by receipt
order by cnt desc
```

6. 确定这个地址的相关交易, 经过研究, 使用collect类型的交易来统计, 对于自己操作pool合约的投资合约, receipt是自己, 而对于操作代理合约的, 根据position_id查找就可以. 
```sql
select *
from polygon_usdc_weth
where transaction_hash in (select transaction_hash
                           from polygon_usdc_weth
                           where receipt = '0x364d5077c4c6bce6280705fe657fc4c55fdac363')

select *
from polygon_usdc_weth
where position_id in (select position_id
                      from polygon_usdc_weth
                      where receipt = '0x28d1c71f83c1a138336f98b929bfd44e54114a8b')

# 查看相关交易的sql

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

7. 导出查询结果. 然后执行[calc_return_rate.py](calc_return_rate.py)
8. 查看导出文件的收益率, 净值等. 并绘图

