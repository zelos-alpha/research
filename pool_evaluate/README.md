# README

## 用途

统计网络上所有的地址的收益率

步骤见1,2,3,4.......


## 遗留问题

1. 对于开始没有swap的position, 统计的金额会是0,
2. 手续费全部是程序根据swap金额和流动性的分成估计的. 没有根据collect的最终金额进行校对.  
3. demeter-fetch中, 对于collect, 如果proxy和pool的金额相差过大, 无法正确匹配proxy和pool的交易. [例子](https://polygonscan.com/tx/0xca50d94a36bc730a4ebb46b9e7535075d7da8a4efbbf9cc53638b058516dc907#eventlog) 
4. 每次swap中, tick得到的收益, 可以用稀疏矩阵来存储(步骤4的代码)