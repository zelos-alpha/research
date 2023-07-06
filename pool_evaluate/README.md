# README

## 用途

统计网络上所有的地址的收益率

步骤见1,2,3,4.......


## 遗留问题

1. 对于开始没有swap的position, 统计的金额会是0,
2. 手续费全部是程序根据swap金额和流动性的分成估计的. 没有根据collect的最终金额进行校对.  
3. (解决)demeter-fetch中, 对于collect, 如果proxy和pool的金额相差过大, 无法正确匹配proxy和pool的交易. [例子](https://polygonscan.com/tx/0xca50d94a36bc730a4ebb46b9e7535075d7da8a4efbbf9cc53638b058516dc907#eventlog) 
4. (放弃)每次swap中, tick得到的收益, 可以用稀疏矩阵来存储(步骤4)(放弃, 因为实际空白值是0, 而不是nan, 如果硬要存nan, 会增加position计算的负担. )
5. 没有根据nft转账的情况, 修正流动性的受益人收益 (步骤6,7)

```python
# 稀疏矩阵的示例代码
import pandas as pd
import numpy as np

def get_random(size=10):
    arr = np.random.randn(size)
    arr[np.random.randint(0, size / 2):np.random.randint(size / 2, size)] = np.nan
    return pd.arrays.SparseArray(arr)


if __name__ == "__main__":
    df = pd.DataFrame({"col1": get_random()})
    print(df)
```