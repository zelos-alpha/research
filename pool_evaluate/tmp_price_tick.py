from demeter.uniswap.helper import tick_to_quote_price
"""
查看tick和价格关系, 评估是否合适做tick的resample
"""
print(tick_to_quote_price(201194,6,18,True))
print(tick_to_quote_price(201204,6,18,True))
print(tick_to_quote_price(201214,6,18,True))


