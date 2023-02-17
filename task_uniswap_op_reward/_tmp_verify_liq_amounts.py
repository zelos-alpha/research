from demeter.broker.liquitidymath import get_amounts
# verify amount and liquidity
# those data is fetched from usdc-dai 0.01 pool, tick space is very narrow, so when tick space changed,
#   liquidity changed dramatically
# tick lower, tick upper, liquidity
# 276323.0,276327.0,1441733136647360348160
# 276324.0,276325.0,280755821401763282944
#
# 276322.0,276327.0,139096284665430848
# 276323.0,276325.0,3023816379733441511424
if __name__ == "__main__":
    am0, am1 = get_amounts(79222845043620512858451684490913578, 276323, 276327, 1441733136647360348160, 6,18)
    print(am0 + am1)

    am0, am1 = get_amounts(79222845043620512858451684490913578, 276324, 276325, 280755821401763282944, 6,18)
    print(am0 + am1)

    am0, am1 = get_amounts(79222845043620512858451684490913578, 276322, 276327, 139096284665430848, 6,18)
    print(am0 + am1)

    am0, am1 = get_amounts(79222845043620512858451684490913578, 276323, 276325, 3023816379733441511424, 6,18)

    print(am0 + am1)
    pass
