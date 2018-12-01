# coding=utf-8

# 根据以往的经验，每年年底都会有一波高送转预期行情。

import tushare as ts

'''
选股原理
一般来说，具备高送转预期的个股，都具有总市值低、每股公积金高、每股收益大，流通股本少的特点。
当然，也还有其它的因素，比如当前股价、经营收益变动情况、以及以往分红送股习惯等等。

这里我们暂时只考虑每股公积金、每股收益、流通股本和总市值四个因素，将公积金大于等于5元，
每股收益大于等于5毛，流通股本在3亿以下，总市值在100亿以内作为高送转预期目标。
'''
def main():
    '''
    数据准备
    '''
    # 调取股票基本面数据和行情数据
    ## 基本面数据
    basic = ts.get_stock_basics()

    ## 行情和市值数据
    hq = ts.get_today_all()

    '''
    数据清洗整理
    '''
    # 对获取到的数据进行清洗和整理，只保留需要的字段。
    ## 当前股价，如果停牌则设置当前价格为上一个交易日股价
    hq['trade'] = hq.apply(lambda x: x.settlement if x.trade == 0 else x.trade, axis=1)

    ## 分别选取流通股本，总股本，每股公积金，每股收益
    basedata = basic[['outstanding', 'totals', 'reservedPerShare', 'esp']]

    ## 选取股票代码，名称，当前价格，总市值，流通市值
    hqdata = hq[['code', 'name', 'trade', 'mktcap', 'nmc']]

    ## 设置行情数据code为index列
    hqdata = hqdata.set_index('code')

    ## 合并两个数据表
    data = basedata.merge(hqdata, left_index=True, right_index=True)

    data.head(10)

    '''
    选股条件
    '''
    ## 将总市值和流通市值换成亿元单位
    data['mktcap'] = data['mktcap'] / 10000
    data['nmc'] = data['nmc'] / 10000

    # 设置参数和过滤值（此次各自调整）

    ## 每股公积金 >= 5
    res = data.reservedPerShare >= 5

    ## 流通股本 <= 3亿
    out = data.outstanding <= 3000

    ## 每股收益 >= 5毛
    eps = data.esp > 0.5

    ## 总市值 < 100亿
    mktcap = data.mktcap <= 100

    # 取并集结果
    allcrit = res & out & eps & mktcap
    selected = data[allcrit]

    # 具有高送转预期股票的结果呈现：
    selected[['name', 'trade', 'reservedPerShare', 'outstanding', 'esp', 'mktcap', 'nmc']].to_excel("gaozhuansong.xls")


if __name__ == "__main__":
    main()

