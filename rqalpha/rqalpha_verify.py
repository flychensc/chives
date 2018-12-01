#验证rqalpha是否正确

import tushare as ts
from datetime import timedelta
from rqalpha.api import logger, update_universe, history_bars


#获取连接备用
CONS = ts.get_apis()

def init(context):
    stock = context.run_info.benchmark
    start_date = context.run_info.start_date
    end_date = context.run_info.end_date

    logger.debug("Verify Stock %s from %s to %s" % (stock[:6],
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d")))

    context.stocks = [stock]
    # 设置要操作的股票池
    update_universe(context.stocks)

    # 15 days 可以得到最近2天的KDJ指标
    context.TIME_PERIOD = 15

    # 获取历史数据
    start_day = (start_date-timedelta(days=context.TIME_PERIOD/5*10)).strftime("%Y-%m-%d")
    end_day = end_date.strftime("%Y-%m-%d")
    #
    # rqalpha没有复权
    # his_data = ts.bar(stock[:6], conn=CONS, adj='qfq', start_date=start_day, end_date=end_day)
    #
    his_data = ts.bar(stock[:6], conn=CONS, start_date=start_day, end_date=end_day)
    context.ref_his_data = his_data.drop(["code", "vol", "amount"], axis=1).sort_index()


def before_trading(context):
    yesterday = (context.now-timedelta(days=1)).strftime("%Y-%m-%d")
    for stock in context.stocks:
        # 获取历史数据
        opens  = history_bars(stock, context.TIME_PERIOD, '1d', 'open')
        highs  = history_bars(stock, context.TIME_PERIOD, '1d', 'high')
        lows   = history_bars(stock, context.TIME_PERIOD, '1d', 'low')
        closes = history_bars(stock, context.TIME_PERIOD, '1d', 'close')

        ref_opens = context.ref_his_data.loc[:yesterday]["open"]
        ref_highs = context.ref_his_data.loc[:yesterday]["high"]
        ref_lows = context.ref_his_data.loc[:yesterday]["low"]
        ref_closes = context.ref_his_data.loc[:yesterday]["close"]
        
        _verify_data("Test yesterday Open", opens, ref_opens.values[-context.TIME_PERIOD:])
        _verify_data("Test yesterday Highs", highs, ref_highs.values[-context.TIME_PERIOD:])
        _verify_data("Test yesterday Lows", lows, ref_lows.values[-context.TIME_PERIOD:])
        _verify_data("Test yesterday Closes", closes, ref_closes.values[-context.TIME_PERIOD:])


def handle_bar(context, bar_dict):
    today = context.now.strftime("%Y-%m-%d")
    for stock in context.stocks:
        # 获取历史数据
        opens  = history_bars(stock, context.TIME_PERIOD, '1d', 'open')
        highs  = history_bars(stock, context.TIME_PERIOD, '1d', 'high')
        lows   = history_bars(stock, context.TIME_PERIOD, '1d', 'low')
        closes = history_bars(stock, context.TIME_PERIOD, '1d', 'close')

        ref_opens = context.ref_his_data.loc[:today]["open"]
        ref_highs = context.ref_his_data.loc[:today]["high"]
        ref_lows = context.ref_his_data.loc[:today]["low"]
        ref_closes = context.ref_his_data.loc[:today]["close"]
        
        _verify_data("Test today Open", opens, ref_opens.values[-context.TIME_PERIOD:])
        _verify_data("Test today Highs", highs, ref_highs.values[-context.TIME_PERIOD:])
        _verify_data("Test today Lows", lows, ref_lows.values[-context.TIME_PERIOD:])
        _verify_data("Test today Closes", closes, ref_closes.values[-context.TIME_PERIOD:])


def _verify_data(attr_name, rq_data, ts_data):
    # 求方差
    if((rq_data-ts_data).var() > 1e-8):
        logger.error('''#%s# fail,
#rqalpha# %s
#tushare# is %s''' % (attr_name, rq_data, ts_data))

