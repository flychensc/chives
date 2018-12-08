from collections import Counter
from datetime import timedelta
from rqalpha.api import *
import tushare as ts
import talib


# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    context.stocks = [context.run_info.benchmark]
    # 设置要操作的股票池
    update_universe(context.stocks)

    # 设置这个策略当中会用到的参数，在策略中可以随时调用，这个策略使用长短均线，我们在这里设定长线和短线的区间，在调试寻找最佳区间的时候只需要在这里进行数值改动
    context.SHORTPERIOD = 5
    context.LONGPERIOD = 10
    
    # 使用MACD需要设置长短均线和macd平均线的参数
    context.SHORTPERIOD = 6
    context.LONGPERIOD = 13
    context.SMOOTHPERIOD = 5

    # KDJ
    context.FASTKPERIOD = 9
    context.SLOWKPERIOD = 3
    context.SLOWDPERIOD = 3

    context.TIME_PERIOD = 100

    # 符合几个条件才进行操作
    context.MATCH_CONDITION = 2
    
    # 获取历史数据
    _load_historys(context)

    # money back flag
    context.first = False
    context.buyed = False
    context.selled = True


def before_trading(context):
    # 开始编写你的主要的算法逻辑
    buy_hits = list()
    sell_hits = list()

    yesterday = (context.now-timedelta(days=1)).strftime("%Y-%m-%d")

    for stock in context.stocks:
        # TODO: 开始编写你的算法吧！

        historys = context.historys[stock].loc[:yesterday]

        # 因为策略需要用到均线，所以需要读取历史数据
        prices = historys['close'].values

        # 使用talib计算长短两根均线，均线以array的格式表达
        short_avg = talib.SMA(prices, context.SHORTPERIOD)
        long_avg = talib.SMA(prices, context.LONGPERIOD)

        # 如果短均线从上往下跌破长均线，也就是在目前的bar短线平均值低于长线平均值，而上一个bar的短线平均值高于长线平均值
        if short_avg[-1] - long_avg[-1] < 0 and short_avg[-2] - long_avg[-2] > 0:
            # 进行清仓
            sell_hits.append(stock)

        # 如果短均线从下往上突破长均线，为入场信号
        if short_avg[-1] - long_avg[-1] > 0 and short_avg[-2] - long_avg[-2] < 0:
            # 满仓入股
            buy_hits.append(stock)

        # 用Talib计算MACD取值，得到三个时间序列数组，分别为macd, signal 和 hist
        macd, signal, hist = talib.MACD(prices, context.SHORTPERIOD,
                                        context.LONGPERIOD, context.SMOOTHPERIOD)

        # macd 是长短均线的差值，signal是macd的均线，使用macd策略有几种不同的方法，我们这里采用macd线突破signal线的判断方法

        # 如果macd从上往下跌破macd_signal

        if macd[-1] - signal[-1] < 0 and macd[-2] - signal[-2] > 0:
            # 进行清仓
            sell_hits.append(stock)

        # 如果短均线从下往上突破长均线，为入场信号
        if macd[-1] - signal[-1] > 0 and macd[-2] - signal[-2] < 0:
            # 满仓入股
            buy_hits.append(stock)
            
        #用前一天的数据进行计算
        highs = historys['high'].values
        lows = historys['low'].values
        closes = historys['close'].values
        slowk, slowd = talib.STOCH(highs,
                            lows,
                            closes,
                            fastk_period=context.FASTKPERIOD,
                            slowk_period=context.SLOWKPERIOD,
                            slowk_matype=0,
                            slowd_period=context.SLOWDPERIOD,
                            slowd_matype=0)
        # 计算J值，J=3D-2K
        slowj = 3*slowd - 2*slowk

        if slowj[-1] - slowd[-1] < 0 and slowj[-2] - slowd[-2] > 0:
            # 进行清仓
            sell_hits.append(stock)

        if slowj[-1] - slowd[-1] > 0 and slowj[-2] - slowd[-2] < 0:
            # 满仓入股
            buy_hits.append(stock)

    # 整理，满足其二就选入
    context.buyable = set()
    context.sellable = set()
    for stock, count in Counter(sell_hits).items():
        if count >= context.MATCH_CONDITION:
            context.sellable.add(stock)
    for stock, count in Counter(buy_hits).items():
        if count >= context.MATCH_CONDITION:
            context.buyable.add(stock)


def handle_bar(context, bar_dict):
    # 取得当前的现金
    cash = context.portfolio.cash
    if cash < 0:
        logger.error("cash:%d" % (cash))

    for stock in context.stocks:
        if context.first:
            context.first = False
            context.buyed = True
            order_percent(stock, 0.8)
            # 记录建仓
            log_msg("建仓", stock, context, bar_dict)
            return
    
        if stock in context.sellable and context.buyed:
            context.buyed = False
            context.selled = True
            # 卖出
            order_target_percent(stock, 0.2)
            # 记录这次卖出
            log_msg("卖出", stock, context, bar_dict)
        if stock in context.buyable and context.selled:
            context.buyed = True
            context.selled = False
            # 买入
            order_target_percent(stock, 0.8)
            # 记录这次买入
            log_msg("买入", stock, context, bar_dict)


def _load_historys(context):   
    start_date = context.run_info.start_date
    end_date = context.run_info.end_date

    start_day = (start_date-timedelta(days=context.TIME_PERIOD)).strftime("%Y-%m-%d")
    end_day = end_date.strftime("%Y-%m-%d")

    #获取连接备用
    cons = ts.get_apis()

    context.historys = dict()
    for stock in context.stocks:
        logger.info("loading history [%(stock)s] %(start_day)s - %(end_day)s" % locals())
        code = stock[:6]
        history = ts.bar(code, conn=cons, adj='qfq', start_date=start_day, end_date=end_day)
        context.historys[stock] = history.drop(["code", "vol", "amount"], axis=1).sort_index()
    # 释放，否则python无法正常退出
    ts.close_apis(cons)


def log_msg(op, stock, context, bar_dict):
    price = bar_dict[stock].close
    shares = context.portfolio.positions[stock].quantity
    cash = context.portfolio.cash
    logger.info("[%s] %s on price %.2f. now shares has %d, cash has %.2f" 
        % (op, stock, price, shares, cash))
