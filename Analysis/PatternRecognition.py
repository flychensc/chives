# coding=utf-8

__author__ = 'Shawn Chen'

import gevent.monkey
gevent.monkey.patch_all()

import logging
import datetime
import talib
import socket
import Store
import gevent
import requests
import tushare as ts
import pandas as pd
from pandas import ExcelWriter
from multiprocessing import freeze_support, Pool
from gevent.queue import Queue
from gevent.pool import Group

logger = logging.getLogger("chives.Analysis.PatternRecognition")

START_DATE = (datetime.date.today()-datetime.timedelta(days=30*6)).strftime("%Y-%m-%d")

def _pattern_recognition(quotes):
    for method in talib.get_function_groups()["Pattern Recognition"]:
        quotes[method] = getattr(talib, method)(quotes['open'].values,
                                                quotes['high'].values,
                                                quotes['low'].values,
                                                quotes['close'].values)
    # remove quotes
    return quotes.drop(['open', 'high', 'low', 'close'], axis=1)


def _worker(stock_id):
    quotes = Store.query_his_data(stock_id).head(20)
    if quotes.size == 0:
        print(u"股票%s历史数据为空" % stock_id)
        # logger.warn(u"股票%s历史数据为空" % stock_id)
        return pd.DataFrame()
    return _pattern_recognition(quotes.drop(['volume'], axis=1))


def pattern_report(stock_list):
    logger.info(u"技术面分析 - Start")
    logger.info(u"共%d只股票待处理" % len(stock_list))

    freeze_support()
    pool = Pool(processes=2)
    async_result = [pool.apply_async(_worker, (stock_id,)) for stock_id in stock_list]
    pool.close()
    pool.join()

    logger.info(u"%d只股票已完成处理" % len(async_result))

    # 获取结果并过滤
    pattern_list = [r.get() for r in async_result if not r.get().empty]
    logger.info(u"其中有效股票共%d只" % len(pattern_list))

    report = list()
    for df in pattern_list:
        # reset index
        df = df.reset_index()
        code = df['code'].iloc[0]
        # 在index操作前，转换date为str
        df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        # new index
        df.index = df['date']
        df = df.drop(['code', 'date'], axis=1)
        # drop NA columns
        df = df[df != 0].dropna(axis='columns', how='all')
        report.append((code, df))

    logger.info(u"技术面分析 - End")
    return report


def start_to_pattern(stock_list):
    sheets = pattern_report(stock_list)

    # store
    filename = datetime.date.today().strftime('PatternReport - %Y-%m-%d.xls')
    logger.info(u"结果存放于文件：%s" % filename)

    writer = ExcelWriter(filename)
    for (label, sheet) in sheets:
        sheet.to_excel(writer, label)
    writer.save()
    pass


def _worker_ex(worker_q, product_q, no):
    while not worker_q.empty():
        try:
            (status, stock_id, index) = worker_q.get(timeout=1)
            logger.debug(u"Worker%d get %s [%s] index is %s" % (no, stock_id, status, index))
        except gevent.queue.Empty:
            logger.debug(u"Worker%d取queue超时" % no)
            continue

        try:
            # 获取复权数据
            quotes = ts.get_k_data(stock_id, start=START_DATE, index=index)
            #reverse
            quotes = quotes.reindex(index=quotes.index[::-1]).head(20)
            # 计算
            if quotes.size == 0:
                print(u"股票%s复权数据为空" % stock_id)
                logger.warn(u"股票%s复权数据为空" % stock_id)
                report_data = pd.DataFrame()
            else:
                # string to float
                quotes['open'] = quotes['open'].apply(lambda x: float(x))
                quotes['high'] = quotes['high'].apply(lambda x: float(x))
                quotes['low'] = quotes['low'].apply(lambda x: float(x))
                quotes['close'] = quotes['close'].apply(lambda x: float(x))

                report_data = _pattern_recognition(quotes.drop(['volume'], axis=1))
                report_data['code'] = stock_id

            logger.debug(u"Worker%d完成股票%s的工作" % (no, stock_id))
            product_q.put(report_data)
        except socket.timeout:
            logger.warn(u"Worker%d处理股票%s捕获异常:%s" % (no, stock_id, "timeout"))
            worker_q.put(("timeout", stock_id, index))
        except OSError:
            logger.warn(u"Worker%d处理股票%s捕获异常:%s" % (no, stock_id, "OSError"))
            worker_q.put(("OSError", stock_id, index))
        gevent.sleep(0)
    else:
        logger.debug(u"Worker%d闲置中, worker_q.qsize：%d, product_q.qsize：%d"
                        % (no, worker_q.qsize(), product_q.qsize()))
        gevent.sleep(3)
    logger.debug(u"Worker%d收工" % no)
    pass


def pattern_report_ex(stock_list, index=False):
    logger.info(u"技术面分析 - Start")
    logger.info(u"共%d只股票待处理" % len(stock_list))

    # 更新基本面(并发处理)
    group = Group()
    worker_q = Queue()
    product_q = Queue()

    for stock_id in stock_list:
        worker_q.put(("ok", stock_id, index))

    for i in range(min(10, worker_q.qsize())):
        group.add(gevent.spawn(_worker_ex,
                               worker_q=worker_q, product_q=product_q,
                               no=i))
    group.join()

    # collect history
    pattern_list = list()
    while not product_q.empty():
        report_data = product_q.get()
        if not report_data.empty:
            pattern_list.append(report_data)
    logger.info(u"其中有效股票共%d只" % len(pattern_list))

    report = list()
    for df in pattern_list:
        code = df['code'].iloc[0]

        # new index
        df.index = df['date']
        df = df.drop(['code', 'date'], axis=1)
        # drop NA columns
        df = df[df != 0].dropna(axis='columns', how='all')
        report.append((code, df))

    logger.info(u"技术面分析 - End")
    return report


def start_to_pattern_ex(stock_list):
    sheets = pattern_report_ex(stock_list)

    # store
    filename = datetime.date.today().strftime('PatternExReport - %Y-%m-%d.xls')
    logger.info(u"结果存放于文件：%s" % filename)

    writer = ExcelWriter(filename)
    for (label, sheet) in sheets:
        sheet.to_excel(writer, label)
    writer.save()
    pass


if __name__ == "__main__":
    # 创建logger
    logger = logging.getLogger("chives")
    logger.setLevel(logging.DEBUG)

    # 定义log格式
    log_format = logging.Formatter(u'%(asctime)s %(name)s:%(levelname)s %(message)s')

    # 创建控制台handler
    console_handle = logging.StreamHandler()
    console_handle.setLevel(logging.DEBUG)
    console_handle.setFormatter(log_format)

    # 注册handler
    logger.addHandler(console_handle)

    stock_ids = ['601390', '601668', '600694', '600048', '601398']
    start_to_pattern(stock_ids)

    start_to_pattern_ex(stock_ids)
