# coding=utf-8

__author__ = 'Shawn Chen'

import gevent.monkey
gevent.monkey.patch_all()

import logging
import datetime
import Analysis
import Store
import gevent
from pandas import ExcelWriter
from gevent.queue import Queue
from gevent.pool import Group

logger = logging.getLogger("chives")


def stocks_report(stocks, prefix='StockReport'):
    logger.info(u"股票分析 - Start")
    report_sheets = list()
    report_sheets.extend(Analysis.pattern_report_ex(stocks))
    index_report = Analysis.pattern_report_ex(['000001'], index=True)[0][1]
    report_sheets.append((u'上证指数', index_report))
    index_report = Analysis.pattern_report_ex(['000300'], index=True)[0][1]
    report_sheets.append((u'沪深300', index_report))

    filename = datetime.date.today().strftime(prefix + ' - %Y-%m-%d.xls')
    logger.info(u"结果存放于文件：%s" % filename)
    writer = ExcelWriter(filename)
    for (label, sheet) in report_sheets:
        sheet.to_excel(writer, label)
    writer.save()
    logger.info(u"股票分析 - End")


def smart_choose():
    logger.info(u"股票筛选 - Start")

    all_stocks = Store.query_all_stocks()
    logger.info(u"共%d只股票待处理" % all_stocks.index.values.size)

    stock_list = list()
    for code, df in Analysis.pattern_report(all_stocks.index.values):
        df = df[df > 0].dropna(axis='columns', how="all")
        for col in range(min(len(df), 3)):
            if df.iloc[col].count() > 0:
                logger.debug(u"股票%s(%s)在%s有%s个技术指标" % (all_stocks['name'][code], code, df.iloc[col].name, df.iloc[col].count()))
            if df.iloc[col].count() > 3:
                logger.info(u"股票%s %s符合要求" % (code, all_stocks['name'][code]))
                stock_list.append(code)
    logger.info(u"共筛选股票%d支" % len(stock_list))

    if len(stock_list):
        stocks_report(stock_list, 'RecommendedReport')

    logger.info(u"股票筛选 - End")


if __name__ == "__main__":
    # 创建logger
    logger = logging.getLogger("chives")
    logger.setLevel(logging.DEBUG)

    # 定义log格式
    log_format = logging.Formatter('%(asctime)s %(name)s:%(levelname)s %(message)s')

    # 创建控制台handler
    console_handle = logging.StreamHandler()
    console_handle.setLevel(logging.DEBUG)
    console_handle.setFormatter(log_format)

    # 创建一个handler，用于写入日志文件
    # file_handle = logging.FileHandler(datetime.date.today().strftime('%Y-%m-%d')+".log", 'w')
    # file_handle.setLevel(logging.DEBUG)
    # file_handle.setFormatter(log_format)

    # 注册handler
    logger.addHandler(console_handle)
    # logger.addHandler(file_handle)

    smart_choose()

    stock_ids = ['600779', '002182', '002195', '002271', '600326', '600009']
    stocks_report(stock_ids)
    pass

