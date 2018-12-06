# coding=utf-8

__author__ = 'Shawn Chen'

import gevent.monkey
gevent.monkey.patch_all()

import logging
import datetime
import tushare as ts

logger = logging.getLogger("chives.Analysis.ReferenceReport")


def profit_report(year):
    logger.info(u"分配预案 - Start")

    # 最新60条
    df = ts.profit_data(year=year, top=60)

    filename = '%s分配预案%s' % (year, datetime.date.today().strftime(' - %Y-%m-%d.xls'))
    logger.info(u"结果存放于文件：%s" % filename)
    df.to_excel(filename)

    logger.info(u"分配预案 - End")


def forecast_report(year, q=2):
    logger.info(u"业绩预告 - Start")

    df=ts.forecast_data(year, q)
    filename = '%sQ%d业绩预告%s' % (year, q, datetime.date.today().strftime(' - %Y-%m-%d.xls'))
    logger.info(u"结果存放于文件：%s" % filename)
    df.to_excel(filename)

    logger.info(u"业绩预告 - End")


def xsg_report():
    logger.info(u"限售股解禁 - Start")

    df=ts.xsg_data()
    filename = '限售股解禁%s' % datetime.date.today().strftime(' - %Y-%m-%d.xls')
    logger.info(u"结果存放于文件：%s" % filename)
    df.to_excel(filename)

    logger.info(u"限售股解禁 - End")


def fund_report(year, q=2):
    logger.info(u"基金持股 - Start")

    df=ts.fund_holdings(year, q)
    filename = '%sQ%d基金持股%s' % (year, q, datetime.date.today().strftime(' - %Y-%m-%d.xls'))
    logger.info(u"结果存放于文件：%s" % filename)
    df.to_excel(filename)

    logger.info(u"基金持股 - End")


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

    # 分配预案
    profit_report('2017')
    # 业绩预告
    forecast_report(2017, 2)
    # 限售股解禁
    xsg_report()
    # 基金持股
    fund_report(2017, 2)

