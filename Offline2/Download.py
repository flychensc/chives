# coding=utf-8

__author__ = 'Shawn Chen'

import gevent.monkey
gevent.monkey.patch_all()

import sys
sys.path.append(".")

import logging
import datetime
import socket
import sqlalchemy
import tushare as ts
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import Setting
from gevent.queue import Queue
from gevent.pool import Group
import gevent.monkey

logger = logging.getLogger("chives.Offline2.Download")

engine = create_engine(Setting.SQL_URL, echo=False)

START_DATE = (datetime.date.today()-datetime.timedelta(days=30*6)).strftime("%Y-%m-%d")

sql_type = {
    "stock_basics": {
        'code': sqlalchemy.CHAR(6),  # 代码
        'name': sqlalchemy.CHAR(8),  # 名称
        'industry': sqlalchemy.CHAR(8),	 # 所属行业
        'area': sqlalchemy.CHAR(8),  # 地区
        'pe': sqlalchemy.FLOAT,  # 市盈率
        'outstanding': sqlalchemy.FLOAT,  # 流通股本
        'totals': sqlalchemy.FLOAT,  # 总股本(万)
        'totalAssets': sqlalchemy.FLOAT,  # 总资产(万)
        'liquidAssets': sqlalchemy.FLOAT,  # 流动资产
        'fixedAssets': sqlalchemy.FLOAT,  # 固定资产
        'reserved': sqlalchemy.FLOAT,  # 公积金
        'reservedPerShare': sqlalchemy.FLOAT,  # 每股公积金
        'eps': sqlalchemy.FLOAT,  # 每股收益
        'bvps': sqlalchemy.FLOAT,  # 每股净资
        'pb': sqlalchemy.FLOAT,  # 市净率
        'timeToMarket': sqlalchemy.DATE,  # 上市日期
    },

    "history": {
        'code': sqlalchemy.CHAR(6),  # 代码
        'date': sqlalchemy.DATE,  # 交易日期

        'open': sqlalchemy.FLOAT,  # 开盘价
        'high': sqlalchemy.FLOAT,  # 最高价
        'close': sqlalchemy.FLOAT,  # 收盘价
        'low': sqlalchemy.FLOAT,  # 最低价

        'volume': sqlalchemy.INTEGER,  # 成交量
    },
}


def _boss(boss_q, worker_q, product_q, his_list):
    # 1. 遍历所有股票
    defective = 0
    raw_total = boss_q.qsize()
    logger.info(u"共%d支股票需要处理" % raw_total)
    while raw_total != product_q.qsize() + defective:
        if not boss_q.empty():
            [status, (stock_id, info)] = boss_q.get()
            logger.debug(u"%s:%s %s" % (status, stock_id, info['name']))

            if status == "ok" or status == "timeout":
                worker_q.put((stock_id, info))
            else:
                defective += 1
            gevent.sleep(0)
        else:
            logger.info(u"已处理%d/%d支股票" % (product_q.qsize(), raw_total))
            logger.debug(u"Boss闲置中, boss_q.qsize：%d, worker_q.qsize：%d, product_q.qsize：%d"
                         % (boss_q.qsize(), worker_q.qsize(), product_q.qsize()))
            gevent.sleep(5)
    logger.info(u"共%d支股票完成处理" % product_q.qsize())

    # collect history
    while not product_q.empty():
        his_list.append(product_q.get())
    logger.debug(u"Boss收工")


def _worker(boss_q, worker_q, product_q, no):
    while not worker_q.empty() or not boss_q.empty() or not product_q.empty():
        if not worker_q.empty():
            try:
                (stock_id, info) = worker_q.get(timeout=1)
                logger.debug(u"Worker%d get %s %s" % (no, stock_id, info['name']))
            except gevent.queue.Empty:
                logger.debug(u"Worker%d取queue超时" % no)
                continue

            try:
                # 获取历史数据
                his_data = ts.get_k_data(stock_id, start=START_DATE)
                #reverse
                his_data = his_data.reindex(index=his_data.index[::-1])
                # string to float
                his_data['open'] = his_data['open'].apply(lambda x: float(x))
                his_data['high'] = his_data['high'].apply(lambda x: float(x))
                his_data['low'] = his_data['low'].apply(lambda x: float(x))
                his_data['close'] = his_data['close'].apply(lambda x: float(x))
                his_data['volume'] = his_data['volume'].apply(lambda x: float(x))
                his_data['date'] = his_data['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
                # trans to MultiIndex
                his_data.index = [his_data['code'], his_data['date']]
                # remove duplicated column 'code'
                his_data = his_data.drop(['code', 'date'], axis=1)

                logger.debug(u"Worker%d完成股票%s %s的工作" % (no, stock_id, info['name']))
                product_q.put(his_data)
            except socket.timeout:
                logger.warn(u"Worker%d处理股票%s %s捕获异常:%s" % (no, stock_id, info['name'], "timeout"))
                boss_q.put(["timeout", (stock_id, info)])
            except Exception as e:
                logger.warn(u"Worker%d处理股票%s %s捕获异常:%s" % (no, stock_id, info['name'], e))
                boss_q.put([e, (stock_id, info)])
            gevent.sleep(0)
        else:
            logger.debug(u"Worker%d闲置中, boss_q.qsize：%d, worker_q.qsize：%d, product_q.qsize：%d"
                         % (no, boss_q.qsize(), worker_q.qsize(), product_q.qsize()))
            gevent.sleep(3)
    logger.debug(u"Worker%d收工" % no)
    pass


def start():
    logger.info(u"离线数据更新 - Start")
    # 更新股票基础信息
    with engine.connect() as conn, conn.begin():
        stock_basics = ts.get_stock_basics()
        # START
        if stock_basics['esp'].dtype == np.dtype('float64'):
            # rename 'eps' to 'esp'
            stock_basics["eps"] = stock_basics["esp"]
        else:
            # convert 'eps'
            # as I found 'esp' field was '0.147㈡' at Feb.26.2016
            # It cause SQL server error.
            logger.warn(u"'esp'非浮点类型")
            def _atof(str):
                try:
                    return float(str)
                except ValueError:
                    # I found 'esp' field was '0.000㈣' at Nov.8.2016
                    return float(str[:-1])
            stock_basics["eps"] = stock_basics["esp"].apply(_atof)
        stock_basics = stock_basics.drop("esp", axis=1)
        # drop timeToMarket is zero
        stock_basics = stock_basics[stock_basics['timeToMarket']!=0]
        # change sql type
        stock_basics['timeToMarket'] = stock_basics['timeToMarket'].apply(lambda x:datetime.datetime.strptime(str(x), "%Y%m%d").date())
        # END
        stock_basics.to_sql("stock_basics", conn,
                            if_exists='replace',
                            dtype=sql_type["stock_basics"])

    # 更新历史数据(并发处理)
    group = Group()
    worker_q = Queue()
    boss_q = Queue()
    product_q = Queue()
    his_list = list()

    logger.debug(u"装载全部股票代码")
    for stock_id, info in stock_basics.iterrows():
        boss_q.put(["ok", (stock_id, info)])

    group.add(gevent.spawn(_boss,
                           boss_q=boss_q, worker_q=worker_q, product_q=product_q,
                           his_list=his_list))

    for i in range(10):
        group.add(gevent.spawn(_worker,
                               boss_q=boss_q, worker_q=worker_q, product_q=product_q,
                               no=i))
    group.join()

    # merge history
    # 获取历史数据
    with engine.connect() as conn, conn.begin():
        pd.concat(his_list).to_sql("history", conn,
                                   if_exists='replace',
                                   index_label=['code', 'date'],
                                   dtype=sql_type["history"])
    logger.info(u"离线数据更新 - End")
    pass


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

    start()
    pass
