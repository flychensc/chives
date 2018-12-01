# coding=utf-8

__author__ = 'Shawn Chen'

from sqlalchemy import Column, CHAR, DATE, FLOAT, MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import logging
import pandas as pd
import Setting

logger = logging.getLogger("chives.Offline2.Store.StockBasic")

metaData = MetaData()

engine = create_engine(Setting.SQL_URL, echo=False)
DBSession = sessionmaker(bind=engine)

Base = declarative_base(metadata=metaData)


class StockBasic(Base):
    __tablename__ = 'stock_basics'

    code = Column(CHAR(6), primary_key=True)  # 代码
    name = Column(CHAR(8))    # 名称
    industry = Column(CHAR(8))    # 所属行业
    area = Column(CHAR(8))    # 地区

    pe = Column(FLOAT)  # 市盈率
    outstanding = Column(FLOAT)  # 流通股本
    totals = Column(FLOAT)  # 总股本(万)
    totalAssets = Column(FLOAT)  # 总资产(万)
    liquidAssets = Column(FLOAT)    # 流动资产
    fixedAssets = Column(FLOAT)  # 固定资产
    reserved = Column(FLOAT)    # 公积金
    reservedPerShare = Column(FLOAT)    # 每股公积金

    eps = Column(FLOAT)  # 每股收益
    bvps = Column(FLOAT)    # 每股净资
    pb = Column(FLOAT)  # 市净率

    timeToMarket = Column(DATE)  # 上市日期


metaData.create_all(engine)


def query_stock_basic(stock_id):
    session = DBSession()
    basic_data = session.query(StockBasic).filter(StockBasic.code == stock_id).one()
    session.close()
    basic_df = pd.DataFrame([[basic_data.code, basic_data.name, basic_data.industry, basic_data.area, basic_data.pe,
                           basic_data.outstanding, basic_data.totals, basic_data.totalAssets, basic_data.liquidAssets, basic_data.fixedAssets,
                           basic_data.reserved, basic_data.reservedPerShare, basic_data.eps, basic_data.bvps, basic_data.pb,
                           basic_data.timeToMarket]],
                          columns=['code', 'name', 'industry', 'area', 'pe',
                                   'outstanding', 'totals', 'totalAssets', 'liquidAssets', 'fixedAssets',
                                   'reserved', 'reservedPerShare', 'eps', 'bvps', 'pb',
                                   'timeToMarket'])
    basic_df.index = basic_df['code']
    return basic_df.drop(['code'], axis=1)


def query_all_stocks():
    session = DBSession()
    all_data = session.query(StockBasic).filter().all()
    session.close()
    all_df = pd.DataFrame(columns=['name', 'industry', 'area', 'pe',
                                   'outstanding', 'totals', 'totalAssets', 'liquidAssets',
                                   'fixedAssets', 'reserved', 'reservedPerShare', 'eps',
                                   'bvps', 'pb', 'timeToMarket'])
    for basic_data in all_data:
        all_df.loc[basic_data.code] = [basic_data.name, basic_data.industry, basic_data.area, basic_data.pe,
                                       basic_data.outstanding, basic_data.totals, basic_data.totalAssets, basic_data.liquidAssets,
                                       basic_data.fixedAssets, basic_data.reserved, basic_data.reservedPerShare, basic_data.eps,
                                       basic_data.bvps, basic_data.pb, basic_data.timeToMarket]
    return all_df


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

    # 注册handler
    logger.addHandler(console_handle)

    print(query_stock_basic('002356'))
    print(query_all_stocks())

