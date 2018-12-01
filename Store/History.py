# coding=utf-8

__author__ = 'Shawn Chen'

from sqlalchemy import Column, CHAR, DATE, FLOAT, INTEGER, UniqueConstraint, Index, MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import logging
import pandas as pd
import Setting

logger = logging.getLogger("chives.Offline2.Store.History")

metaData = MetaData()

engine = create_engine(Setting.SQL_URL, echo=False)
DBSession = sessionmaker(bind=engine)

Base = declarative_base(metadata=metaData)


class History(Base):
    __tablename__ = 'history'

    code = Column(CHAR(6), primary_key=True, index=True)
    date = Column(DATE, primary_key=True, index=True)   # 交易日期

    open = Column(FLOAT)    # 开盘价
    high = Column(FLOAT)    # 最高价
    close = Column(FLOAT)   # 收盘价
    low = Column(FLOAT)     # 最低价

    volume = Column(INTEGER)    # 成交量

    stock_his_ID = UniqueConstraint('code', 'date')

    code_idx = Index('code')
    date_idx = Index('date')


metaData.create_all(engine)


def query_his_data(stock_id):
    session = DBSession()
    his_data = [[data.code, data.date, data.open, data.high, data.close, data.low, data.volume] for data in session.query(History).filter(History.code == stock_id).all()]
    session.close()
    his_df = pd.DataFrame(his_data, columns=['code', 'date', 'open', 'high', 'close', 'low', 'volume'])
    his_df.index = [his_df['code'], his_df['date']]
    return his_df.drop(['code', 'date'], axis=1)

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

    session = DBSession()

    all_data = session.query(History).filter(History.code == '601668').all()
    print(all_data[0].code, all_data[0].date)
    print(all_data[-1].code, all_data[-1].date)

    session.close()
