from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import *
from datetime import datetime, timedelta
import csv

engine = create_engine('sqlite:///spx_out.db')
Base = declarative_base()

class Spx(Base):
    __tablename__ = 'spx'
    TableId = Column(Integer, primary_key=True)
    DateId = Column(String)
    Open = Column(Float)
    High = Column(Float)
    Low = Column(Float)
    AdjClose = Column(Float)
    Volume = Column(Integer)
Spx.__table__.create(bind=engine, checkfirst=True)

rowlist = []
with open('spx.csv', 'r') as f:
    reader = csv.DictReader(f)
    for line in reader:
        row = {}
        newdate = datetime.strptime(line['Date'], '%Y-%m-%d').strftime('%Y-%m-%d')
        row['DateId'] = newdate
        row['Open'] = line['Open']
        row['High'] = line['High']
        row['Low']  = line['Low']
        row['AdjClose'] = line['Adj Close']
        row['Volume'] = line['Volume']
        rowlist.append(row)

Session = sessionmaker(bind=engine)
session = Session() 
for item in rowlist:
    row = Spx(**item)
    session.add(row)
session.commit()