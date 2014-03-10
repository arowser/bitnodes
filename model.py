# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, BigInteger
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
import ipaddr
import logging

#logging.basicConfig(level=logging.DEBUG, filename='exc.log')

def my_con_func():
    import sqlite3.dbapi2 as sqlite
    con = sqlite.connect("btc.db")
    #con.text_factory=str
    return con

#engine = create_engine("sqlite:///",creator=my_con_func, poolclass=NullPool)
#engine = create_engine('postgresql://postgres:user@127.0.0.1:5432/pass')

SQLBase = declarative_base()
metadata = MetaData()
Session = sessionmaker(bind=engine)

table = Table('node', metadata, 
              Column('ip', BigInteger, primary_key=True), 
              Column('port', Integer),
              Column('version', Integer),
              Column('height', Integer),
              Column('timestamp', Integer),
              Column('services', Integer, default=0), # node network service: This node can be asked for full blocks instead of just headers
              Column('agent', String),
              Column('country', String),
              Column('ipv6', Integer, default=0) 
              )

metadata.create_all(engine)

class Node(SQLBase):
    __tablename__ = 'node'
    ip = Column(BigInteger, primary_key=True)
    port = Column(Integer)
    version = Column(Integer)
    height  = Column(Integer)
    timestamp  = Column(Integer)
    services = Column(Integer)
    agent = Column(String)
    country = Column(String)
    ipv6  = Column(Integer)

    def save(self):
        try:
            session = Session()
            session.add(self)
            session.commit()
        except:
            logging.exception("model:")

    def __repr__(self):
        return "<('ip %d')>" % (self.ip)

#add node from version message
#update it if node exists 
def addNode(msg):
    try:

       if  msg['from_addr']['ipv4'] != '':
           ip = int(ipaddr.IPv4Address(msg['from_addr']['ipv4']))
       elif  msg['from_addr']['ipv6'] != '':
           ip = int(ipaddr.IPv6Address(msg['from_addr']['ipv6']))
       else:
          logging.exception("NoIpNode:")
          logging.exception(msg)
          return

       session = Session()
       node = session.query(Node).filter_by(ip=ip).first()
       if not node:
           node = Node()

       node.ip = ip
       node.port = msg['from_addr']['port']
       node.version = msg['version']
       node.height = msg['start_height']
       node.timestamp = msg['timestamp']
       node.services = msg['services']
       node.agent = msg['user_agent']

       session.add(node)
       session.commit()

    except:
       logging.exception("AddNode:")
 
#add node from address message
def addaddr(msg):
    try:
       if  msg['ipv4'] != '':
           ip = int(ipaddr.IPv4Address(msg['ipv4']))
       elif  msg['ipv6'] != '':
           ip = int(ipaddr.IPv6Address(msg['ipv6']))
       else:
          logging.exception("NoIpNode:")
          logging.exception(msg)
          return

       session = Session()
       node = session.query(Node).filter_by(ip=ip).first()
       if not node:
          node = Node()
       else:
          return
 
       node.port = msg['port']
       node.timestamp = msg['timestamp']
       node.services = msg['services']
 
       session.add(node)
       session.commit()

    except:
       logging.exception("AddAddr:")
 
