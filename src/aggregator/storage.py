
from time import sleep

from thrift.transport.TTransport import TTransportException
from pycassa.system_manager import *
from pycassa.pool import ConnectionPool, AllServersUnavailable
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException, InvalidRequestException

KEYSPACE = 'news'
NEWS_COLUMN_FAMILY = 'all_news'
SITES_COLUMN_FAMILY = 'site_news'


class StorageCassandra:
    def __init__(self, server_list=['localhost:9160'], expireTime=30):
        self.sys = None
        self.pool = None

        while True:
            try:
                self.sys = SystemManager(server_list[0])
                self.pool = ConnectionPool(
                    KEYSPACE, server_list, pool_size=15)
                break
            except AllServersUnavailable:
                print("None of the servers can be connected to. " \
                      "Waiting on connection pool for 1 second...")
                sleep(1)
            except TTransportException:
                print('Could not connect to a Cassandra node:' % server_list[0])
                sleep(1)
            except InvalidRequestException:
                self.sys.create_keyspace(KEYSPACE, SIMPLE_STRATEGY, {'replication_factor': '1'})
                self.pool = ConnectionPool(
                    KEYSPACE, server_list, pool_size=15)

        try:
            self.news = ColumnFamily(self.pool, NEWS_COLUMN_FAMILY)
        except NotFoundException:
            print('Create column family:', NEWS_COLUMN_FAMILY)
            try:
                self.sys.create_column_family(
                    KEYSPACE, NEWS_COLUMN_FAMILY)
            except InvalidRequestException, exc:
                if exc.why.startswith('Cannot add already existing column family'):
                    pass
                else:
                    raise exc

            self.news = ColumnFamily(self.pool, NEWS_COLUMN_FAMILY)
        
        try:
            self.site_news = ColumnFamily(self.pool, SITES_COLUMN_FAMILY)
        except NotFoundException:
            print('Create column family', SITES_COLUMN_FAMILY)
            try:
                self.sys.create_column_family(
                    KEYSPACE, SITES_COLUMN_FAMILY)
            except InvalidRequestException, exc:
                if exc.why.startswith('Cannot add already existing column family'):
                    pass
                else:
                    raise exc

            self.site_news = ColumnFamily(self.pool, SITES_COLUMN_FAMILY)

        sec_per_day = 86400
        self.ttl = int(expireTime) * sec_per_day
      
    def storeNews(self, rows):
        print rows
        self.news.batch_insert(rows, ttl=self.ttl)
    
    def storeSiteNews(self, rows):
        self.site_news.batch_insert(rows, ttl=self.ttl)

    def getSiteNews(self, key):
        try:
            record = self.news.get(key)
            #print record
            return record
        except:
            print("missing record:", self.news, key)
            return None
    def getNews(self,key):
        try:
            record = self.site_news.get(key)
            return record
        except:
            print("missing record:", self.site_news, key)
            return None
