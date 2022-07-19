from cmath import log
from clickhouse_driver import connect
from clickhouse_driver import errors
from queries.ddl import ddl
import os
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


class ClickhouseServer(object):
    def __init__(self):

        self.ch_host = os.environ['CH_HOST']
        self.conn = None
        self.cursor = None
        self.query = ddl.Ddl()

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            logging.info('success execute query: %s' % query)

        except errors as e:
            logging.error('error executing query: %s' % e)

    def insert_data(self, query, data):
        self.cursor.execute(query, data)

    def init(self):
        try:
            self.conn = connect('clickhouse://' + self.ch_host)
            self.cursor = self.conn.cursor()
        except Error as e:
            print(e.message)
