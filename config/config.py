from clickhouse_driver import connect
from queries.ddl import ddl
import os


class Config(object):
    def __init__(self):

        self.ch_host = os.environ['CH_HOST']
        self.conn = None
        self.cursor = None
        self.query = ddl.Ddl()

    def execute_query(self, query):
        self.cursor.execute(query)

    def insert_data(self, query, data):
        self.cursor.execute(query, data)

    def init(self):

        self.conn = connect("clickhouse://" + self.ch_host)
        self.cursor = self.conn.cursor()
        self.cursor.execute(self.query.create_ddl_database())
        self.cursor.execute(self.query.create_ddl_history_table())
