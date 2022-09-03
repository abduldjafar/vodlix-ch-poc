from cmath import log
from clickhouse_driver import connect
from clickhouse_driver import errors
from clickhouse_driver.dbapi.errors import OperationalError as op_error
from queries.ddl import ddl
import os
import logging
from const import const

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)


class ClickhouseServer(object):
    def __init__(self):

        self.ch_host = os.environ["CH_HOST"]
        self.conn = None
        self.cursor = None
        self.query = ddl.Ddl()

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            logging.info("success execute query: %s" % query)

            return None

        except op_error as e:
            logging.error("error executing query: %s" % e)

            return errors

    def insert_data(self, query, data):
        try:
            self.cursor.execute(query, data)

            return (
                None,
                const.SUCCESS_EXECUTE_QUERY_CODE,
                const.SUCCESS_EXECUTE_QUERY_MESSAGE,
            )
        except op_error as e:
            logging.error("error insert datas: %s" % e)

            return (
                op_error,
                const.OPERATIONAL_ERROR_CODE,
                const.OPERATIONAL_ERROR_MESSAGE,
            )

    def init(self):
        try:
            self.conn = connect("clickhouse://" + self.ch_host)
            self.cursor = self.conn.cursor()
        except errors.Error as e:
            print(e.message)
