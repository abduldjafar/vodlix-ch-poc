from distutils import errors
from queries.ddl import ddl
from servers.clickhouse import ClickhouseServer


class CoreServices(object):
    def __init__(self):
        self.query = ddl.Ddl()
        self.db = ClickhouseServer()
        self.db.init()

    def insert_datas(self, payload, tb_name, db_name):
        datas = payload["data"]
        columnns = "({})".format(",".join(datas.keys()))
        insert_datas_query = self.query.insert_data_to_table(
            db_name=db_name, tb_name=tb_name, columns=columnns
        )
        (error, code, message) = self.db.insert_data(insert_datas_query, [datas])
        return error, code, message
    
    def insert_sessions(self, payload, tb_name, db_name):
        datas = payload["data"]
        columnns = "({})".format(",".join(datas.keys()))
        insert_datas_query = self.query.insert_data_to_table(
            db_name=db_name, tb_name=tb_name, columns=columnns
        )
        (error, code, message) = self.db.insert_data(insert_datas_query, [datas])
        return error, code, message

    def insert_event(self, payload, tb_name, db_name):
        datas = payload["data"]
        columnns = "({})".format(",".join(datas.keys()))
        insert_datas_query = self.query.insert_data_to_table(
            db_name=db_name, tb_name=tb_name, columns=columnns
        )
        (error, code, message) = self.db.insert_data(insert_datas_query, [datas])
        return error, code, message
    
    def insert_source(self, payload, tb_name, db_name):
        datas = payload["data"]
        columnns = "({})".format(",".join(datas.keys()))
        insert_datas_query = self.query.insert_data_to_table(
            db_name=db_name, tb_name=tb_name, columns=columnns
        )
        (error, code, message) = self.db.insert_data(insert_datas_query, [datas])
        return error, code, message