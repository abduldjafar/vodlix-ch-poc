import queue
from queries.ddl import ddl
from config.config import Config


class DDLServices(object):
    def __init__(self):
        self.query = ddl.Ddl()
        self.config = Config()
        self.config.init()

    def create_database(self, db_name):
        query = self.query.create_database(db_name)
        self.config.execute_query(query)

    def create_table(self, db_name, tb_name):

        query = self.query.create_table(tb_name, db_name)
        insert_query_ddl = self.query.insert_into_ddl_history()

        self.config.execute_query(query)
        self.config.insert_data(
            insert_query_ddl, [{'db_name': db_name, 'tb_name': tb_name}]
        )

    def selec_data_from_table(self,db_name,tb_name,columns,limit,offset):
        
        query = self.query.select_datas(columns,db_name,tb_name,limit,offset)
        self.config.execute_query(query)
        datas = self.config.cursor.fetchall()

        return datas
