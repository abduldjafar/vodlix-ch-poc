class Ddl(object):
    def __init__(self):
        pass

    def create_database(self, db_name):
        return """
        CREATE DATABASE {}
        """.format(
            db_name
        )

    def create_table(self, table_name, db_name):
        return """
        CREATE TABLE {}.{} (
            x String,
            y String
        ) ENGINE = MergeTree order by x ;
        """.format(
            db_name, table_name
        )

    def create_ddl_database(self):
        return """
        CREATE DATABASE IF NOT EXISTS ddl;
        """

    def create_ddl_history_table(self):
        return """
        CREATE TABLE IF NOT EXISTS ddl.history (
            db_name String,
            tb_name String,
            created_at DateTime('Asia/Istanbul')
        ) ENGINE = MergeTree order by db_name  partition by db_name;
        """

    def insert_into_ddl_history(self):
        return "INSERT INTO ddl.history (db_name,tb_name) VALUES "
    
    def delete_table(self,db_name, table_name):
        return "DROP TABLE IF EXIST {}.{}".format(db_name, table_name)

    def select_datas(self,columns,db_name, table_name,limit,offset):
        columns = ",".join(columns)
        return "SELECT {} FROM {}.{} LIMIT {},{}".format(columns,db_name,table_name,offset,limit)
