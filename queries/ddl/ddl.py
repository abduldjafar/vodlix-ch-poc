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
            object_id Int32,
            object_type Int32,
            object_url String,
            content_length String,
            title String,
            collection_id Int32,
            content_list_id Int32,
            partner_id Int32,
            event_type String,
            event_value Int32,
            bitrate Int32,
            width Int32,
            height Int32,
            app String,
            app_version Int32,
            app_id String,
            utm_source String,
            utm_term String,
            utm_id Int32,
            utm_medium String,
            referrer String,
            referrer_path String,
            userid Int32,
            session_id Int32,
            ip String,
            user_agent String,
            browser String,
            browser_version Int32,
            os String,
            os_version String,
            device String,
            device_name String,
            country String,
            region String,
            city String,
            latitude String,
            longitude String,
            isp String,
            internet_speed Int32


        ) ENGINE = MergeTree order by userid  partition by userid;
        """.format(
            db_name, table_name
        )
    
    def create_table_with_columns(self, table_name, db_name,columns,order_by):
        return """
        CREATE TABLE {}.{} (
            {}
        ) ENGINE = MergeTree order by {} ;
        """.format(
            db_name, table_name,columns,order_by
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
        return "DROP TABLE  {}.{}".format(db_name, table_name)

    def select_datas(self,columns,db_name, table_name,limit,offset):
        columns = ",".join(columns)
        return "SELECT {} FROM {}.{} LIMIT {},{}".format(columns,db_name,table_name,offset,limit)
    
    def insert_data_to_table(self,db_name,tb_name,columns):
        return "INSERT INTO {}.{} {} VALUES ".format(db_name,tb_name,columns)
    
    def add_new_column(self,tb_name,column_name,data_type):
        return "ALTER TABLE {} ADD COLUMN {} {} FIRST".format(tb_name,column_name,data_type)
    
    def delete_column(self,tb_name,column_name):
        return "ALTER TABLE {} DROP COLUMN {}".format(tb_name,column_name)
    
    def use_db(self,db_name):
        return "use {};".format(db_name)
