class Ddl(object):
    def __init__(self):
        pass

    def create_database(self, db_name):
        return """
        CREATE DATABASE {}
        """.format(
            db_name
        )

    def create_table_session(self, tb_name,db_name):
        return """
        CREATE TABLE IF NOT EXISTS {}.{} (
            userid Nullable(Int32),
            session_id Int32,
            ip Nullable(String),
            user_agent Nullable(String),
            browser Nullable(String),
            browser_version Nullable(Int32),
            os Nullable(String),
            os_version Nullable(String),
            device Nullable(String),
            device_name Nullable(String),
            country Nullable(String),
            region Nullable(String),
            city Nullable(String),
            latitude Nullable(Float32),
            longitude Nullable(Float32),
            isp Nullable(String),
            internet_speed Nullable(Int32),
            app Nullable(String),
            app_version Nullable(Int32),
            app_id Nullable(String)


        ) ENGINE = MergeTree order by session_id  partition by session_id;
        """.format(
            db_name, tb_name
        )

    def create_view_default_table_joined_sessions_table(self,db_name, tb_name):
        return """
        create view {}.view_{} as SELECT * EXCEPT `b.session_id`
        FROM
        (
            SELECT *
            FROM {}.{} as a
            INNER JOIN {}.tb_sessions  as b ON a.session_id = b.session_id
        )
        """.format(db_name,tb_name,db_name,tb_name,db_name)
    def create_default_table(self, table_name, db_name):
        return """
        CREATE TABLE IF NOT EXISTS {}.{} (
            object_id Nullable(Int32),
            object_type Nullable(Int32),
            object_url Nullable(String),
            content_length Nullable(String),
            title Nullable(String),
            collection_id Nullable(Int32),
            content_list_id Nullable(Int32),
            partner_id Nullable(Int32),
            event_type Nullable(String),
            event_value Nullable(Int32),
            bitrate Nullable(Int32),
            width Nullable(Int32),
            height Nullable(Int32),
            utm_source Nullable(String),
            utm_term Nullable(String),
            utm_id Nullable(Int32),
            utm_medium Nullable(String),
            referrer Nullable(String),
            referrer_path Nullable(String),
            session_id Int32


        ) ENGINE = MergeTree order by session_id  partition by session_id;
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
            is_default Bool,
            created_at DateTime('Asia/Istanbul')
        ) ENGINE = MergeTree order by db_name  partition by db_name;
        """

    def insert_into_ddl_history(self):
        return "INSERT INTO ddl.history (db_name,tb_name,is_default) VALUES "
    
    def delete_table(self,db_name, table_name):
        return "DROP TABLE  {}.{}".format(db_name, table_name)

    def select_datas(self,columns,db_name, table_name,limit,offset):
        columns = ",".join(columns)
        return "SELECT {} FROM {}.{} LIMIT {},{}".format(columns,db_name,table_name,offset,limit)
    
    def select_datas_where(self,columns,db_name, table_name,limit,offset,wheres):
        wheres = "where "+ " and ".join(wheres) if len(wheres) > 0 else ""
        columns = ",".join(columns)
        return "SELECT {} FROM {}.{} {} LIMIT {},{}".format(columns,db_name,table_name,wheres,offset,limit)
    
    def insert_data_to_table(self,db_name,tb_name,columns):
        return "INSERT INTO {}.{} {} VALUES ".format(db_name,tb_name,columns)
    
    def add_new_column(self,tb_name,column_name,data_type):
        return "ALTER TABLE {} ADD COLUMN {} {} FIRST".format(tb_name,column_name,data_type)
    
    def delete_column(self,tb_name,column_name):
        return "ALTER TABLE {} DROP COLUMN {}".format(tb_name,column_name)
    
    def use_db(self,db_name):
        return "use {};".format(db_name)
