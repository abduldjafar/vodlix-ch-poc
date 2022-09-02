class Ddl(object):
    def __init__(self):
        pass

    def create_database(self, db_name):
        return """
        CREATE DATABASE {}
        """.format(
            db_name
        )

    def create_stats_database(self, db_name):
        return """
        CREATE DATABASE {}_stats
        """.format(
            db_name
        )

    def create_table_sources(self, tb_name, db_name):
        return """
        CREATE TABLE IF NOT EXISTS {}.{} (
            source_id Int32,
            utm_source Nullable(String),
            utm_term Nullable(String),
            utm_id Nullable(Int32),
            utm_medium Nullable(String),
            referrer Nullable(String),
            referrer_path Nullable(String)
        ) ENGINE = MergeTree order by source_id;
        """.format(
            db_name, tb_name
        )

    def create_table_session(self, tb_name, db_name):
        return """
        CREATE TABLE IF NOT EXISTS {}.{} (
            session_id String,
            userid String,
            app Nullable(String),
            app_version Nullable(Int32),	
            app_identifier Nullable(String),	
            ip Nullable(String),
            browser Nullable(String),
            browser_version Nullable(Int32),
            os Nullable(String),
            os_version	Nullable(Int32),
            device_type Nullable(String),
            device_name Nullable(String),	
            country Nullable(String),	
            region Nullable(String),
            city Nullable(String),
            latitude Nullable(Float32),
            longitude Nullable(Float32),
            isp Nullable(String),
            speed Nullable(Int32)
        ) ENGINE = MergeTree order by userid;
        """.format(
            db_name, tb_name
        )

    def create_view_default_table_joined_sessions_table(self, db_name, tb_name):
        return """
        create view {}.view_{} as 
        SELECT 
            a.object_id as object_id,
            a.object_type as object_type,
            a.object_url as object_url,
            a.content_length as content_length,
            a.title as title,
            a.collection_id as collection_id,
            a.content_list_id as content_list_id,
            a.partner_id as partner_id,
            a.event_type as event_type,
            a.event_value as event_value,
            a.bitrate as bitrate,
            a.width as width,
            a.height as height,
            a.source_id as source_id,
            a.session_id as session_id,
            b.userid as userid,
            b.ip as ip,
            b.user_agent as user_agent,
            b.browser as browser,
            b.browser_version as browser_version,
            b.os as os,
            b.os_version as os_version,
            b.device as device,
            b.device_name as device_name,
            b.country as country,
            b.region as region,
            b.city as city,
            b.latitude  as latitude ,
            b.longitude  as longitude ,
            b.isp  as  isp,
            b.internet_speed  as internet_speed,
            b.app  as app ,
            b.app_version  as app_version ,
            b.app_id  as app_id ,
            c.utm_source  as utm_source ,
            c.utm_term  as utm_term ,
            c.utm_id  as utm_id,
            c.utm_medium  as utm_medium ,
            c.referrer  as referrer,
            c.referrer_path as referrer_path
        FROM
        (
            SELECT *
            FROM {}.{} as a
            INNER JOIN {}.tb_sessions  as b ON a.session_id = b.session_id
            INNER JOIN {}.tb_sources as c on a.source_id = c.source_id
        )
        """.format(
            db_name, tb_name, db_name, tb_name, db_name, db_name
        )

    def create_event_table(self, table_name, db_name):
        return """
        CREATE TABLE IF NOT EXISTS {}.{} (
            stat_id Nullable(String),
            object_id Nullable(Int32),
            object_type Nullable(Int32),
            content_length Nullable(Int32),
            content_list_id Nullable(Int32),
            uploader_id Nullable(String),
            partner_id Nullable(String),
            event_type String,
            event_value Float32,
            bitrate Nullable(Int32),
            cust_1 Nullable(String),
            cust_2 Nullable(String),
            cust_3 Nullable(String),
            cust_4 Nullable(String),
            session_id Nullable(String),
            source_id Nullable(String),
            timestamp DateTime DEFAULT now()
        ) ENGINE = MergeTree order by event_value partition by event_type;
        """.format(
            db_name, table_name
        )

    def create_table_with_columns(self, table_name, db_name, columns, order_by):
        return """
        CREATE TABLE {}.{} (
            {}
        ) ENGINE = MergeTree order by {} ;
        """.format(
            db_name, table_name, columns, order_by
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

    def create_master_database(self):
        return """
        CREATE DATABASE IF NOT EXISTS master_stats
        """

    def create_master_table(self):
        return """
        CREATE TABLE IF NOT EXISTS master_stats.master_table (
            username String,
            db_name String
        ) ENGINE = MergeTree order by db_name;
        """

    def check_existing_user(self, username):
        return """
            select 
                username 
            from master_stats.master_table 
            where username = '{}'    
        """.format(
            username
        )

    def insert_into_ddl_history(self):
        return "INSERT INTO ddl.history (db_name,tb_name,is_default) VALUES "

    def delete_table(self, db_name, table_name):
        return "DROP TABLE  {}.{}".format(db_name, table_name)

    def select_datas(self, columns, db_name, table_name, limit, offset):
        columns = ",".join(columns)
        return "SELECT {} FROM {}.{} LIMIT {},{}".format(
            columns, db_name, table_name, offset, limit
        )

    def select_datas_where(self, columns, db_name, table_name, limit, offset, wheres):
        wheres = "where " + " and ".join(wheres) if len(wheres) > 0 else ""
        columns = ",".join(columns)
        return "SELECT {} FROM {}.{} {} LIMIT {},{}".format(
            columns, db_name, table_name, wheres, offset, limit
        )

    def insert_data_to_table(self, db_name, tb_name, columns):
        return "INSERT INTO {}.{} {} VALUES ".format(db_name, tb_name, columns)

    def add_new_column(self, tb_name, column_name, data_type):
        return "ALTER TABLE {} ADD COLUMN {} {} FIRST".format(
            tb_name, column_name, data_type
        )

    def delete_column(self, tb_name, column_name):
        return "ALTER TABLE {} DROP COLUMN {}".format(tb_name, column_name)

    def use_db(self, db_name):
        return "use {};".format(db_name)
