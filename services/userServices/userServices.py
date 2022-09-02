from ast import Delete
from queries.ddl import ddl
from servers.clickhouse import ClickhouseServer

class UserService(object):
    def __init__(self):
        self.query = ddl.Ddl()
        self.db = ClickhouseServer()
        self.db.init()
    
    def is_users_exists(self,username):

        query = self.query.check_existing_user(username)
        self.db.execute_query(query)
        datas = self.db.cursor.fetchall()
        is_exists = True if len(datas) > 0 else False

        return is_exists
    
    def create_user(self,username):
        db_name = "{}_stats".format(username)
        tb_events = "tb_events"
        tb_sessions = "tb_sessions"
        tb_sources = "tb_sources"

        is_users_exists = self.is_users_exists(username)
        add_user_query = self.query.insert_data_to_table("master_stats","master_table","(username,db_name)")
        create_user_stat_db_query = self.query.create_database(db_name)

        if is_users_exists:
            return "user already exists"
        else:
            self.db.execute_query(create_user_stat_db_query)
            create_event_table_query = self.query.create_event_table(tb_events, db_name)
            create_session_table_query = self.query.create_table_session(
                tb_sessions, db_name
            )
            create_sources_table_query = self.query.create_table_sources(
                tb_sources, db_name
            )

            create_view_default_table_joined_sessions_table = (
                self.query.create_view_default_table_joined_sessions_table(db_name, tb_events)
            )

            self.db.execute_query(create_session_table_query)
            self.db.execute_query(create_event_table_query)
            self.db.execute_query(create_sources_table_query)
            #self.db.execute_query(create_view_default_table_joined_sessions_table)

            
            self.db.insert_data(add_user_query,[{"username":username,"db_name":db_name}])

            return "user created"

    def list_users(self):
        pass

    def delete_user(self,username):
        pass