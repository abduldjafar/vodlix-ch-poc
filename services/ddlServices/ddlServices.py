from queries.ddl import ddl
from servers.clickhouse import ClickhouseServer

class DDLServices(object):
    def __init__(self):
        self.query = ddl.Ddl()
        self.db = ClickhouseServer()
        self.db.init()

    def create_database(self, db_name):
        query = self.query.create_database(db_name)
        self.db.execute_query(query)

    def create_default_table(self, db_name, tb_name):

        create_default_table_query = self.query.create_default_table(tb_name, db_name)
        create_session_table_query = self.query.create_table_session(
            "tb_sessions", db_name
        )
        create_sources_table_query = self.query.create_table_sources(
            "tb_sources", db_name
        )

        create_view_default_table_joined_sessions_table = (
            self.query.create_view_default_table_joined_sessions_table(db_name, tb_name)
        )
        insert_query_ddl = self.query.insert_into_ddl_history()

        self.db.execute_query(create_session_table_query)
        self.db.execute_query(create_default_table_query)
        self.db.execute_query(create_sources_table_query)
        self.db.execute_query(create_view_default_table_joined_sessions_table)
        self.db.insert_data(
            insert_query_ddl,
            [{"db_name": db_name, "tb_name": tb_name, "is_default": True}],
        )

    def create_table_with_columns(self, payload):
        db_name = payload["db_name"]
        tb_name = payload["tb_name"]
        columns = payload["columns"]
        order_by = payload["order_by"]
        insert_query_ddl = self.query.insert_into_ddl_history()
        self.db.insert_data(
            insert_query_ddl,
            [{"db_name": db_name, "tb_name": tb_name, "is_default": False}],
        )

        list_columns = []

        for column in columns.keys():
            list_columns.append("{} {}".format(column, columns[column]))

        fixed_columns = ",".join(list_columns)

        query = self.query.create_table_with_columns(
            tb_name, db_name, fixed_columns, order_by
        )
        self.db.execute_query(query)

    def insert_data(self, payload):
        tb_sources_columns = {
            "source_id": 0,
            "utm_source": None,
            "utm_term": None,
            "utm_id": None,
            "utm_medium": None,
            "referrer": None,
            "referrer_path": None,
        }
        tb_sessions_column = {
            "userid": None,
            "session_id": 0,
            "ip": None,
            "user_agent": None,
            "browser": None,
            "browser_version": None,
            "os": None,
            "os_version": None,
            "device": None,
            "device_name": None,
            "country": None,
            "region": None,
            "city": None,
            "latitude": None,
            "longitude": None,
            "isp": None,
            "internet_speed": None,
            "app": None,
            "app_version": None,
            "app_id": None,
        }

        tb_default_columns = {
            "object_id": None,
            "object_type": None,
            "object_url": None,
            "content_length": None,
            "title": None,
            "collection_id": None,
            "content_list_id": None,
            "partner_id": None,
            "event_type": None,
            "event_value": None,
            "bitrate": None,
            "width": None,
            "height": None,
            "source_id":None,
            "session_id": 0,
        }

        db_name = payload["db_name"]
        tb_name = payload["tb_name"]
        datas = payload["datas"]

        if (
            "session_id" not in datas
            or datas["session_id"] == 0
            or datas["session_id"] is None
        ):
            if "session_id" not in datas:
                return False, "session_id not exists"
            else:
                return False, "session_id not valied : {} value".format(
                    datas["session_id"]
                )

        def insert_datas(column_dict, tb_name):
            list_datas_columns = list(
                filter(
                    lambda x: x in column_dict.keys(),
                    [column for column in datas.keys()],
                )
            )
            fixed_columns = "(" + ",".join(list_datas_columns) + ")"
            query_insert = self.query.insert_data_to_table(
                db_name, tb_name, fixed_columns
            )

            self.db.insert_data(
                query_insert, [{x: datas[x] for x in list_datas_columns}]
            )

        def check_is_tbsessions_or_tbsources(checking_query):
            self.db.execute_query(checking_query)
            datas = self.db.cursor.fetchall()
            is_exists = True if len(datas) > 0 else False

            return is_exists

        select_query = self.query.select_datas_where(
            ["is_default"],
            "ddl",
            "history",
            "1",
            "0",
            ["db_name='{}'".format(db_name), "tb_name='{}'".format(tb_name)],
        )
        self.db.execute_query(select_query)
        is_defult_table = self.db.cursor.fetchall()[0][0]

        if is_defult_table:
            query_for_check_sessionid = self.query.select_datas_where(
                ["session_id"],
                db_name,
                "tb_sessions",
                "1",
                "0",
                ["session_id='{}'".format(datas["session_id"])],
            )

            query_for_check_sourcesid = self.query.select_datas_where(
                ["source_id"],
                db_name,
                "tb_sources",
                "1",
                "0",
                ["source_id='{}'".format(datas["source_id"])],
            )

            if check_is_tbsessions_or_tbsources(query_for_check_sessionid) != True:
                insert_datas(tb_sessions_column, "tb_sessions")

            if (
                "source_id" in datas or datas["source_id"] != None
            ) and check_is_tbsessions_or_tbsources(query_for_check_sourcesid) != True:
                insert_datas(tb_sources_columns, "tb_sources")

            insert_datas(tb_default_columns, tb_name)
        return True, "sucess inserted datas"

    def selec_data_from_table(self, db_name, tb_name, column, limit="", offset=""):

        columns = column.columns
        if limit == "" or offset == "":
            limit = "10000"
            offset = "0"

        query = self.query.select_datas(columns, db_name, tb_name, limit, offset)
        self.db.execute_query(query)
        datas = self.db.cursor.fetchall()

        data_responses = []

        for data in datas:
            datas_dict = {}
            for index in range(len(columns)):
                datas_dict[columns[index]] = data[index]
            data_responses.append(datas_dict)

        return data_responses

    def selec_data_using_payload(self, db_name, tb_name, payload):

        columns = payload["columns"]
        wheres = payload["where"]
        limit = payload["limit"]
        page = payload["page"]

        filtered_conditions = (
            [
                "{}{}{}".format(where["column"], where["condition"], where["value"])
                for where in wheres
            ]
            if len(wheres) > 0
            else wheres
        )

        page = page - 1

        query = self.query.select_datas_where(
            columns, db_name, tb_name, str(limit), str(page), filtered_conditions
        )
        self.db.execute_query(query)
        datas = self.db.cursor.fetchall()

        data_responses = []

        for data in datas:
            datas_dict = {}
            for index in range(len(columns)):
                datas_dict[columns[index]] = data[index]
            data_responses.append(datas_dict)

        return data_responses

    def create_ddl_history_table(self):
        self.db.execute_query(self.query.create_ddl_database())
        self.db.execute_query(self.query.create_ddl_history_table())

    def delete_table(self, db_name, tb_name):
        query = self.query.delete_table(db_name, tb_name)
        self.db.execute_query(query)

    def alter_table(self, payload):
        tb_name = payload["tb_name"]
        db_name = payload["db_name"]
        column_name = payload["column_name"]
        operation_type = payload["operation_type"]

        self.db.execute_query(self.query.use_db(db_name))

        if operation_type == "ADD":

            data_type = payload["data_type"]
            query = self.query.add_new_column(tb_name, column_name, data_type)

        elif operation_type == "DELETE":
            query = self.query.delete_column(tb_name, column_name)

        self.db.execute_query(query)
