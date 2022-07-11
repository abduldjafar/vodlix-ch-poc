from math import fabs
from typing import Union
from services.ddlServices import ddlServices
from fastapi import FastAPI, Path, Request, Body
from models.models import Database, Table, Column
from httpResponses.httpResponses import CustomHttpResponses

app = FastAPI()
ddlSvc = ddlServices.DDLServices()
customHttpresp = CustomHttpResponses()




@app.post("/api/v1/databases")
async def create_database(database: Database):
    ddlSvc.create_database(database.database_name)

    msg = "success create database {}".format(database.database_name)

    return customHttpresp.post_success_responses(msg)


@app.post("/api/v1/tables")
async def create_table_with_default_schema(table: Table):
    ddlSvc.create_default_table(table.database_name, table.table_name)

    msg = "success create table {}.{}".format(table.database_name, table.table_name)

    return customHttpresp.post_success_responses(msg, {
        "view_datas":"{}.view_{}".format(table.database_name, table.table_name)
    })


@app.delete("/api/v1/tables")
async def delete_table(table: Table):
    ddlSvc.delete_table(table.database_name, table.table_name)

    msg = "success  delete {}.{}".format(table.database_name, table.table_name)

    return customHttpresp.delete_success_responses(msg)


@app.post("/api/v1/{database}/{table}")
async def get_datas_from_table_that_already_defined(
    payload: dict = Body(...,example={
    "columns":[
        "session_id"
    ],
    "where":[
        {
            "column":"title",
            "condition":"=",
            "value":"'Teken 3'"
        },
        
        {
                "column":"longitude",
                "condition":"<",
                "value":"15"
        },
        {
                "column":"longitude",
                "condition":">",
                "value":"15"
        },
        {
                "column":"longitude",
                "condition":"<=",
                "value":"15"
        },
        {
                "column":"longitude",
                "condition":">=",
                "value":"15"
        }
        
    ],
    "page":1,
    "limit":100
}),
    database: str = Path(title="database that want to get datas"),
    table: str = Path(title="table that want to get datas"),
):
    data_responses = ddlSvc.selec_data_using_payload(database, table, payload)

    msg = "success get data from {}.{}".format(database, table)

    return customHttpresp.post_success_responses(msg, data_responses)


@app.post("/api/v1/table_custom")
async def create_table_with_spesific_columns(payload: dict = Body(...,example={
    "tb_name":"asepso",
	"db_name":"asek",
	"columns":{
		"name":"String",
		"address":"String",
		"date":"Int32"
	},
	"order_by":"name"
})):
    ddlSvc.create_table_with_columns(payload)
    return customHttpresp.post_success_responses("success create table")


@app.get("/api/v1/table_ddl_history")
async def create_table_ddl_history():
    ddlSvc.create_ddl_history_table()
    return customHttpresp.get_success_responses("success create table")


@app.post("/api/v1/datas")
async def insert_data(payload: dict = Body(...,example={
    'tb_name': 'v1',
    'db_name': 'devel2',
    'datas':{
    "userid":0,
            "session_id":1111 ,
            "ip":"192.168.100.1",
            "user_agent":"Chrome",
            "browser" :"chrome",
            "browser_version" :123,
            "os":"linux",
            "os_version":"amd64",
            "device":"android",
            "device_name" :"alpine",
            "country":"indonesia",
            "region":"asia",
            "city":"Jakarta",
            "latitude":123.45,
            "longitude":111.56 ,
            "isp" :"Telkomsel",
            "internet_speed":3336,
            "app":"app",
            "app_version":1,
            "app_id" :"app_id",
            "object_id":1,
            "object_type":1,
            "object_url":"http://www.testing.com",
            "content_length":"Nullable(String)",
            "title" :"Nullable(String)",
            "collection_id" :123,
            "content_list_id":123,
            "partner_id" :123,
            "event_type":"watch",
            "event_value" :123456,
            "bitrate":123456,
            "width":123456,
            "height" :123456,
            "utm_source" :"utm_source",
            "utm_term" :"utm_term",
            "utm_id":123,
            "utm_medium" :"utm_medium",
            "referrer" :"referrer",
            "referrer_path":"referrer_path"

}
    })):
    data_inserted,message= ddlSvc.insert_data(payload)

    if data_inserted != True:
        return customHttpresp.post_failed_responses(message)
    else:
        return customHttpresp.post_success_responses(message)


@app.put("/api/v1/tables")
async def alter_table(payload: dict = Body(...,example={
	"tb_name":"uji",
	"db_name":"testing",
	"column_name":"asoi",
	"operation_type":"DELETE",
	"data_type":"String"
})):
    ddlSvc.alter_table(payload)
    return customHttpresp.put_success_responses("success do alter operation")
