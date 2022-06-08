from math import fabs
from typing import Union
from services.ddlServices import ddlServices
from fastapi import FastAPI, Path, Request, Body
from models.models import Database, Table, Column

app = FastAPI()
ddlSvc = ddlServices.DDLServices()


def setup_responses(msg, is_error, response_code, request_type, data):
    dict = {
        "msg": msg,
        "type": request_type,
        "response_code": response_code,
        "is_error": is_error,
        "data": data,
    }

    return dict


def post_success_responses(msg, data=None):
    is_error = False
    response_code = 200
    request_type = "POST"

    return setup_responses(msg, is_error, response_code, request_type, data)

def delete_success_responses(msg, data=None):
    is_error = False
    response_code = 200
    request_type = "DELETE"

    return setup_responses(msg, is_error, response_code, request_type, data)


def get_success_responses(msg, data=None):
    is_error = False
    response_code = 200
    request_type = "GET"

    return setup_responses(msg, is_error, response_code, request_type, data)

def put_success_responses(msg, data=None):
    is_error = False
    response_code = 200
    request_type = "PUT"

    return setup_responses(msg, is_error, response_code, request_type, data)


@app.post("/api/v1/databases")
async def create_database(database: Database):
    ddlSvc.create_database(database.database_name)

    msg = "success create database {}".format(database.database_name)

    return post_success_responses(msg)


@app.post("/api/v1/tables")
async def create_table_with_default_schema(table: Table):
    ddlSvc.create_table(table.database_name, table.table_name)

    msg = "success create table {}.{}".format(table.database_name, table.table_name)

    return post_success_responses(msg)

@app.delete("/api/v1/tables")
async def delete_table(table: Table):
    ddlSvc.delete_table(table.database_name, table.table_name)

    msg = "success  delete {}.{}".format(table.database_name, table.table_name)

    return delete_success_responses(msg)


@app.post("/api/v1/{database}/{table}")
async def get_datas_from_table_that_already_defined(
    column: Column,
    database: str = Path(title="database that want to get datas"),
    table: str = Path(title="table that want to get datas"),
):
    data_responses = []
    datas = ddlSvc.selec_data_from_table(database, table, column.columns)

    columns = column.columns

    for data in datas:
        datas_dict = {}
        for index in range(len(columns)):
            datas_dict[columns[index]] = data[index]
        data_responses.append(datas_dict)

    msg = "success get data from {}.{}".format(database, table)

    return post_success_responses(msg, data_responses)


@app.post("/api/v1/table_custom")
async def create_table_with_spesific_columns(payload: dict = Body(...)):
    ddlSvc.create_table_with_columns(payload)
    return post_success_responses("success create table")


@app.get("/api/v1/table_ddl_history")
async def create_table_ddl_history():
    ddlSvc.create_ddl_history_table()

    return get_success_responses("success create table")

@app.post("/api/v1/datas")
async def insert_data(payload: dict = Body(...)):
    ddlSvc.insert_data(payload)
    return post_success_responses("success insert datas")

@app.put("/api/v1/tables")
async def alter_table(payload: dict = Body(...)):
    ddlSvc.alter_table(payload)
    return put_success_responses("success do alter operation")
