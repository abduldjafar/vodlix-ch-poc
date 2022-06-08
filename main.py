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
    ddlSvc.create_table(table.database_name, table.table_name)

    msg = "success create table {}.{}".format(table.database_name, table.table_name)

    return customHttpresp.post_success_responses(msg)


@app.delete("/api/v1/tables")
async def delete_table(table: Table):
    ddlSvc.delete_table(table.database_name, table.table_name)

    msg = "success  delete {}.{}".format(table.database_name, table.table_name)

    return customHttpresp.delete_success_responses(msg)


@app.post("/api/v1/{database}/{table}")
async def get_datas_from_table_that_already_defined(
    column: Column,
    database: str = Path(title="database that want to get datas"),
    table: str = Path(title="table that want to get datas"),
):
    data_responses = ddlSvc.selec_data_from_table(database, table, column)

    msg = "success get data from {}.{}".format(database, table)

    return customHttpresp.post_success_responses(msg, data_responses)


@app.post("/api/v1/table_custom")
async def create_table_with_spesific_columns(payload: dict = Body(...)):
    ddlSvc.create_table_with_columns(payload)
    return customHttpresp.post_success_responses("success create table")


@app.get("/api/v1/table_ddl_history")
async def create_table_ddl_history():
    ddlSvc.create_ddl_history_table()
    return customHttpresp.get_success_responses("success create table")


@app.post("/api/v1/datas")
async def insert_data(payload: dict = Body(...)):
    ddlSvc.insert_data(payload)
    return customHttpresp.post_success_responses("success insert datas")


@app.put("/api/v1/tables")
async def alter_table(payload: dict = Body(...)):
    ddlSvc.alter_table(payload)
    return customHttpresp.put_success_responses("success do alter operation")
