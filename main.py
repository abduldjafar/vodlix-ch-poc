from math import fabs
from typing import Union
from services.ddlServices import ddlServices
from fastapi import FastAPI
from models.models import Database, Table

app = FastAPI()
ddlSvc = ddlServices.DDLServices()


def setup_responses(msg, is_error, response_code, request_type):
    dict = {
        "msg": msg,
        "type": request_type,
        "response_code": response_code,
        "is_error": is_error,
    }

    return dict


def post_success_responses(msg):
    is_error = False
    response_code = 200
    request_type = "POST"

    return setup_responses(msg, is_error, response_code, request_type)

@app.post("/api/v1/databases")
async def create_database(database: Database):
    ddlSvc.create_database(database.database_name)

    msg = "success create database {}".format(database.database_name)
    
    return post_success_responses(msg)



@app.post("/api/v1/tables")
async def create_database(table: Table):
    ddlSvc.create_table(table.database_name, table.table_name)

    msg = "success create table {}.{}".format(table.database_name, table.table_name)

    return post_success_responses(msg)
