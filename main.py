from email import message
from services.ddlServices import ddlServices
from services.userServices import userServices
from services.coreServices import coreServices
from fastapi import FastAPI, Body, Response
from models.models import User, ExampleBody
from httpResponses.httpResponses import CustomHttpResponses

app = FastAPI()
ddlSvc = ddlServices.DDLServices()
userSvc = userServices.UserService()
customHttpresp = CustomHttpResponses()
example = ExampleBody()
coreSvc = coreServices.CoreServices()


@app.get("/api/v1/master-datas")
async def create_master_datas(response: Response):
    ddlSvc.create_master_datas()

    msg = "success create database master database and master table"

    return customHttpresp.responses(msg, 200, "GET")


@app.post("/api/v1/user")
async def create_user(user_model: User, response: Response):

    (error, code, msg) = userSvc.create_user(user_model.username)

    response.status_code = code
    return customHttpresp.responses(msg, code, "POST")


@app.post("/api/v1/session")
async def add_session(
    response: Response,
    payload: dict = Body(..., example=example.example_insert_session_table()),
):
    username = payload["username"]
    db_name = "{}_stats".format(username)
    tb_name = "tb_sessions"

    (error, code, msg) = coreSvc.insert_sessions(payload, tb_name, db_name)

    response.status_code = code
    return customHttpresp.responses(msg, code, "POST")


@app.post("/api/v1/event")
async def add_event(
    response: Response,
    payload: dict = Body(..., example=example.example_insert_event_table()),
):
    username = payload["username"]
    db_name = "{}_stats".format(username)
    tb_name = "tb_events"

    (error, code, msg) = coreSvc.insert_event(payload, tb_name, db_name)
    response.status_code = code

    return customHttpresp.responses(msg, code, "POST")


@app.post("/api/v1/source")
async def add_source(
    response: Response,
    payload: dict = Body(..., example=example.example_insert_sources_table()),
):
    username = payload["username"]
    db_name = "{}_stats".format(username)
    tb_name = "tb_sources"

    (error, code, msg) = coreSvc.insert_source(payload, tb_name, db_name)
    response.status_code = code

    return customHttpresp.responses(msg, code, "POST")
