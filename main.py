from services.ddlServices import ddlServices
from services.userServices import userServices
from services.coreServices import coreServices
from fastapi import FastAPI, Path, Body
from models.models import User,ExampleBody
from httpResponses.httpResponses import CustomHttpResponses

app = FastAPI()
ddlSvc = ddlServices.DDLServices()
userSvc = userServices.UserService() 
customHttpresp = CustomHttpResponses()
example = ExampleBody()
coreSvc = coreServices.CoreServices()


@app.get("/api/v1/master-datas")
async def create_master_datas():
    ddlSvc.create_master_datas()

    msg = "success create database master database and master table"

    return customHttpresp.post_success_responses(msg)

@app.post("/api/v1/user")
async def create_user(user_model: User):
    userSvc.create_user(user_model.username)

    msg = "success create {},{}_stats db and all related tables".format(user_model.username, user_model.username)

    return customHttpresp.post_success_responses(msg)

@app.post("/api/v1/session")
async def add_session(payload: dict = Body(..., example=example.example_insert_session_table())):
    username = payload['username']
    db_name = "{}_stats".format(username)
    tb_name = "tb_sessions"
    coreSvc.insert_datas(payload,tb_name,db_name)

@app.post("/api/v1/event")
async def add_event(payload: dict = Body(..., example=example.example_insert_event_table())):
    username = payload['username']
    db_name = "{}_stats".format(username)
    tb_name = "tb_events"
    coreSvc.insert_datas(payload,tb_name,db_name)

@app.post("/api/v1/source")
async def add_source(payload: dict = Body(..., example=example.example_insert_sources_table())):
    username = payload['username']
    db_name = "{}_stats".format(username)
    tb_name = "tb_sources"
    coreSvc.insert_datas(payload,tb_name,db_name)