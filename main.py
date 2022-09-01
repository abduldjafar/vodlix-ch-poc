from services.ddlServices import ddlServices
from services.userServices import userServices
from fastapi import FastAPI, Path, Body
from models.models import User
from httpResponses.httpResponses import CustomHttpResponses

app = FastAPI()
ddlSvc = ddlServices.DDLServices()
userSvc = userServices.UserService() 
customHttpresp = CustomHttpResponses()

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