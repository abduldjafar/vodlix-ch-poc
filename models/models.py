import re
from pydantic import BaseModel
from typing import List


class Database(BaseModel):
    database_name: str


class Table(BaseModel):
    database_name: str
    table_name: str


class Column(BaseModel):
    columns: List[str]

class User(BaseModel):
    username: str

class ExampleBody(object):
    def __init__(self):
        pass

    def example_alter_table(self):
        return {
            "tb_name": "uji",
            "db_name": "testing",
            "column_name": "asoi",
            "operation_type": "DELETE",
            "data_type": "String",
        }
    def example_insert_sources_table(self):
        return {
            "username":"abdul",
            "data":{
                "source_id" :32,
                "utm_source":"",
                "utm_term" :"",
                "utm_id":32,
                "utm_medium":"",
                "referrer":"",
                "referrer_path":""
            }

        }
    def example_insert_session_table(self):
        return {
            "username":"abdul",
            "data":{
                "userid":"xxx",	
                "session_id":"",
                "app":"",	
                "app_version":1,	
                "app_identifier":"",	
                "ip":""	,
                "browser":"",	
                "browser_version"	: 123,
                "os":"",	
                "os_version":1,	
                "device_type":"",
                "device_name":"",	
                "country":"",	
                "region":"",	
                "city":"",	
                "latitude":111.111,
                "longitude":123.44,
                "isp":"",
            "speed":111
            }
            
        }
    def example_insert_event_table(self):
        return {
            "username":"abdul",
            "data":{
                "stat_id":"",
                "object_id":1,
                "object_type":1,
                "content_length":0,
                "content_list_id":1,
                "uploader_id":"",
                "partner_id":"",	
                "event_type":"",
                "event_value":111,
                "bitrate":123456,
                "cust_1":"",
                "cust_2":"",
                "cust_3":"",
                "cust_4":"",
                "session_id":"",
                "source_id":"",
                "timestamp":""
            }
             
        }
    def example_insert_data(self):
        return {
            "tb_name": "v1",
            "db_name": "devel2",
            "datas": {
                "userid": 0,
                "session_id": 1111,
                "ip": "192.168.100.1",
                "user_agent": "Chrome",
                "browser": "chrome",
                "browser_version": 123,
                "os": "linux",
                "os_version": "amd64",
                "device": "android",
                "device_name": "alpine",
                "country": "indonesia",
                "region": "asia",
                "city": "Jakarta",
                "latitude": 123.45,
                "longitude": 111.56,
                "isp": "Telkomsel",
                "internet_speed": 3336,
                "app": "app",
                "app_version": 1,
                "app_id": "app_id",
                "object_id": 1,
                "object_type": 1,
                "object_url": "http://www.testing.com",
                "content_length": "Nullable(String)",
                "title": "Nullable(String)",
                "collection_id": 123,
                "content_list_id": 123,
                "partner_id": 123,
                "event_type": "watch",
                "event_value": 123456,
                "bitrate": 123456,
                "width": 123456,
                "height": 123456,
                "utm_source": "utm_source",
                "utm_term": "utm_term",
                "utm_id": 123,
                "utm_medium": "utm_medium",
                "referrer": "referrer",
                "referrer_path": "referrer_path",
            },
        }

    def example_create_table_with_spesific_columns(self):
        return {
            "tb_name": "asepso",
            "db_name": "asek",
            "columns": {"name": "String", "address": "String", "date": "Int32"},
            "order_by": "name",
        }

    def example_get_datas_from_table_that_already_defined(self):
        return {
            "columns": ["session_id"],
            "where": [
                {"column": "title", "condition": "=", "value": "'Teken 3'"},
                {"column": "longitude", "condition": "<", "value": "15"},
                {"column": "longitude", "condition": ">", "value": "15"},
                {"column": "longitude", "condition": "<=", "value": "15"},
                {"column": "longitude", "condition": ">=", "value": "15"},
            ],
            "page": 1,
            "limit": 100,
        }
