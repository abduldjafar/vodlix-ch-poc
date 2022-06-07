from pydantic import BaseModel

class Database(BaseModel):
    database_name: str

class Table(BaseModel):
    database_name: str
    table_name: str