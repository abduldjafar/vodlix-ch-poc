from pydantic import BaseModel
from typing import List


class Database(BaseModel):
    database_name: str

class Table(BaseModel):
    database_name: str
    table_name: str

class Column(BaseModel):
    columns: List[str]