from pydantic import BaseModel
from datetime import datetime
from unpack import unpack

class data(BaseModel):
    num: dict
    date: datetime
    type: str
    name: str
    state: str
    Year: int
    Last: bool
    INN: int
    owner: str
    prev_lic: str
    prev_date: str
    forw_lic: str
    forw_date: str
    N: str
    E: str
    rad_N: int
    rad_E: int
    month: int
    prev_owner: int

class result(BaseModel):
    count: int 
    data: data

d = unpack()
x = data.model_construct(**d)
