from sqlalchemy import create_engine, String, Integer

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm import sessionmaker

import sys
sys.path.append('.')
from Parser import client 


class Base(DeclarativeBase):
    pass


class raw_data(Base):
    __tablename__ = "raw_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    num = mapped_column(String(50))
    date = mapped_column(String(50))
    type = mapped_column(String(50))
    state = mapped_column(String(50))
    name = mapped_column(String(50))
    owner_full = mapped_column(String)
    coords = mapped_column(String)
    forw_full = mapped_column(String)
    prew_full = mapped_column(String)
    doc_details = mapped_column(String)
    agency = mapped_column(String)
    status = mapped_column(String(50))
    purpose = mapped_column(String(50))
    image = mapped_column(String(50))
    link = mapped_column(String(50))
    changes = mapped_column(String)
    ordered = mapped_column(String)
    date_stop = mapped_column(String(50))
    date_end = mapped_column(String(50))
    stop_end_conditions = mapped_column(String)
    link_alsn = mapped_column(String(50))


engine = create_engine("sqlite:///database.db")
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)

data = client.get_data_from_reestr(filter='oil')[0]
for row in data:
    obj = raw_data(**row)
    session.add(obj)
session.commit()
session.close()

