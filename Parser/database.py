from sqlalchemy import create_engine, String, Integer

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm import sessionmaker

from headers import filter as _filter

import sys
sys.path.append('.')
from Parser import client 


class Base(DeclarativeBase):
    pass


class raw_data(Base):
    __tablename__ = "raw_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    num: Mapped[str] = mapped_column(String(50))
    date: Mapped[str] = mapped_column(String(50))
    type: Mapped[str] = mapped_column(String(50)) # Вид полезного ископаемого
    state: Mapped[str] = mapped_column(String(50))
    name: Mapped[str] = mapped_column(String(50))
    owner_full: Mapped[str] = mapped_column(String)
    coords: Mapped[str] = mapped_column(String)
    forw_full: Mapped[str] = mapped_column(String)
    prew_full: Mapped[str] = mapped_column(String)
    doc_details: Mapped[str] = mapped_column(String)
    agency: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String(50))
    purpose: Mapped[str] = mapped_column(String(50)) 
    image: Mapped[str] = mapped_column(String(50))
    link: Mapped[str] = mapped_column(String(50))
    changes: Mapped[str] = mapped_column(String)
    ordered: Mapped[str] = mapped_column(String)
    date_stop: Mapped[str] = mapped_column(String(50))
    date_end: Mapped[str] = mapped_column(String(50))
    stop_end_conditions: Mapped[str] = mapped_column(String)
    link_alsn: Mapped[str] = mapped_column(String(50))
    
    #: Новый столбец для типа лицензии из фильтра
    filter: Mapped[str] = mapped_column(String(60))


engine = create_engine("sqlite:///database.db")
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(bind=engine)

filters = _filter()

for k in filters.keys():
    data = client.get_data_from_reestr(filter=k)[0]
    print (f'Извлек данные для: {k}')
    
    for row in data:
        obj = raw_data(**row)
        session.add(obj)
        session.commit()
    print('saved')

session.close()

