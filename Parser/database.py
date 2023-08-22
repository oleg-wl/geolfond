from sqlalchemy import create_engine, text, String, Integer

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import TEXT

from .headers import filter as _filter

import sys
sys.path.append('.')
from Parser import client 

class Base(DeclarativeBase):
    pass
class raw_data(Base):
    __tablename__ = "raw_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    num: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    date: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    type: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True) # Вид полезного ископаемого
    state: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    name: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    owner_full: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    coords: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    forw_full: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    prew_full: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    doc_details: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    agency: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    status: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    purpose: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True) 
    image: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    link: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    changes: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    ordered: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    date_stop: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    date_end: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    stop_end_conditions: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    link_alsn: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)
    
    #: Новый столбец для типа лицензии из фильтра
    filter: Mapped[str] = mapped_column(String(255).with_variant(TEXT(30000, charset="utf8"), "mysql", "mariadb"), default=None, nullable=True)

sql = text('SELECT * FROM raw_data')

def with_database(func):
    def wrapper():
        
        engine = create_engine("mysql+pymysql://geolfond:0451@localhost:3306/geolfond?charset=utf8mb4")
        Session = sessionmaker(bind=engine)
        session = Session()

        result = func(session, engine)
        session.commit()
        session.close()
        return result
    return wrapper

@with_database
def update_database(session, engine) -> None:
    """Полностью обновляет таблицу raw_data в базе данных"""

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    filters = _filter()

    for k in filters.keys():
        data = client.get_data_from_reestr(filter=k)
        print (f'Извлек данные для: {k}, всего строк: {len(data)}')
        
        for row in data:
            obj = raw_data(**row)
            session.add(obj)
        session.commit()
        print('commit saved')

    session.close()

