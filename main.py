import json
from datetime import datetime as dt
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy import select

Base = declarative_base()


class Publisher(Base):
    __tablename__ = 'publisher'

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(length=100), unique=True, nullable=False)


class Book(Base):
    __tablename__ = 'book'

    id = sa.Column(sa.Integer, primary_key=True)
    title = sa.Column(sa.String(length=100), unique=True, nullable=False)
    id_publisher = sa.Column(sa.Integer, sa.ForeignKey('publisher.id'), nullable=False)

    publisher = relationship(Publisher, backref='book')


class Shop(Base):
    __tablename__ = 'shop'

    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String(length=100), unique=True, nullable=False)


class Stock(Base):
    __tablename__ = 'stock'

    id = sa.Column(sa.Integer, primary_key=True)
    id_book = sa.Column(sa.Integer, sa.ForeignKey('book.id'), nullable=False)
    id_shop = sa.Column(sa.Integer, sa.ForeignKey('shop.id'), nullable=False)
    count = sa.Column(sa.Integer, nullable=False)

    book = relationship(Book, backref='stock')
    shop = relationship(Shop, backref='stock')


class Sale(Base):
    __tablename__ = 'sale'

    id = sa.Column(sa.Integer, primary_key=True)
    price = sa.Column(sa.String, nullable=False)
    date_sale = sa.Column(sa.DateTime, nullable=False)
    id_stock = sa.Column(sa.Integer, sa.ForeignKey('stock.id'), nullable=False)
    count = sa.Column(sa.Integer, nullable=False)

    stock = relationship(Stock, backref='sale')


def create_tables(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


db_type = input('Введите тип базы данных, например: postgresql\n')
db_login = input('Введите пользователя базы данных\n')
db_pass = input('Введите пароль от пользователя базы данных\n')
db_host = input('Введите хост для подключения к базе данных\n')
db_port = input('Введите порт для подключения к базе данных\n')
db_name = input('Введите название базе данных\n')

if not db_type:
    db_type = 'postgresql'
if not db_login:
    db_login = 'postgres'
if not db_pass:
    db_pass = 'postgres'
if not db_host:
    db_host = 'localhost'
if not db_port:
    db_port = 5432
if not db_name:
    db_name = 'postgres'

DSN = f'{db_type}://{db_login}:{db_pass}@{db_host}:{db_port}/{db_name}'
engine1 = sa.create_engine(DSN)
create_tables(engine1)

Session = sessionmaker(bind=engine1)
session = Session()

with open('tests_data.json') as f:
    data = json.load(f)
    for d in data:
        if d['model'] == 'publisher':
            pub = Publisher(id=d['pk'], name=d['fields']['name'])
            session.add(pub)
            session.commit()

    for d in data:
        if d['model'] == 'book':
            book = Book(id=d['pk'], title=d['fields']['title'], id_publisher=d['fields']['id_publisher'])
            session.add(book)
            session.commit()

    for d in data:
        if d['model'] == 'shop':
            shop = Shop(id=d['pk'], name=d['fields']['name'])
            session.add(shop)
            session.commit()

    for d in data:
        if d['model'] == 'stock':
            stock = Stock(
                id=d['pk'], id_book=d['fields']['id_book'],
                id_shop=d['fields']['id_shop'], count=d['fields']['count']
            )
            session.add(stock)
            session.commit()

    for d in data:
        if d['model'] == 'sale':
            date_time = dt.strptime(d['fields']['date_sale'], '%Y-%m-%dT%H:%M:%S.%fZ')
            sale = Sale(
                id=d['pk'], price=d['fields']['price'], id_stock=d['fields']['id_stock'], count=d['fields']['count'],
                date_sale=date_time
            )
            session.add(sale)
            session.commit()

pub_clause = None
pub_id = None
pub_name = None

while not pub_clause or (pub_clause and pub_clause not in ['1', '2']):
    pub_clause = input(
        'По чему будем искать продажи книг? Введите корректную цифру!'
        '\nЕсли по идентификатору издателя, то введите цифру 1'
        '\nЕсли по названию издателя, то введите цифру 2\n'
    )

if pub_clause == '1':
    while not pub_id:
        pub_id = int(input('Введите идентификатор издателя\n'))
    stmt = (
        select(Publisher, Book, Shop, Sale).join(Book, Publisher.id == Book.id_publisher).
        join(Stock, Book.id == Stock.id_book).join(Shop, Stock.id_shop == Shop.id).
        join(Sale, Stock.id == Sale.id_stock).where(Publisher.id == pub_id)
    )
elif pub_clause == '2':
    while not pub_name:
        pub_name = input('Введите название издателя\n')
    stmt = (
        select(Publisher, Book, Shop, Sale).join(Book, Publisher.id == Book.id_publisher).
        join(Stock, Book.id == Stock.id_book).join(Shop, Stock.id_shop == Shop.id).
        join(Sale, Stock.id == Sale.id_stock).where(Publisher.name == pub_name)
    )

result = session.execute(stmt)
for row in result:
    print(
        f'{row.Book.title} | {row.Shop.name} | {row.Sale.price} | '
        f'{row.Sale.date_sale.replace(microsecond=0)}'
    )
