from sqlalchemy import create_engine, Table, Column, Date, Numeric, String, Integer, MetaData
import json


conf_file = 'scripts/conf.json'


with open(conf_file, 'r') as f:
    conf = json.load(f)['DATABASE']['dashboard_digital_ocean']

user = conf['user']
password = conf['password']
host = conf['host']
port = conf['port']
database = conf.get('database', '')

meta = MetaData()
engine = create_engine(
    rf'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

connection = engine.raw_connection()
cursor = connection.cursor()

command = '''
DROP TABLE IF EXISTS pmax_data;

CREATE TABLE pmax_data (
    "id" SERIAL PRIMARY KEY,
    "charge_code" VARCHAR(15),
    "client" VARCHAR(50),
    "function" VARCHAR(15),
    "date" DATE,
    "channel" VARCHAR(50),
    "revenue" NUMERIC,
    "cost" NUMERIC,
    "profit" NUMERIC

);
'''
cursor.execute(command)
connection.commit()

# pmax_data = Table(
#     'pmax_data', meta,
#     Column('id', Integer, primary_key=True),
#     Column('charge_code', String(10)),
#     Column('client', String),
#     Column('function', String),
#     Column('date', Date),
#     Column('channel', String),
#     Column('revenue', Numeric),
#     Column('cost', Numeric),
#     Column('profit', Numeric),

# )

# meta.create_all(engine)
