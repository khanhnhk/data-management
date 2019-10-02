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


pmax_data = Table(
    'pmax_data', meta,
    Column('id', Integer, primary_key=True),
    Column('charge_code', String),
    Column('client', String),
    Column('function', String),
    Column('date', Date),
    Column('channel', String),
    Column('revenue', Numeric),
    Column('cost', Numeric),
    Column('profit', Numeric),

)

meta.create_all(engine)
