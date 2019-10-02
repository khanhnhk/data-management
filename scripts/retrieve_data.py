from utils import *
import pandas as pd
from gspread_pandas import Spread, Client
import re
import json
from pandas_to_postgres import DataFrameCopy
from pmax_data_table import *


def move_dashboards():
    client = Client()
    # Get Dashboard links:
    dashboard_links_sheet = Spread(spread='1geNkTULCutp7PgcqiuMKaNkH7Ynp1nGu3oX1NqoXHBA',
                                   client=client,
                                   sheet='Dashboard').sheet_to_df()
    sheet_ids = dashboard_links_sheet['Link dashboard'].str.extract(
        '/d/([^/]+)', expand=False).unique().tolist()
    for sheet_id in sheet_ids:
        try:
            client.move_file(sheet_id,
                             '/New Dashboards')
        except Exception as e:
            print('Error with sheet id:' + str(sheet_id))
            pass


def get_raw_data_sheet_to_df(spreadsheet, client):
    spread = Spread(spread=spreadsheet)
    sheets = spread.sheets
    raw = [x for x in sheets if x.__dict__['_properties']
           ['title'].replace(' ', '').lower() == 'rawdata']
    return spread.sheet_to_df(sheet=raw[0], index=0)


def filter_dashboard(df, cols_to_filter):
    df_cols = list(df.columns)
    cols = list(set(cols_to_filter).intersection(df.columns))
    lower_cols = [x.replace(' ', '_').lower() for x in cols]
    if len(cols) == 0:
        return pd.DataFrame()
    _df = df.loc[:, cols].copy()
    _df.columns = map(lambda x: x.replace(' ', '_').lower(), _df.columns)
    for col in ['revenue', 'cost', 'profit']:
        _df[col] = pd.to_numeric(_df[col].astype(
            'str').str.replace(',', '').replace('#REF!', ''), errors='coerce').fillna(0)
    return _df


def df_to_postgres(df, table_obj):
    with connect_postgres(conf_file='scripts/conf.json', workspace='dashboard_digital_ocean') as conn:
        DataFrameCopy(df, conn=conn, table_obj=table_obj).copy()


if __name__ == '__main__':
    client = Client()
    df = get_raw_data_sheet_to_df(
        spreadsheet='1sfBw-tB5Ar3dkLxpo5gV7vkFL5mC2x-_Ro41ikqQ21o', client=client)
    _df = filter_dashboard(df=df, cols_to_filter=[
        'Charge Code', 'Client', 'Function', 'Date',
        'Channel', 'Revenue', 'Cost', 'Profit'
    ])

    print(_df.head())
    print(_df.columns)
    # print(_df.index)
    df_to_postgres(_df, table_obj=pmax_data)
