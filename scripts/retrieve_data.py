from utils import *
import pandas as pd
from gspread_pandas import Spread, Client
import re
import json
from pandas_to_postgres import DataFrameCopy
from pmax_data_table import *
import logging

conf_file = 'scripts/conf.json'

cols_to_filter = [
    'Charge Code', 'Client', 'Function', 'Date',
    'Channel', 'Revenue', 'Cost', 'Profit'
]


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


def get_raw_data_sheet_to_df(spreadsheet, client, cols_to_filter):
    spread = Spread(spread=spreadsheet)

    meta = spread.__dict__.get('_spread_metadata')
    project_id = meta.get('spreadsheetId')
    project_name = meta.get('properties').get('title')

    sheets = spread.sheets
    raw = [x for x in sheets if 'rawdata' in x.__dict__['_properties']
           ['title'].replace(' ', '').replace('.', '').lower(
    ) and 'pivot' not in x.__dict__['_properties']
        ['title'].replace(' ', '').replace('.', '').lower()]

    raw.sort(key=lambda x: len(x.__dict__['_properties']
                               ['title'].replace(' ', '').replace('.', '')), reverse=False)
    df = spread.sheet_to_df(sheet=raw[0], index=0)

    # Check for column names:
    df_cols = list(map(lambda x: x.lower(), list(df.columns)))
    cols_to_filter = list(map(lambda x: x.lower(), cols_to_filter))
    missing_cols = []
    for col in cols_to_filter:
        if col not in df_cols:
            missing_cols.append(col)
    if len(missing_cols) > 0:

        print(
            f"Project {project_name} is missing (id: {project_id}) " + ", ".join(missing_cols))
        for missing_col in missing_cols:
            df[missing_col] = ''

    cols = list(set(cols_to_filter).intersection(df.columns))
    lower_cols = [x.replace(' ', '_').lower() for x in cols]
    if len(cols) == 0:
        return pd.DataFrame(columns=cols_to_filter)
    _df = df.loc[:, cols].copy()
    _df = _df.loc[:, ~_df.T.duplicated(keep='first')]
    _df.columns = map(lambda x: x.replace(' ', '_').lower(), _df.columns)
    _df['date'] = pd.to_datetime(_df['date'], errors='coerce')
    for col in ['revenue', 'cost', 'profit']:
        _df[col] = pd.to_numeric(_df[col].astype(
            'str').str.replace(',', ''), errors='coerce').fillna(0)
    return _df


# def filter_dashboard(df, cols_to_filter):
#     df_cols = list(df.columns)
#     cols = list(set(cols_to_filter).intersection(df.columns))
#     lower_cols = [x.replace(' ', '_').lower() for x in cols]
#     if len(cols) == 0:
#         return pd.DataFrame()
#     _df = df.loc[:, cols].copy()
#     _df.columns = map(lambda x: x.replace(' ', '_').lower(), _df.columns)
#     for col in ['revenue', 'cost', 'profit']:
#         _df[col] = pd.to_numeric(_df[col].astype(
#             'str').str.replace(',', ''), errors='coerce').fillna(0)
#     return _df


def df_to_postgres(df, table_obj):
    with connect_postgres(conf_file='scripts/conf.json', workspace='dashboard_digital_ocean') as conn:
        DataFrameCopy(df, conn=conn, table_obj=table_obj).copy()


if __name__ == '__main__':
    client = Client()
    projects = client.list_spreadsheet_files_in_folder(
        '1jQRJDeB369tnTVckeD-dueWOxtyReOy2')
    successes = {}
    failures = {}
    s = get_raw_data_sheet_to_df(
        spreadsheet='1OeKURzKKtZ8qKuhvUkKVK2VmP6dvitGPUiveTtgjVkY', client=client, cols_to_filter=[
                    'Charge Code', 'Client', 'Function', 'Date',
                    'Channel', 'Revenue', 'Cost', 'Profit'
        ])

    for project in projects:
        try:
            # print(' '.join(['Getting data for project ', project.get(
            #     'name'), '(id', project.get('id'), ')']))
            df = get_raw_data_sheet_to_df(
                spreadsheet=project.get('id'), client=client, cols_to_filter=[
                    'Charge Code', 'Client', 'Function', 'Date',
                    'Channel', 'Revenue', 'Cost', 'Profit'
                ])

            # print('Inserting data to pmax_data... ')
            df.to_sql('pmax_data',
                      con=setup_engine(
                          conf_file=conf_file,
                          workspace='dashboard_digital_ocean'),
                      method=psql_insert_copy, if_exists='append',
                      index=False)
            successes[project.get('name')] = project.get('id')
            print(' '.join(['Successfully update data', project.get(
                'name'), '(id', project.get('id'), ')']))
        except Exception as e:
            print(' '.join(['Project', project.get(
                'name'), '(id', project.get('id')+'):']))
            print(e)
            failures[project.get('name')] = project.get('id')

    # print('Failures:')
    # print(failures)
    # # for project_name, project_id in failures.items():
    # #     try:
    # #         # print(' '.join(['Getting data for project ', project.get(
    # #         #     'name'), '(id', project.get('id'), ')']))
    # #         df = get_raw_data_sheet_to_df(
    # #             spreadsheet=project_id, client=client, cols_to_filter=[
    # #                 'Charge Code', 'Client', 'Function', 'Date',
    # #                 'Channel', 'Revenue', 'Cost', 'Profit'
    # #             ])

    # #         # print('Inserting data to pmax_data... ')
    # #         df.to_sql('pmax_data',
    # #                   con=setup_engine(
    # #                       conf_file=conf_file,
    # #                       workspace='dashboard_digital_ocean'),
    # #                   method=psql_insert_copy, if_exists='append',
    # #                   index=False)
    # #         successes[project_name] = project_id
    # #         print(' '.join(['Successfully update data',
    # #                         project_name, '(id', project_id, ')']))
    # #     except Exception as e:
    # #         print(' '.join(['Project', project_name, '(id', project_id+'):']))
    # #         print(e)
    # #         failures[project_name] = project_id

    # print('Failures:')
    # print(failures)
