from pandas.core.common import _maybe_box_datetimelike
from utils import authorize_google_sheets
from gspread_pandas import Spread, Client
import pandas as pd
import json
import numpy as np
import os


def get_raw_data_sheet_to_df(spreadsheet, client, cols_to_check):
    spread = Spread(spread=spreadsheet)

    # Get metadata:
    meta = spread.__dict__.get('_spread_metadata')
    project_id = meta.get('spreadsheetId')
    project_name = meta.get('properties').get('title')

    # Locate RawData sheet
    sheets = spread.sheets

    raw = [x for x in sheets if 'rawdata' in x.__dict__['_properties']
           ['title'].replace(' ', '').replace('.', '').lower(
    ) and 'pivot' not in x.__dict__['_properties']
        ['title'].replace(' ', '').replace('.', '').lower()]

    raw.sort(key=lambda x: len(x.__dict__['_properties']
                               ['title'].replace(' ', '').replace('.', '')), reverse=False)
    df = spread.sheet_records_to_df(
        empty2zero=False, header_rows=1, sheet=raw[0],
        default_blank=np.nan)

    # Check for column names:
    df_cols_lower = list(map(lambda x: x.lower(), list(df.columns)))
    cols_to_check = list(map(lambda x: x.lower(), cols_to_check))

    missing_cols = []
    for col in cols_to_check:
        if col not in df_cols_lower:
            missing_cols.append(col)
    if len(missing_cols) > 0:

        print(
            f"Project {project_name} (id: https://docs.google.com/spreadsheets/d/{project_id}) is missing  " + ", ".join(missing_cols))
        for missing_col in missing_cols:
            df[missing_col] = ''
    # df.columns = map(lambda x: x.replace(
    #     ' ', '_').replace('.', '').lower(), df.columns)
    df = df.loc[df['Date'].notnull(), ~df.columns.duplicated(keep='first')]
    # Add additional fields for future lookup and update
    df['_meta_sheetID'] = project_id
    df['_meta_projectName'] = project_name
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    for col in ['Revenue', 'Cost', 'Profit']:
        df[col] = pd.to_numeric(df[col].astype(
            'str').str.replace(',', ''), errors='coerce').fillna(0)
    df = df.infer_objects()
    return df


def update_raw_data(spreadsheet, client):
    df = get_raw_data_sheet_to_df(spreadsheet=spreadsheet, client=client,
                                  cols_to_check=[
                                      'Charge Code', 'Client', 'Function', 'Date',
                                      'Channel', 'Revenue', 'Cost', 'Profit'
                                  ])
    project_name = df['_meta_projectName'][0]
    project_id = df['_meta_sheetID'][0]
    df.to_csv(os.path.join(BASE_DIR, 'raw_data_v2',
                           f'{project_name}_{project_id}.csv'), index=None)
    print(f'Updated {project_name} (ID: {project_id})')


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if __name__ == '__main__':

    client = Client()
    projects = client.list_spreadsheet_files_in_folder(
        '1jQRJDeB369tnTVckeD-dueWOxtyReOy2')
    successes = {}
    failures = {}
    print(projects)
    for project in projects:
        try:
            # print(' '.join(['Getting data for project ', project.get(
            #     'name'), '(id', project.get('id'), ')']))
            df = get_raw_data_sheet_to_df(
                spreadsheet=project.get('id'), client=client, cols_to_check=[
                    'Charge Code', 'Client', 'Function', 'Date',
                    'Channel', 'Revenue', 'Cost', 'Profit'
                ])

            project_name = project.get('name')
            project_id = project.get('id')
            df.to_csv(os.path.join(BASE_DIR, 'raw_data_v2',
                                   f'{project_name}_{project_id}.csv'), index=None)

        except Exception as e:
            print(' '.join(['Project', project.get(
                'name'), '(id', project.get('id')+'):']))
            print(e)
            failures[project.get('name')] = project.get('id')
