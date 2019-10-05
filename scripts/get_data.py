from pandas.core.common import _maybe_box_datetimelike
from utils import authorize_google_sheets
from gspread_pandas import Spread, Client
from pymongo import MongoClient
import pandas as pd
import json
import numpy as np

CONN_STRING = '<INPUT CONN STRING HERE>'


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
    df_cols = list(map(lambda x: x.lower(), list(df.columns)))
    cols_to_check = list(map(lambda x: x.lower(), cols_to_check))

    missing_cols = []
    for col in cols_to_check:
        if col not in df_cols:
            missing_cols.append(col)
    if len(missing_cols) > 0:

        print(
            f"Project {project_name} is missing (id: {project_id}) " + ", ".join(missing_cols))
        for missing_col in missing_cols:
            df[missing_col] = ''

    df = df.loc[:, ~df.T.duplicated(keep='first')]
    df.columns = map(lambda x: x.replace(
        ' ', '_').replace('.', '').lower(), df.columns)

    # Add additional fields for future lookup and update
    df['_meta_sheetID'] = project_id
    df['_meta_projectName'] = project_name
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    for col in ['revenue', 'cost', 'profit']:
        df[col] = pd.to_numeric(df[col].astype(
            'str').str.replace(',', ''), errors='coerce').fillna(0)
    df = df.infer_objects()
    return df


def df_to_mongo(df, collection):
    try:
        # Ensure columns are unique:
        df = df.loc[:, ~df.T.duplicated(keep='first')]

        # Omit null/ nan values and convert to rows:
        df_list = [dict((k, _maybe_box_datetimelike(v)) for k, v in zip(
            df.columns, row) if v != None and v == v and '#' not in str(v)) for row in df.values]

        # Import to collection
        collection.insert_many(df_list)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    client = Client()
    projects = client.list_spreadsheet_files_in_folder(
        '1jQRJDeB369tnTVckeD-dueWOxtyReOy2')
    successes = {}
    failures = {}

    # Establish connection to MongoDB on DigitalOcean & Database pmax to prepare for input
    client = MongoClient(CONN_STRING)
    pmax_db = client["pmax"]

    # Connecting to raw_data collection:
    raw_data_collection = pmax_db["raw_data"]

    for project in projects:
        try:
            # print(' '.join(['Getting data for project ', project.get(
            #     'name'), '(id', project.get('id'), ')']))
            df = get_raw_data_sheet_to_df(
                spreadsheet=project.get('id'), client=client, cols_to_check=[
                    'Charge Code', 'Client', 'Function', 'Date',
                    'Channel', 'Revenue', 'Cost', 'Profit'
                ])
            df_to_mongo(df, collection=raw_data_collection)
            successes[project.get('name')] = project.get('id')
            print(' '.join(['Successfully update data', project.get(
                'name'), '(id', project.get('id'), ')']))

        except Exception as e:
            print(' '.join(['Project', project.get(
                'name'), '(id', project.get('id')+'):']))
            print(e)
            failures[project.get('name')] = project.get('id')

        finally:
            client.close()
