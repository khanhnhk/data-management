from utils import authorize_google_sheets
from gspread_pandas import Spread, Client

import pandas as pd
import json
import numpy as np
import os
from copy_sheet import *
from sqlalchemy import create_engine
import io
from io import StringIO
import psycopg2
import csv
from sqlalchemy.orm.session import sessionmaker


# AUTHORIZATION:
creds = None
# The file token.pickle stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.

if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            os.path.join(BASE_DIR, 'credentials.json'), SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)


def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def create_spread_in_folder(spread_name, client, path='/'):
    # Create a new sheet if not exists:
    try:
        spread = Spread(spread=spread_name)
    except Exception:
        spread = Spread(spread=spread_name, create_spread=True)
        spread.move(path)

    return spread


def share_to_service_acc(spreadsheet, client):
    spread = Spread(spread=spreadsheet)
    spread.share('gdocsync@pmaxcrm.iam.gserviceaccount.com',
                 perm_type='user',
                 role='reader',
                 notify=False)


def get_raw_data_sheet_to_df(spreadsheet, client, cols_to_check, rename_title=None):

    spread = Spread(spread=spreadsheet)

    # Get metadata:
    meta = spread.__dict__.get('_spread_metadata')
    project_id = meta.get('spreadsheetId')
    project_name = meta.get('properties').get('title')

    # Locate raw_data sheet
    sheets = spread.sheets
    try:
        raw_data = [x for x in sheets if 'rawdata' in x.__dict__['_properties']
                    ['title'].replace(' ', '').replace('.', '').lower(
        ) and 'pivot' not in x.__dict__['_properties']
            ['title'].replace(' ', '').replace('.', '').lower()]

        raw_data.sort(key=lambda x: len(x.__dict__['_properties']
                                        ['title'].replace(' ', '').replace('.', '')), reverse=False)
        raw_data_sheet = raw_data[0]
        if rename_title is not None:
            raw_data_sheet.update_title(rename_title)

        df = spread.sheet_records_to_df(
            empty2zero=False, header_rows=1, sheet=raw_data_sheet,
            default_blank=np.nan)

        # Check for column names:
        df_cols_lower = list(
            map(lambda x: x.strip().lower(), list(df.columns)))
        cols_to_check = list(
            map(lambda x: x.strip().lower(), list(set(cols_to_check))))
        df.columns = df_cols_lower
        missing_cols = []
        for col in cols_to_check:
            if col not in df_cols_lower:
                missing_cols.append(col)
        if len(missing_cols) > 0:

            print(
                f"[RawData] Project {project_name} (id: https://docs.google.com/spreadsheets/d/{project_id} ) is missing  " + ", ".join(missing_cols))
            for missing_col in missing_cols:
                df[missing_col] = ''
        # df.columns = map(lambda x: x.replace(
        #     ' ', '_').replace('.', '').lower(), df.columns)
        df = df.loc[df['date'].notnull(), ~df.columns.duplicated(keep='first')]
        # Add additional fields for future lookup and update
        df['_meta_spreadsheetid'] = project_id
        df['_meta_projectname'] = project_name
        df = df.reset_index()
        df['_ref_id'] = '__'.join(['', project_id, ''])
        df['_ref_id'] = df['_ref_id'] + df['index'].astype('str')
        df.drop(['index'], axis=1, inplace=True)

        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        for col in ['revenue', 'cost', 'profit']:
            df[col] = pd.to_numeric(df[col].astype(
                'str').str.replace(',', ''), errors='coerce').fillna(0)
        df = df.infer_objects()
        return df
    except Exception as e:
        print(str(
            e) + f' while getting RawData sheet on {project_name}: https://docs.google.com/spreadsheets/d/{spreadsheet}')


def get_kpi_sheet_to_df(spreadsheet, client, cols_to_check, rename_title=None):

    spread = Spread(spread=spreadsheet)

    # Get metadata:
    meta = spread.__dict__.get('_spread_metadata')
    project_id = meta.get('spreadsheetId')
    project_name = meta.get('properties').get('title')

    # Locate KPI sheet
    sheets = spread.sheets
    try:
        kpi = [x for x in sheets if 'kpi' in x.__dict__['_properties']
               ['title'].replace(' ', '').replace('.', '').lower(
        ) and 'pivot' not in x.__dict__['_properties']
            ['title'].replace(' ', '').replace('.', '').lower()]

        kpi.sort(key=lambda x: len(x.__dict__['_properties']
                                   ['title'].replace(' ', '').replace('.', '')), reverse=False)
        kpi_sheet = kpi[0]
        if rename_title is not None:
            kpi_sheet.update_title(rename_title)

        df = spread.sheet_records_to_df(
            empty2zero=False, header_rows=1, sheet=kpi_sheet,
            default_blank=np.nan)

        # Check for column names:
        df_cols_lower = list(
            map(lambda x: x.strip().lower(), list(df.columns)))
        cols_to_check = list(
            map(lambda x: x.strip().lower(), list(set(cols_to_check))))
        df.columns = df_cols_lower
        missing_cols = []
        for col in cols_to_check:
            if col not in df_cols_lower:
                missing_cols.append(col)
        if len(missing_cols) > 0:

            print(
                f"[KPI] Project {project_name} (id: https://docs.google.com/spreadsheets/d/{project_id} ) is missing  " + ", ".join(missing_cols))
            for missing_col in missing_cols:
                df[missing_col] = ''
        # df.columns = map(lambda x: x.replace(
        #     ' ', '_').replace('.', '').lower(), df.columns)
        df = df.loc[df['date'].notnull(), ~df.columns.duplicated(keep='first')]

        # Add additional fields for future lookup and update
        df['_meta_spreadsheetid'] = project_id
        df['_meta_projectname'] = project_name
        df = df.reset_index()
        df['_ref_id'] = '__'.join(['', project_id, ''])
        df['_ref_id'] = df['_ref_id'] + df['index'].astype('str')
        df.drop(['index'], axis=1, inplace=True)

        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        for col in ['revenue', 'cost', 'profit']:
            df[col] = pd.to_numeric(df[col].astype(
                'str').str.replace(',', ''), errors='coerce').fillna(0)
        df = df.infer_objects()
        return df
    except Exception as e:
        print(str(
            e) + f' while getting KPI sheet on {project_name}: https://docs.google.com/spreadsheets/d/{spreadsheet}')


def update_sheet_title(spreadsheet, raw_title=None, kpi_title=None):
    try:
        spread = Spread(spreadsheet)
        # Get metadata:
        meta = spread.__dict__.get('_spread_metadata')
        project_id = meta.get('spreadsheetId')
        project_name = meta.get('properties').get('title')

        # Locate KPI sheet
        sheets = spread.sheets
        try:
            raw_data = [x for x in sheets if 'rawdata' in x.__dict__['_properties']
                        ['title'].replace(' ', '').replace('.', '').lower(
            ) and 'pivot' not in x.__dict__['_properties']
                ['title'].replace(' ', '').replace('.', '').lower()]

            raw_data.sort(key=lambda x: len(x.__dict__['_properties']
                                            ['title'].replace(' ', '').replace('.', '')), reverse=False)
            if len(raw_data) != 0:
                raw_data_sheet = raw_data[0]
                raw_data_sheet.update_title(str(raw_title))
        except Exception as e:
            print(e)
            print('Could not find Raw data sheet in project' + str(project_name) +
                  'at ' + f'https://docs.google.com/spreadsheets/d/{project_id}')

        try:

            # Locate KPI sheet:
            kpi = [x for x in sheets if 'kpi' in x.__dict__['_properties']
                   ['title'].replace(' ', '').replace('.', '').lower(
            ) and 'pivot' not in x.__dict__['_properties']
                ['title'].replace(' ', '').replace('.', '').lower()]

            kpi.sort(key=lambda x: len(x.__dict__['_properties']
                                       ['title'].replace(' ', '').replace('.', '')), reverse=False)
            if len(kpi) != 0:
                kpi_sheet = kpi[0]
                kpi_sheet.update_title(str(kpi_title))

        except Exception as e:
            print(e)
            print('Could not find KPI sheet in project' + str(project_name) +
                  'at ' + f'https://docs.google.com/spreadsheets/d/{project_id}')
    except Exception as e:
        print(e)
        print('Error in project' + str(project_name) +
              'at ' + f'https://docs.google.com/spreadsheets/d/{project_id}')


def update_raw_data(spreadsheet, client, copy=False, path='/Tableau Data New/Raw Data2', extract_cols=None, engine=None):
    if extract_cols is None:
        extract_cols = []
    else:
        extract_cols = list(
            set([str(x).strip().lower().replace(' ', '_').replace('.', '_') for x in extract_cols]))

    try:
        df = get_raw_data_sheet_to_df(spreadsheet=spreadsheet, client=client,
                                      cols_to_check=[
                                          'Charge Code', 'Client', 'Function', 'date',
                                          'Channel', 'revenue', 'cost', 'profit'
                                      ])

        project_name = df['_meta_projectname'][0]
        project_id = df['_meta_spreadsheetid'][0]
        df.columns = [x.strip().lower().replace(' ', '_').replace('.', '_')
                      for x in list(df.columns)]
        df = df.loc[:, ~df.columns.duplicated()]
        # sub_df = pd.melt(df, id_vars='_ref_id', value_vars=[
        #       x for x in list(df.columns) if x not in extract_cols and x not in ['_meta_spreadsheetid', '_meta_projectname', '_ref_id', '']],
        #       var_name='key', value_name='value').dropna(how='any')
        # sub_df = sub_df[(sub_df['value'] != 0) & (sub_df['value'] != '')]
        df = df.reindex(columns=[x for x in list(
            set(extract_cols + ['_meta_spreadsheetid', '_meta_projectname', '_ref_id']))])
        if copy:
            spread = create_spread_in_folder(
                spread_name=f'RawData v3 - {project_name}', client=client, path=path)
            target_spread_id = spread.__dict__.get(
                '_spread_metadata')['spreadsheetId']

            spread.df_to_sheet(df=df, index=False,
                               replace=True, sheet=project_name)

        if engine is not None:
            # Update main df:
            df.to_sql('pmax_project_performance', engine,
                      method=psql_insert_copy, if_exists='append', index=None)

        print(f'Updated RawData {project_name} (ID: {project_id})')
    except Exception as e:
        print('Got ' + str(e) +
              f' while updating Raw Data for {spreadsheet} )')


def update_kpi(spreadsheet, client, target_spreadsheet_name=None, path='/Tableau Data New/KPI', copy=False, extract_cols=None, engine=None):
    if extract_cols is None:
        extract_cols = []
    else:
        extract_cols = list(
            set([str(x).strip().lower().replace(' ', '_').replace('.', '_') for x in extract_cols]))

    try:
        df = get_kpi_sheet_to_df(spreadsheet=spreadsheet, client=client,
                                 cols_to_check=[
                                     'Charge Code', 'Client', 'Function', 'date',
                                     'Channel', 'revenue', 'cost', 'profit'
                                 ])
        project_name = df['_meta_projectname'][0]
        project_id = df['_meta_spreadsheetid'][0]
        df.columns = [x.strip().lower().replace(' ', '_').replace('.', '_')
                      for x in list(df.columns)]
        df = df.loc[:, (~df.columns.duplicated())]
        # sub_df = pd.melt(df, id_vars='_ref_id', value_vars=[
        #       x for x in list(df.columns) if x not in extract_cols and x not in ['_meta_spreadsheetid', '_meta_projectname', '_ref_id', '']],
        #       var_name='key', value_name='value').dropna(how='any')
        # sub_df = sub_df[(sub_df['value'] != 0) & (sub_df['value'] != '')]
        df = df.reindex(columns=[x for x in list(
            set(extract_cols + ['_meta_spreadsheetid', '_meta_projectname', '_ref_id']))])
        if copy:
            spread = create_spread_in_folder(
                spread_name=f'KPI v3 - {project_name}', client=client, path=path)
            spread.df_to_sheet(df=df, index=False,
                               replace=True, sheet=project_id)
        # df.to_csv(os.path.join(BASE_DIR, 'kpi_v2',
        #                        f'KPI v3 - {project_name}.csv'), index=None)
        print(f'Updated KPI {project_name} (ID: {project_id} )')

        if engine is not None:
            # Update main df:
            df.to_sql('pmax_project_kpi', engine,
                      method=psql_insert_copy, if_exists='append', index=None)
            # Session = sessionmaker()
            # Session.configure(bind=engine)
            # session = Session()
            # session.bulk_insert_mappings('pmax_project_kpi', df.to_dict(orient="records"))
            # session.close()
            # Update sub df:
            # sub_df.to_sql('pmax_project_kpi_sub', engine,
            #               method=psql_insert_copy, if_exists='append', index=None)

    except Exception as e:
        print('Got ' + str(e) + f' while updating KPI for {spreadsheet} ')


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
