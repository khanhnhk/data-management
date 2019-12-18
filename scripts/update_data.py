# %%
import caffeine
import io
import json
import multiprocessing
import os
import time
import psycopg2
from gspread import SpreadsheetNotFound
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_pandas import Client, Spread
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from download_data_as_csv_no_lower import *

_func = None


def worker_init(func):
    global _func
    _func = func


def worker(x):
    return _func(x)


def update(sheet_id, extract_cols=None, copy=False, engine=None, error_list=None, update_sub=False):
    update_raw_data(spreadsheet=sheet_id, client=client,
                    copy=copy, extract_cols=extract_cols, engine=engine, error_list=error_list, update_sub=update_sub)
    update_kpi(spreadsheet=sheet_id, client=client,
               copy=copy, extract_cols=extract_cols, engine=engine, error_list=error_list, update_sub=update_sub)


def share_permission(sheet_id):
    try:
        c = Spread(spread=sheet_id)
        c.add_permission(
            'gdocsync@pmaxcrm.iam.gserviceaccount.com|reader|no')
        print(f'Shared {sheet_id} to service account.')
    except Exception as e:
        print(f'Failed to share {sheet_id} to service account.')
        print(e)


if __name__ == '__main__':
    print('Starting...')
    CONNECTION_STRING = os.environ['PMAX_AWS_CONNECTION_STRING']
    UPDATE_MAIN = True
    UPDATE_SUB = True
    MAX_RETRIES = 3

    engine = create_engine(CONNECTION_STRING)
    conn = engine.connect()
    client = Client()
    extract_cols = [
        'Charge Code', 'Client', 'Function', 'Date',
        'Channel', 'Revenue', 'Cost', 'Profit'
    ]

    # # Update projects in execution only
    project_list = [i[0] for i in engine.execute(
        'SELECT dashboard_in_execution()').fetchall()]
    exclude_list = []
    error_list = []

    to_update = [x for x in list(
        set(project_list)) if x not in exclude_list]

    statements = {}
    # Remove current data:
    if UPDATE_MAIN:
        statements['s1'] = text(
            "DELETE FROM pmax_project_performance WHERE _meta_spreadsheetid IN :id_list")
        statements['s2'] = text(
            "DELETE FROM pmax_project_kpi WHERE _meta_spreadsheetid IN :id_list")

    if UPDATE_SUB:
        statements['s3'] = text(
            "DELETE FROM pmax_project_raw_sub WHERE _meta_spreadsheetid IN :id_list")
        statements['s4'] = text(
            "DELETE FROM pmax_project_kpi_sub WHERE _meta_spreadsheetid IN :id_list")

    for _, statement in statements.items():
        conn.execute(statement, id_list=tuple(to_update))
    conn.close()
    print('Updating:' + str(len(to_update)) + ' projects')
    for i in list(set(to_update)):
        update(i, extract_cols=extract_cols,
               engine=engine, error_list=error_list, update_sub=UPDATE_SUB)

    if len(error_list) > 0:
        retry = 1
        while len(set(error_list)) > 0 and retry <= MAX_RETRIES:
            print('\n'*3)
            print('-'*10)
            print(f'Waiting to update error projects... (attempt {retry})')
            print(error_list[:5])
            time.sleep(180)
            for i in list(set(error_list)):
                update(i, extract_cols=extract_cols,
                       engine=engine, update_sub=UPDATE_SUB, error_list=error_list)
            retry += 1

    print('Done')

    caffeine.off()
