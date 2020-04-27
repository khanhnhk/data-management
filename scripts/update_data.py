
import io
import json
import multiprocessing
import os
import time
import psycopg2
from gspread import SpreadsheetNotFound
import gspread
import caffeine
from oauth2client.service_account import ServiceAccountCredentials
from gspread_pandas import Client, Spread
from sqlalchemy import create_engine
from sqlalchemy.sql import text


from download_data_as_csv_no_lower import *
from apiclient import discovery
from httplib2 import Http
from oauth2client import file, client, tools
_func = None


def worker_init(func):
    global _func
    _func = func


def worker(x):
    return _func(x)


def update(sheet_id, extract_cols=None, copy=False, engine=None, error_list_raw=None, error_list_kpi=None, update_sub=False, update_main=True):
    update_raw_data(spreadsheet=sheet_id, client=client,
                    copy=copy, extract_cols=extract_cols, engine=engine, error_list_raw=error_list_raw, update_sub=update_sub, update_main=update_main)

    update_kpi(spreadsheet=sheet_id, client=client,
               copy=copy, extract_cols=extract_cols, engine=engine, error_list_kpi=error_list_kpi, update_sub=update_sub, update_main=update_main)

    update_creative(spreadsheet=sheet_id, extract_cols=creative_cols,
                    client=client, engine=engine, error_list_raw=error_list_raw)

    update_planning(spreadsheet=sheet_id, extract_cols=planning_cols,
                    client=client, engine=engine, error_list_raw=error_list_raw)


def share_permission(sheet_id):
    try:
        c = Spread(spread=sheet_id)
        c.add_permission(
            'khanhnh994@gmail.com|reader|no')
        print(f'Shared {sheet_id} to service account.')
    except Exception as e:
        print(f'Failed to share {sheet_id} to service account.')
        print(e)


if __name__ == '__main__':
    print('Starting...')
    CONNECTION_STRING = os.environ['PMAX_AWS_CONNECTION_STRING']
    UPDATE_MAIN = True
    UPDATE_SUB = False
    MAX_RETRIES = 2

    engine = create_engine(CONNECTION_STRING)
    conn = engine.connect()
    client = Client()
    extract_cols = [
        'Charge Code', 'Client', 'Function', 'Date',
        'Channel', 'Revenue', 'Cost', 'Profit'
    ]

    creative_cols = ['Charge Code', 'Client', 'date', 'revenue', 'cost',
                     'profit', 'creative - type', 'creative - source', 'channel']
    planning_cols = [
        'Charge Code', 'Client', 'Function', 'Date',
        'Channel', 'Revenue', 'Cost', 'Profit', 'Subchannel'
    ]
    # # Update all dashboards:
    # SCOPES = ['https://www.googleapis.com/auth/drive.readonly.metadata']

    # store = file.Storage('storage.json')
    # creds = store.get()
    # if not creds or creds.invalid:
    #     flow = client.flow_from_clientsecrets('client_secrets.json', SCOPES)
    #     creds = tools.run_flow(flow, store)

    # drive_service = discovery.build(
    #     'drive', 'v3', http=creds.authorize(Http()))

    # all_dashboards = []

    # page_token = None
    # while True:
    #     response = drive_service.files().list(q="name contains 'INTERNAL dashboard' and not name contains 'Copy' and not name contains 'RawData' and not name contains 'KPI' and trashed = False and mimeType = 'application/vnd.google-apps.spreadsheet' and not name contains '.xls'",
    #                                           spaces='drive',
    #                                           fields='nextPageToken, files(id, name)',
    #                                           pageToken=page_token).execute()
    #     for file in response.get('files', []):
    #         all_dashboards.append(file.get('id'))
    #         print('Found file: %s (%s)' % (file.get('name'), file.get('id')))
    #     page_token = response.get('nextPageToken', None)
    #     if page_token is None:
    #         break

    # # Update projects in execution only
    # project_list = [i[0] for i in engine.execute("SELECT dashboard_in_execution();").fetchall()]
    # project_list = [i[0] for i in engine.execute(
    #     """select "_meta_spreadsheetid" from pmax_project_performance 
    #         where date_trunc('month',date) = '2020-03-01'
    #         group by 1 
    #         having sum(revenue) > 0 or sum(cost) > 0 """).fetchall()]
    project_list = [i[0] for i in engine.execute(
        """select DISTINCT substring(regexp_split_to_table(re.internal_dashboard, '[;,]'), '([^/]{44})') 
        from pmax_crm_launching_request re
        join pmax_crm_projects pro on pro.id = re.project_id
        where pro.charge_code in 
            ('CECOL001','CEDOM001','CEFEC013','CEFEC011','CEFEC017','CEFEC019','CEFEC016','CEILA009','CELGC001','CELOC001','CELOG001','CEMRI001','CEMIT001','CENGO001','CEPNJ028','CEPNJ026','CEPNJ027','CEPNJ001','CEPNJ022','CEPNJ023','CEPNJ029','CEPNJ024','CESTA001','CEUNI001','CEUTO001','CEVIE001','CEVSM001','CEVST001','CEVTA018','CEWAT001','CEFEC020','CEVCM003','CEDUR001','CEGHN002')
         """).fetchall()]
    project_list = ['1lkSQZFnkPSkXsEj1gs8Ch9mfRKNFbnMa7VNb_lEA7e8']
    # project_list = ['1-giqGuDras3MLmOztJSYvp9BFLSrjIRWhOCPNJdZpoo']
    exclude_list = []
    error_list_raw = []
    error_list_kpi = []

    to_update = [x for x in list(
        set(project_list)) if x not in exclude_list]
    # to_update = [x for x in list(
    #     set(all_dashboards)) if x not in exclude_list]
    print('Updating:' + str(len(to_update)) + ' projects')
    statements = {}
    # Remove current data:
    if UPDATE_MAIN:
        print('Deleting main data...')
        statements['s1'] = text(
            "DELETE FROM pmax_project_performance WHERE _meta_spreadsheetid IN :id_list")
        statements['s2'] = text(
            "DELETE FROM pmax_project_kpi WHERE _meta_spreadsheetid IN :id_list")
        statements['s3'] = text(
            "DELETE FROM pmax_creative_performance WHERE _meta_spreadsheetid IN :id_list")

    if UPDATE_SUB:
        print('Deleting sub data...')
        statements['s4'] = text(
            "DELETE FROM pmax_project_raw_sub WHERE _meta_spreadsheetid IN :id_list")
        statements['s5'] = text(
            "DELETE FROM pmax_project_kpi_sub WHERE _meta_spreadsheetid IN :id_list")

    for _, statement in statements.items():
        conn.execute(statement, id_list=tuple(to_update))
    conn.close()

    for i in list(set(to_update)):
        update(i, extract_cols=extract_cols,
               engine=engine, error_list_raw=error_list_raw, error_list_kpi=error_list_kpi, update_main=UPDATE_MAIN, update_sub=UPDATE_SUB)
        # share_permission(i)

    error_list = list(set(error_list_raw).union(set(error_list_kpi)))
    if len(error_list) > 0:
        retry = 1
        while len(set(error_list)) > 0 and retry <= MAX_RETRIES:
            print('\n'*3)
            print('-'*10)
            print(f'Waiting to update error projects... (attempt {retry})')
            print(len(error_list))
            print(error_list[:5])
            time.sleep(180)
            for i in list(set(error_list)):
                if i in error_list_raw:
                    update_raw_data(i, extract_cols=extract_cols,
                                    engine=engine, update_main=UPDATE_MAIN, update_sub=UPDATE_SUB, error_list_raw=error_list_raw, client=Client())
                if i in error_list_kpi:
                    update_kpi(i, extract_cols=extract_cols,
                               engine=engine, update_main=UPDATE_MAIN, update_sub=UPDATE_SUB, error_list_kpi=error_list_kpi, client=Client())

            error_list = list(set(error_list_raw).union(set(error_list_kpi)))
            retry += 1

    print('Done')
    caffeine.off()
