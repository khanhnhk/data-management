import json
import psycopg2
import pickle
import os.path
import csv
from io import StringIO
from sqlalchemy import create_engine
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


def authorize_google_sheets(conf_file, scopes):
    with open(conf_file, 'r') as f:
        conf = json.load(f)
    creds = None
    CREDENTIALS_PATH = conf.get('CREDENTIALS').get('GOOGLE_PATH')
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
                CREDENTIALS_PATH, scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    return service.spreadsheets()


def connect_postgres(conf_file, workspace='dashboard_digital_ocean'):
    with open(conf_file, 'r') as f:
        conf = json.load(f)['DATABASE'][workspace]
    connection = psycopg2.connect(user=conf['user'],
                                  password=conf['password'],
                                  host=conf['host'],
                                  port=conf['port'],
                                  database=conf.get('database', ''))
    return connection


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
