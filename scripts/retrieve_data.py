from utils import *
import pandas as pd
from gspread_pandas import Spread, Client
import re
import json


def get_raw_data_sheet(spreadsheet, client):
    spread = Spread(spread=spreadsheet)
    sheets = spread.sheets
    print(sheets[0].__dict__)
    raw = [x for x in sheets if x.__dict__['_properties']
           ['title'].replace(' ', '').lower() == 'rawdata']
    print(sheets)
    print(raw[0])


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


if __name__ == '__main__':
    client = Client()
    get_raw_data_sheet(
        spreadsheet='1sfBw-tB5Ar3dkLxpo5gV7vkFL5mC2x-_Ro41ikqQ21o', client=client)
