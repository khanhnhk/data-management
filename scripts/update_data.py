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

from download_data_as_csv_no_lower import *


_func = None


def worker_init(func):
    global _func
    _func = func


def worker(x):
    return _func(x)


execution_list = []

# target_sheet = {}
exclude_list = []


def update(sheet_id, extract_cols=None, copy=False, engine=None):
    update_raw_data(spreadsheet=sheet_id, client=client,
                    copy=copy, extract_cols=extract_cols, engine=engine)
    update_kpi(spreadsheet=sheet_id, client=client,
               copy=copy, extract_cols=extract_cols, engine=engine)


# scope = ['https://spreadsheets.google.com/feeds',
#          'https://www.googleapis.com/auth/drive']

# credentials = ServiceAccountCredentials.from_json_keyfile_name('gspread-april-2cd … ba4.json', scope)

# gc = gspread.authorize(credentials='./google_credentials.json')


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
    # print('Waiting 10 mins...')
    # time.sleep(600)
    print('Starting...')
    CONNECTION_STRING = os.environ['PMAX_AWS_CONNECTION_STRING']
    # print(CONNECTION_STRING)

    engine = create_engine(CONNECTION_STRING)

    # Delete pmax_project_performance:
    # engine.execute('DELETE FROM pmax_project_performance')
    # engine.execute('DELETE FROM pmax_kpi_performance')

    client = Client()
    extract_cols = [
        'Charge Code', 'Client', 'Function', 'Date',
        'Channel', 'Revenue', 'Cost', 'Profit'
    ]

    # Update 2019 projects only
    # project_list = [x.get('id') for x in client.list_spreadsheet_files_in_folder(
    # #     '1uGbox1nJQuV2TywjDC85ak3ytJU5wC-p') if 'copy of' not in x.get('name').lower()][::-1]
    project_list = ['12GtZEM0ZHg0HFnxM_bph-MMyl1zPsiyHmlVNpL0-0bo', '13wMtsM6ZNEUPLy8h5uUhkCzM4Gu6a5M_6v_4ZClR5L8', '14c_9NCvPyxU2zyXXKMF0LxJiy8s9YEmCjghpS6Sva_w', '15ke9m38Q-V9W-DBHTGjrQejErvAm0iOaiySUAgClQQc', '16FP8f1sAeviH5CrQExtKGmyPnR9zStGkM1N-Bg0B6BI', '17VT9POm0426RL0U72K89FlWMXsqnJqRvfEhOtAq1Onc', '19jFD_wVoZ2cp-XmT0ltLIANqTNxhpQrdpb0dhvGfl5M', '1AkAHPQCoPWfwoff54tRt0_CGfMOWE8pOhhVLg0V1ZNw', '1apLp4MsKbA5Jh5yh-cBh8C64nRwU0tQMBXJCfqPy4yw', '1bOyzrgUxMPsd8RgiXfbqH6I4qHiSq60Sp9_N_bAD57A', '1bzjnFISlAQM5WjlSrkPCGRhjvqgV_jwEdes1cyvCIjk', '1cSbrppryRDpX7It2eQwLaJx-mq5t0aQe352HjPt11m8', '1dze4xB-vBMvPmis7qYJZLgnsb7MjPTVGUT9qdnBzWV8', '1en0Q8QywBRsux2WX5c32dZje61LWPAPlqgRMTY0fatA', '1ffgVA4whDuxra4I52QmHJT3ckVSFXtuLJwLlnuyfZuo', '1Hivg2LWbkgy44J7eX7dVUcatum01geQ0XG3N9myGGdc', '1i0B1UWjN-0-2Vj7ci_Z8XdF5LzRP9XE-tRyQJRmEYyI', '1IeVtV4QwQMZTOZToDJBOyUjAyx2YHUZdBzQGV8J6f3M', '1IqR2G3R7-7RP3FfyaxsJHeg_29BKT1zY01ysACftwPw', '1K0OFebySMw1AbX70gEawypQ3agNN4oZqGIsnQhdoMek', '1K75r8aR4gG3sCzt2qTOG9glYXTPmeLDNOuMocJfn4NI', '1MtPOeI25Xf6EYezFy3ENuJErVuzYvM7Ya6OCVwqdFZM', '1-nHUi1o2_Oks0vQpzw88L7Gelx79RBNc3kVFZhOOB44',
                    '1pLz965wsTjOAgcE8M9FfkYKM6fXZ7_Gj0HIXKaTe2Jk', '1PYalTH6ekx7se-3_E83fugqgb_S6t4OH12S3yA_RI30', '1r17gORLWGd8xy73OgKxt2CT0kJdh3Aqbz6DI6IqO9TY', '1rQQbCo0NoA04UjAL14Bl3prTLsWtbRV7yAnpCSL7dis', '1sfBw-tB5Ar3dkLxpo5gV7vkFL5mC2x-_Ro41ikqQ21o', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1tGOKFUtHZf4Spz270_n4x2kcYlY0XAOzSj2dc6cYP14', '1v47JWn5WEtmB63EpUdrDRl0I50spTcc9ignCDdj_tcQ', '1xuOM6aTNepo00KJ73CG0i7B75wT58ExeSFWHXf-oNHU', '1YwYoOkG6E2NptQ9Owdu85vqHKX6cFI2eOQT9LqIgtd4', '1ZjkGjGHJVqYGPQ6N_v1uo8KLItrzTtTvXWCXjeQLdQs', '1ZZ3LBtkykE_tnmdUjfCyXY1utIYBnErR5uZRRH3u4No', '13OC-hKGLfv-1hVnInAnRFgfABobplcquAUcOe5j_AeA', '1FKfZrZmHYKTbAEiEE0ewpWfo1U9El4PWlYbA9LTQsJo', '1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8', '1PS5vGHM7rTnsoAiIgx_RQFbyiK3qnVps0zlmK4jTCC8', '1RjkOkJPE9vL43N4TVEPM4OldB2S_eZv2ASkJh2JZUXA', '1RzyMa_PXj4aXdtQaWNsGruNv-pIDLeJgK0KkFnQCDP8', '1T1Vg-o8E-8dUuBNHcqr8rZiIT1khPFq_jyFenEQnVnE', '1E7w_3Pxcb0GYQr84yLY7_jdn29XJPnnIF9-2dtAE6dA', '1jqjm_mfq_NA6lLsMiO0PGw7lDaE-2p44etqiADRJR34', '16G3uvpNmWTccby5SJqKHYQqiX-6gEbBprLa3CYcg7Xo', '1kUgyfa0FlYPtnGsQ3N8X8PXHaALrjEKgvff5AwTfuTE', '1UjJNM9rHU8nfLLpjCGdggU8xFet6Iwn8CQ-iOMpKmbI']

    to_update = [x for x in list(
        set(project_list + execution_list)) if x not in exclude_list]

    to_update = [
        '1HQCw0ULE0R9DfKAchJAJ5QB1YrnOYp5shJH94MY0Khk',
        '19jFD_wVoZ2cp-XmT0ltLIANqTNxhpQrdpb0dhvGfl5M',

    ]
    print('Updating:' + str(len(to_update)) + ' projects')

    # with multiprocessing.Pool(6, initializer=worker_init, initargs=(lambda x: update(x, extract_cols=extract_cols, copy=False, engine=engine),)) as p:
    #     p.map(worker, list(set(to_update)))
    for i in list(set(to_update)):
        # time.sleep(3)
        update(i, extract_cols=extract_cols, engine=engine)
        # update_sheet_title(
        #     spreadsheet=i, raw_title='Raw Data', kpi_title='KPIs')
        time.sleep(4)
        # share_permission(i)

    print('Done')
    caffeine.off()
