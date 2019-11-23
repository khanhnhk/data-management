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
        c = gc()
        c.insert_permission(
            sheet_id, 'gdocsync@pmaxcrm.iam.gserviceaccount.com', perm_type='user', role='reader')
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
    project_list = ['1xuOM6aTNepo00KJ73CG0i7B75wT58ExeSFWHXf-oNHU', '1RzyMa_PXj4aXdtQaWNsGruNv-pIDLeJgK0KkFnQCDP8', '1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8', '1v47JWn5WEtmB63EpUdrDRl0I50spTcc9ignCDdj_tcQ', '13wMtsM6ZNEUPLy8h5uUhkCzM4Gu6a5M_6v_4ZClR5L8', '1YwYoOkG6E2NptQ9Owdu85vqHKX6cFI2eOQT9LqIgtd4', '1pLz965wsTjOAgcE8M9FfkYKM6fXZ7_Gj0HIXKaTe2Jk', '1P-r4jd2YZciJ2sHc1SUJBtYZeju6V_XrmY1YJIcC54U', '1xzq-kKqeNoK2He_u-fWOCZSuCt647BrQ1UTlaTpRtaY', '16joHW5Ru9jUxAA4ZOlQbaNAD6Xep6gcT-ytgxEr9XRo', '1ffgVA4whDuxra4I52QmHJT3ckVSFXtuLJwLlnuyfZuo', '1en0Q8QywBRsux2WX5c32dZje61LWPAPlqgRMTY0fatA', '1IMjqwEJtKfKLeWbRwN-aBH6kcFEh6cYf7Hw1angti4E', '1SpC1kusutR2JQefoOT27SX0SS7ri_EdfrXkq6x_Q-as', '1r17gORLWGd8xy73OgKxt2CT0kJdh3Aqbz6DI6IqO9TY', '1bOyzrgUxMPsd8RgiXfbqH6I4qHiSq60Sp9_N_bAD57A', '1bU-H8QQH_aagCNTK7O5vK8t5WQwAxBNGDQTntqf8siA', '1ZW-7cEbbMpBXLgi2SHQqknQVU5iZRlbr8Vap78lmxB8', '1AoW_5VuqFcuSlRd9MN64qsfaaiEWvneBvSPBYFyFgE0', '1Hi_3SdtJEBLUxcUndGzQQ42-mX6oDovw6WDp4gCl-yE', '1ZZ3LBtkykE_tnmdUjfCyXY1utIYBnErR5uZRRH3u4No', '1PYalTH6ekx7se-3_E83fugqgb_S6t4OH12S3yA_RI30', '1K0OFebySMw1AbX70gEawypQ3agNN4oZqGIsnQhdoMek', '1bzjnFISlAQM5WjlSrkPCGRhjvqgV_jwEdes1cyvCIjk', '1IeVtV4QwQMZTOZToDJBOyUjAyx2YHUZdBzQGV8J6f3M', '11lmr8nrLsN8UIc-K7HVm24Wa7-qZaSiydY3MYxAy-ss', '1FULEBQbNyVRQic3z9ax3Z8KXV8qTqKMSlJv4ZIu4M0o', '1g2vIXZlYrMdglzjqy_jhw14UEZAgxKLm56X3kkDlYiU', '1tjrnUcT6GHS9zmO0IirpuYJwt2g_TcHg86-qXfQa_VY', '1IqR2G3R7-7RP3FfyaxsJHeg_29BKT1zY01ysACftwPw', '19jFD_wVoZ2cp-XmT0ltLIANqTNxhpQrdpb0dhvGfl5M',
                    '14c_9NCvPyxU2zyXXKMF0LxJiy8s9YEmCjghpS6Sva_w', '12GtZEM0ZHg0HFnxM_bph-MMyl1zPsiyHmlVNpL0-0bo', '1Hivg2LWbkgy44J7eX7dVUcatum01geQ0XG3N9myGGdc', '1rQQbCo0NoA04UjAL14Bl3prTLsWtbRV7yAnpCSL7dis', '1sfBw-tB5Ar3dkLxpo5gV7vkFL5mC2x-_Ro41ikqQ21o', '1ABzEcRnffDCFOyXS9g806sLpwkTtBlY4BFxwe1wTXJM', '1JCDTATto1aGZJa-d8MEEwd6l49_LzYcYP2_Ko78QdXg', '1fiW_xkFgnv0PdR97nOQirftoLkgAiinptu4PMwfZPyM', '1cSbrppryRDpX7It2eQwLaJx-mq5t0aQe352HjPt11m8', '1MtPOeI25Xf6EYezFy3ENuJErVuzYvM7Ya6OCVwqdFZM', '1dze4xB-vBMvPmis7qYJZLgnsb7MjPTVGUT9qdnBzWV8', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '138reYsIjff7l6XfpgivwTSlHwr5V-98Hvm3fQx4FjRU', '1FKfZrZmHYKTbAEiEE0ewpWfo1U9El4PWlYbA9LTQsJo', '1tGOKFUtHZf4Spz270_n4x2kcYlY0XAOzSj2dc6cYP14', '1apLp4MsKbA5Jh5yh-cBh8C64nRwU0tQMBXJCfqPy4yw', '15ke9m38Q-V9W-DBHTGjrQejErvAm0iOaiySUAgClQQc', '1K75r8aR4gG3sCzt2qTOG9glYXTPmeLDNOuMocJfn4NI', '17VT9POm0426RL0U72K89FlWMXsqnJqRvfEhOtAq1Onc', '13OC-hKGLfv-1hVnInAnRFgfABobplcquAUcOe5j_AeA', '1808Bj1lJGcyTROBt8B3NzVraCN7eVyuegInhmH0-lQE', '1ejd-Z6G_Z1XHgYjiLMMoMU_RIL2iPcHyhax2tZjyCHI', '1EW5nSMhDJ_f-5LSBIdI7Rz9Kg_ko2jKr4A9AUzSnHOI', '1i0B1UWjN-0-2Vj7ci_Z8XdF5LzRP9XE-tRyQJRmEYyI', '1_jaywf4RxLDcxfTPNPKtLpGBL2kaCuMlB-SbTBYomh4', '1pr7DGWXgM0kM8fYRm9Jxro86CUJyfMpMSnm0aEHEvnw', '1PS5vGHM7rTnsoAiIgx_RQFbyiK3qnVps0zlmK4jTCC8', '1T1Vg-o8E-8dUuBNHcqr8rZiIT1khPFq_jyFenEQnVnE', '1Yeto26mIsYpetQhLCkDlP2uL-oqG-bJ1-2dX0CNmbi4', '1AkAHPQCoPWfwoff54tRt0_CGfMOWE8pOhhVLg0V1ZNw', '1RjkOkJPE9vL43N4TVEPM4OldB2S_eZv2ASkJh2JZUXA', '16FP8f1sAeviH5CrQExtKGmyPnR9zStGkM1N-Bg0B6BI']

    to_update = [x for x in list(
        set(project_list + execution_list)) if x not in exclude_list]

    to_update = [
        '1Hi_3SdtJEBLUxcUndGzQQ42-mX6oDovw6WDp4gCl-yE',
        # '1pr7DGWXgM0kM8fYRm9Jxro86CUJyfMpMSnm0aEHEvnw',
        # '1RjkOkJPE9vL43N4TVEPM4OldB2S_eZv2ASkJh2JZUXA',
    ]
    print('Updating:' + str(len(to_update)) + ' projects')
    # print(to_update[:5])

    # with multiprocessing.Pool(6, initializer=worker_init, initargs=(lambda x: update(x, extract_cols=extract_cols, copy=False, engine=engine),)) as p:
    #     p.map(worker, list(set(to_update)))
    for i in list(set(to_update)):
        time.sleep(3)
        update(i, extract_cols=extract_cols, engine=engine)
        time.sleep(3)
        # share_permission(i)

    # update('15ke9m38Q-V9W-DBHTGjrQejErvAm0iOaiySUAgClQQc',
    #        extract_cols=extract_cols, engine=engine)
    # update('1apLp4MsKbA5Jh5yh-cBh8C64nRwU0tQMBXJCfqPy4yw',
    #        extract_cols=extract_cols, engine=engine)
    # update('1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8',
    #        extract_cols=extract_cols, engine=engine)
    # update('1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ')

    print('Done')
    caffeine.off()
