import caffeine
import io
import json
import multiprocessing
import os
import time

import psycopg2
from gspread import SpreadsheetNotFound
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


if __name__ == '__main__':
    # print('Waiting 10 mins...')
    # time.sleep(600)
    print('Starting...')
    CONNECTION_STRING = os.environ['PMAX_CONNECTION_STRING']
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
    #     '1uGbox1nJQuV2TywjDC85ak3ytJU5wC-p') if 'copy of' not in x.get('name').lower()][::-1]
    project_list = ['1GtYfB1KzdENNLkEgmJRBMPwQI8I6Yhb0SiiLq-6doyc', '1SpC1kusutR2JQefoOT27SX0SS7ri_EdfrXkq6x_Q-as', '1PYalTH6ekx7se-3_E83fugqgb_S6t4OH12S3yA_RI30', '1IqR2G3R7-7RP3FfyaxsJHeg_29BKT1zY01ysACftwPw', '19jFD_wVoZ2cp-XmT0ltLIANqTNxhpQrdpb0dhvGfl5M', '1PYalTH6ekx7se-3_E83fugqgb_S6t4OH12S3yA_RI30', '1JCDTATto1aGZJa-d8MEEwd6l49_LzYcYP2_Ko78QdXg', '14c_9NCvPyxU2zyXXKMF0LxJiy8s9YEmCjghpS6Sva_w', '1tjrnUcT6GHS9zmO0IirpuYJwt2g_TcHg86-qXfQa_VY', '1_s2htsS8TPLEGyXu7nOqyoJCgwehQPGnNEBZGjfCD9U', '1YwYoOkG6E2NptQ9Owdu85vqHKX6cFI2eOQT9LqIgtd4', '1MtPOeI25Xf6EYezFy3ENuJErVuzYvM7Ya6OCVwqdFZM', '1dze4xB-vBMvPmis7qYJZLgnsb7MjPTVGUT9qdnBzWV8', '14c_9NCvPyxU2zyXXKMF0LxJiy8s9YEmCjghpS6Sva_w', '1Hivg2LWbkgy44J7eX7dVUcatum01geQ0XG3N9myGGdc', '1ZW-7cEbbMpBXLgi2SHQqknQVU5iZRlbr8Vap78lmxB8', '1vPnFM2c29InZicQULj3T6G00lm1fPsUl_qKdoktmpjk', '1pLz965wsTjOAgcE8M9FfkYKM6fXZ7_Gj0HIXKaTe2Jk', '1xuOM6aTNepo00KJ73CG0i7B75wT58ExeSFWHXf-oNHU', '138reYsIjff7l6XfpgivwTSlHwr5V-98Hvm3fQx4FjRU', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1sfBw-tB5Ar3dkLxpo5gV7vkFL5mC2x-_Ro41ikqQ21o', '1i0B1UWjN-0-2Vj7ci_Z8XdF5LzRP9XE-tRyQJRmEYyI', '1RzyMa_PXj4aXdtQaWNsGruNv-pIDLeJgK0KkFnQCDP8', '1bU-H8QQH_aagCNTK7O5vK8t5WQwAxBNGDQTntqf8siA', '16joHW5Ru9jUxAA4ZOlQbaNAD6Xep6gcT-ytgxEr9XRo', '19bnvBBFiM0QXavZRukCGUs7IoEZ18iopNTGkWO0BSKE', '108TZiP4U4NT7PGKhXI0gihGSlYNXXoDtfwpEol-WpsY', '1839gw8Y3mQBNEEmJ9yosgcR1sica8nEmqp4Z0tjjMDA', '1OeKURzKKtZ8qKuhvUkKVK2VmP6dvitGPUiveTtgjVkY', '1ZZ3LBtkykE_tnmdUjfCyXY1utIYBnErR5uZRRH3u4No', '12GtZEM0ZHg0HFnxM_bph-MMyl1zPsiyHmlVNpL0-0bo', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1_jnj-fGwlH5o-KDvhD8VgP1evT5j6sB-jlg85zeodIw', '1en0Q8QywBRsux2WX5c32dZje61LWPAPlqgRMTY0fatA', '1ffgVA4whDuxra4I52QmHJT3ckVSFXtuLJwLlnuyfZuo', '1bzjnFISlAQM5WjlSrkPCGRhjvqgV_jwEdes1cyvCIjk',
                    '1ABzEcRnffDCFOyXS9g806sLpwkTtBlY4BFxwe1wTXJM', '13wMtsM6ZNEUPLy8h5uUhkCzM4Gu6a5M_6v_4ZClR5L8', '11lmr8nrLsN8UIc-K7HVm24Wa7-qZaSiydY3MYxAy-ss', '1IMjqwEJtKfKLeWbRwN-aBH6kcFEh6cYf7Hw1angti4E', '1xuOM6aTNepo00KJ73CG0i7B75wT58ExeSFWHXf-oNHU', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1rQQbCo0NoA04UjAL14Bl3prTLsWtbRV7yAnpCSL7dis', '1gyOKJ_JArFOB18rDs9NGqQhipljsz2lzU0x27na30og', '1YJiAFJlCxkfplWHbflu1BTnQZNLu0C2E5wD6qzjKyNA', '1EW5nSMhDJ_f-5LSBIdI7Rz9Kg_ko2jKr4A9AUzSnHOI', '1ENeFppx5_s2JWLJAQ-QuDT7YwwOYQwydFekdtp5m_MI', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1hvzG9Nfm8xuoTuyIzCXW_dWu9kxp0l0cC2yKl2SpHhc', '1IeVtV4QwQMZTOZToDJBOyUjAyx2YHUZdBzQGV8J6f3M', '1r17gORLWGd8xy73OgKxt2CT0kJdh3Aqbz6DI6IqO9TY', '1ENeFppx5_s2JWLJAQ-QuDT7YwwOYQwydFekdtp5m_MI', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1g2vIXZlYrMdglzjqy_jhw14UEZAgxKLm56X3kkDlYiU', '1Hi_3SdtJEBLUxcUndGzQQ42-mX6oDovw6WDp4gCl-yE', '1pr7DGWXgM0kM8fYRm9Jxro86CUJyfMpMSnm0aEHEvnw', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1j0LcxTNCPBj8dmV0HzISITrgXHLtnfeghF4nOfHEw9U', '1sfBw-tB5Ar3dkLxpo5gV7vkFL5mC2x-_Ro41ikqQ21o', '1PtR0IEeMym-CPLlgTlL_MqV1wZj7I2fgOQAHZp_u8as', '1JF5gVKm75Rt0mWfJWf1a78Eg1CMwuGKmpWRH8KGQVlI', '1P-r4jd2YZciJ2sHc1SUJBtYZeju6V_XrmY1YJIcC54U', '1CwCDu-CyMQRF6dXZpXt6O6ehdHQg7Atdp_HX5JV8IiY', '12p7V7ui0QTQjFClnY1c3qoS_xJYNRDyFn_Hf-g-th00', '1YwjZxDSccv6EKdPcjHGU1kUxXzUZbLHLAetfRqG2KQo', '1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8', '1cSbrppryRDpX7It2eQwLaJx-mq5t0aQe352HjPt11m8', '1QOgZ8F8XUB6ApZTER4s3rhhi1Vr1SnpMFPK6Q8-Ik3k', '1K0OFebySMw1AbX70gEawypQ3agNN4oZqGIsnQhdoMek', '1kuUu5VKl3SOBF42R7Z0b47OTjok_OhWiopwqdK2yb9c', '1ZZ3LBtkykE_tnmdUjfCyXY1utIYBnErR5uZRRH3u4No', '1xTRmdweADjNb5C_XFze_o65-DAzuIFWBs22-O3jAlcU', '1ABzEcRnffDCFOyXS9g806sLpwkTtBlY4BFxwe1wTXJM']

    to_update = [x for x in list(
        set(project_list + execution_list)) if x not in exclude_list]

    # to_update = ['1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8',
    #              '1j0LcxTNCPBj8dmV0HzISITrgXHLtnfeghF4nOfHEw9U']
    print('Updating:' + str(len(to_update)) + ' projects')
    print(to_update[:5])

    with multiprocessing.Pool(8, initializer=worker_init, initargs=(lambda x: update(x, extract_cols=extract_cols, copy=False, engine=engine),)) as p:
        p.map(worker, to_update)

    update('1dze4xB-vBMvPmis7qYJZLgnsb7MjPTVGUT9qdnBzWV8',
           extract_cols=extract_cols, engine=engine)
    # update('1bzjnFISlAQM5WjlSrkPCGRhjvqgV_jwEdes1cyvCIjk',
    #        extract_cols=extract_cols, engine=engine)
    # # update('1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8')

    print('Done')
    caffeine.off()
