from download_data_as_csv_no_lower import *
from gspread_pandas import Spread, Client
import os
import json
import multiprocessing
from gspread import SpreadsheetNotFound


_func = None


def worker_init(func):
    global _func
    _func = func


def worker(x):
    return _func(x)


execution_list = ['1IeVtV4QwQMZTOZToDJBOyUjAyx2YHUZdBzQGV8J6f3M', '1pLz965wsTjOAgcE8M9FfkYKM6fXZ7_Gj0HIXKaTe2Jk', '1IMjqwEJtKfKLeWbRwN-aBH6kcFEh6cYf7Hw1angti4E', '1SpC1kusutR2JQefoOT27SX0SS7ri_EdfrXkq6x_Q-as', '1bU-H8QQH_aagCNTK7O5vK8t5WQwAxBNGDQTntqf8siA', '1ZW-7cEbbMpBXLgi2SHQqknQVU5iZRlbr8Vap78lmxB8', '1Hi_3SdtJEBLUxcUndGzQQ42-mX6oDovw6WDp4gCl-yE', '1PYalTH6ekx7se-3_E83fugqgb_S6t4OH12S3yA_RI30', '1ZZ3LBtkykE_tnmdUjfCyXY1utIYBnErR5uZRRH3u4No', '1ffgVA4whDuxra4I52QmHJT3ckVSFXtuLJwLlnuyfZuo', '11lmr8nrLsN8UIc-K7HVm24Wa7-qZaSiydY3MYxAy-ss', '1P-r4jd2YZciJ2sHc1SUJBtYZeju6V_XrmY1YJIcC54U', '1g2vIXZlYrMdglzjqy_jhw14UEZAgxKLm56X3kkDlYiU', '1r17gORLWGd8xy73OgKxt2CT0kJdh3Aqbz6DI6IqO9TY',
                  '1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8', '1IqR2G3R7-7RP3FfyaxsJHeg_29BKT1zY01ysACftwPw', '1en0Q8QywBRsux2WX5c32dZje61LWPAPlqgRMTY0fatA', '1OOI7lpvBAK4g_M308Xm4Z-m8qZM64APXA_a8PvFIKpY', '1QSFPJOIUuZtWIqxheiqhhsyS1Grp1mUubooFfFpMwUk', '1leYrrtIKHpMphjH-iMiSpkoM_2y-FkBM0NaJVBpABX8', '1Hivg2LWbkgy44J7eX7dVUcatum01geQ0XG3N9myGGdc', '1d9Hj48kYMVefn-F36goqiKv6tH3_QvPrkrBaLIVfIe0', '1rQQbCo0NoA04UjAL14Bl3prTLsWtbRV7yAnpCSL7dis', '14c_9NCvPyxU2zyXXKMF0LxJiy8s9YEmCjghpS6Sva_w', '13wMtsM6ZNEUPLy8h5uUhkCzM4Gu6a5M_6v_4ZClR5L8', '1i0B1UWjN-0-2Vj7ci_Z8XdF5LzRP9XE-tRyQJRmEYyI', '1sfBw-tB5Ar3dkLxpo5gV7vkFL5mC2x-_Ro41ikqQ21o', '1xuOM6aTNepo00KJ73CG0i7B75wT58ExeSFWHXf-oNHU', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1ABzEcRnffDCFOyXS9g806sLpwkTtBlY4BFxwe1wTXJM', '138reYsIjff7l6XfpgivwTSlHwr5V-98Hvm3fQx4FjRU', '1dze4xB-vBMvPmis7qYJZLgnsb7MjPTVGUT9qdnBzWV8', '1T1Vg-o8E-8dUuBNHcqr8rZiIT1khPFq_jyFenEQnVnE', '1IMjqwEJtKfKLeWbRwN-aBH6kcFEh6cYf7Hw1angti4E', '1dze4xB-vBMvPmis7qYJZLgnsb7MjPTVGUT9qdnBzWV8', '1MtPOeI25Xf6EYezFy3ENuJErVuzYvM7Ya6OCVwqdFZM', '1YwYoOkG6E2NptQ9Owdu85vqHKX6cFI2eOQT9LqIgtd4', '1cSbrppryRDpX7It2eQwLaJx-mq5t0aQe352HjPt11m8',
                  '1EW5nSMhDJ_f-5LSBIdI7Rz9Kg_ko2jKr4A9AUzSnHOI']

# target_sheet = {}

exclude_list = ['1VYpgpdSoC3aYCt8Cd5y2HzYQ68Ghnfx1mKFZVlfH7Nw', '1leYrrtIKHpMphjH-iMiSpkoM_2y-FkBM0NaJVBpABX8',
                '1OOI7lpvBAK4g_M308Xm4Z-m8qZM64APXA_a8PvFIKpY', '1mqImKR4SEK_T-c3lUlpMdkMMavHY1scAg2BhKMEZ7J8', '108TZiP4U4NT7PGKhXI0gihGSlYNXXoDtfwpEol-WpsY', '1c90YN6TM8KnikHv4e2z0dgaliRiH9liWdwl9xfPK1s0', '1JF5gVKm75Rt0mWfJWf1a78Eg1CMwuGKmpWRH8KGQVlI', '1SpC1kusutR2JQefoOT27SX0SS7ri_EdfrXkq6x_Q-as', '1rQQbCo0NoA04UjAL14Bl3prTLsWtbRV7yAnpCSL7dis', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1vPnFM2c29InZicQULj3T6G00lm1fPsUl_qKdoktmpjk', '1QMRY9TzDJhl9tahTE76fz6O1gOoHiHgFPEE0ujumEVc', '1ZW-7cEbbMpBXLgi2SHQqknQVU5iZRlbr8Vap78lmxB8', '1Ko4O4FhRDFVE05ift0CPOHYJELZg_-FlSZjpXDJ1TJk', '1wWWGMMMIR_CzDMT7JpS39Qg_840_S5Zz-mAJSBjXkBY', '1_s2htsS8TPLEGyXu7nOqyoJCgwehQPGnNEBZGjfCD9U', '1dze4xB-vBMvPmis7qYJZLgnsb7MjPTVGUT9qdnBzWV8', '1tjrnUcT6GHS9zmO0IirpuYJwt2g_TcHg86-qXfQa_VY', '19bnvBBFiM0QXavZRukCGUs7IoEZ18iopNTGkWO0BSKE', '1PziappvwNUNhREQM3sx72FHly6ftXkQ-L9YCxpJilMw', '1YwYoOkG6E2NptQ9Owdu85vqHKX6cFI2eOQT9LqIgtd4', '14c_9NCvPyxU2zyXXKMF0LxJiy8s9YEmCjghpS6Sva_w', '11lmr8nrLsN8UIc-K7HVm24Wa7-qZaSiydY3MYxAy-ss', '1g2vIXZlYrMdglzjqy_jhw14UEZAgxKLm56X3kkDlYiU', '1i0B1UWjN-0-2Vj7ci_Z8XdF5LzRP9XE-tRyQJRmEYyI', '16joHW5Ru9jUxAA4ZOlQbaNAD6Xep6gcT-ytgxEr9XRo', '1FULEBQbNyVRQic3z9ax3Z8KXV8qTqKMSlJv4ZIu4M0o', '1m5KpsuWgVbwn-qrh-aPa7Ag95e7RCzijVgZKhhYHmm4', '1LHKJW7D_69KEKC8GDdaznseT2gXZh61SAPqBXuYQcYg', '1mRfjE-B6sLLYGQOf7BRczZFKQNXTidf4YozOF0iSnRI', '1GtYfB1KzdENNLkEgmJRBMPwQI8I6Yhb0SiiLq-6doyc', '1xuOM6aTNepo00KJ73CG0i7B75wT58ExeSFWHXf-oNHU', '1mI6Hh-fCKaVbUgwecXDw4ksbrRjxZFq-z0mWEWkVDG8', '1YJiAFJlCxkfplWHbflu1BTnQZNLu0C2E5wD6qzjKyNA', '1YDO5VheKvgTcAmVWLcGqTvnJPVs8VdaB4t2nV4ojrgE', '1ABzEcRnffDCFOyXS9g806sLpwkTtBlY4BFxwe1wTXJM', '14NCIXDyz6N0B5AtOPf2aHCZCq66PZ4w7WKufZ7OTzzg', '1Yeto26mIsYpetQhLCkDlP2uL-oqG-bJ1-2dX0CNmbi4', '1hvzG9Nfm8xuoTuyIzCXW_dWu9kxp0l0cC2yKl2SpHhc', '1sfBw-tB5Ar3dkLxpo5gV7vkFL5mC2x-_Ro41ikqQ21o', '1RzyMa_PXj4aXdtQaWNsGruNv-pIDLeJgK0KkFnQCDP8', '1CwCDu-CyMQRF6dXZpXt6O6ehdHQg7Atdp_HX5JV8IiY', '1IeVtV4QwQMZTOZToDJBOyUjAyx2YHUZdBzQGV8J6f3M', '19z1QiQuNVFl797ZFojxKscxXBWyY97umAlowWmjvJ48', '1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8', '1ZZ3LBtkykE_tnmdUjfCyXY1utIYBnErR5uZRRH3u4No', '1xzq-kKqeNoK2He_u-fWOCZSuCt647BrQ1UTlaTpRtaY',
                '1MtPOeI25Xf6EYezFy3ENuJErVuzYvM7Ya6OCVwqdFZM', '1QOgZ8F8XUB6ApZTER4s3rhhi1Vr1SnpMFPK6Q8-Ik3k', '1qDo-KLmhPWQbNO2iegmn4nLzgwxupEeQKv9iM-axP0I', '1ghITLuYB5H_t6b_ZHq61MYU_dgv5I5izNcHk8lg_TCg', '12GtZEM0ZHg0HFnxM_bph-MMyl1zPsiyHmlVNpL0-0bo', '1PtR0IEeMym-CPLlgTlL_MqV1wZj7I2fgOQAHZp_u8as', '1839gw8Y3mQBNEEmJ9yosgcR1sica8nEmqp4Z0tjjMDA', '1OeKURzKKtZ8qKuhvUkKVK2VmP6dvitGPUiveTtgjVkY', '1pr7DGWXgM0kM8fYRm9Jxro86CUJyfMpMSnm0aEHEvnw', '1hPSQDE-zP4m5qmUmvFY6IxHQ1EO0KU5Az5jU7ABxt4Q', '1IqR2G3R7-7RP3FfyaxsJHeg_29BKT1zY01ysACftwPw', '1CHN_BhpbGJQXVEfHsyYfgxbHmATAUU4BbNXzKjuxuP0', '1en0Q8QywBRsux2WX5c32dZje61LWPAPlqgRMTY0fatA', '1cSbrppryRDpX7It2eQwLaJx-mq5t0aQe352HjPt11m8', '13KkN1QBEzgHuGkndskY52N5v4pbc9zZL-wmTvbO6Bhk', '11Q-gNwvExUiEMU4OteAe9g6RFZvyJknlPqU6KR12cSw', '1Y76FtjKolMct86A_tzsWgMUAV0RMQFeK7bN8q8Tet1s', '1ENeFppx5_s2JWLJAQ-QuDT7YwwOYQwydFekdtp5m_MI', '1bU-H8QQH_aagCNTK7O5vK8t5WQwAxBNGDQTntqf8siA', '1d9Hj48kYMVefn-F36goqiKv6tH3_QvPrkrBaLIVfIe0', '15ke9m38Q-V9W-DBHTGjrQejErvAm0iOaiySUAgClQQc', '138reYsIjff7l6XfpgivwTSlHwr5V-98Hvm3fQx4FjRU', '1P-r4jd2YZciJ2sHc1SUJBtYZeju6V_XrmY1YJIcC54U', '1QSFPJOIUuZtWIqxheiqhhsyS1Grp1mUubooFfFpMwUk', '13OC-hKGLfv-1hVnInAnRFgfABobplcquAUcOe5j_AeA', '1gShTlX_1f-_9oARr1iugPaukQHPYy5W4U7bizIKnrWA', '1pLz965wsTjOAgcE8M9FfkYKM6fXZ7_Gj0HIXKaTe2Jk', '1YwjZxDSccv6EKdPcjHGU1kUxXzUZbLHLAetfRqG2KQo', '1PS5vGHM7rTnsoAiIgx_RQFbyiK3qnVps0zlmK4jTCC8', '1_jaywf4RxLDcxfTPNPKtLpGBL2kaCuMlB-SbTBYomh4', '1ffgVA4whDuxra4I52QmHJT3ckVSFXtuLJwLlnuyfZuo', '12p7V7ui0QTQjFClnY1c3qoS_xJYNRDyFn_Hf-g-th00', '1-vo3qCDqS7MCR253vKMgJ5yXF-4NFg9USjM4G_3cK_c', '1fiW_xkFgnv0PdR97nOQirftoLkgAiinptu4PMwfZPyM', '1PYalTH6ekx7se-3_E83fugqgb_S6t4OH12S3yA_RI30', '1_jnj-fGwlH5o-KDvhD8VgP1evT5j6sB-jlg85zeodIw', '13wDR1PsrqQnG7ScUqfbiVvE96P1W23dj1-nh6-UpH5M', '1a1VM_5DLyYVi3iQDrRYP8wSwl07VVvFo5wm4kbhYTUQ', '1IMjqwEJtKfKLeWbRwN-aBH6kcFEh6cYf7Hw1angti4E', '1R8Z4XPaK10TVNH3kpsxr2oRinWgjwDw2A5wdXjOKrjk', '1Hi_3SdtJEBLUxcUndGzQQ42-mX6oDovw6WDp4gCl-yE', '1EW5nSMhDJ_f-5LSBIdI7Rz9Kg_ko2jKr4A9AUzSnHOI', '13wMtsM6ZNEUPLy8h5uUhkCzM4Gu6a5M_6v_4ZClR5L8', '1FqRsmaa27qrGDxxsicOerJix_X7UiACJ783asluIys0', '1Hivg2LWbkgy44J7eX7dVUcatum01geQ0XG3N9myGGdc', '1o9yFOQlJPHt7XHNMPoknrESRG2Xs8PTlQFNKwlcknSg', '1r17gORLWGd8xy73OgKxt2CT0kJdh3Aqbz6DI6IqO9TY']


def update(sheet_id, extract_cols=None, copy=False):
    update_raw_data(spreadsheet=sheet_id, client=client,
                    copy=copy, extract_cols=extract_cols)
    # update_kpi(spreadsheet=sheet_id, client=client,
    #            copy=copy, extract_cols=extract_cols)


if __name__ == '__main__':
    client = Client()
    extract_cols = [
        'Charge Code', 'Client', 'Function', 'Date',
        'Channel', 'Revenue', 'Cost', 'Profit'
    ]

    # # Update 2019 projects only
    project_list = [x.get('id') for x in client.list_spreadsheet_files_in_folder(
        '1uGbox1nJQuV2TywjDC85ak3ytJU5wC-p') if 'copy of' not in x.get('name').lower()]

    to_update = [x for x in list(
        set(project_list + execution_list)) if x not in exclude_list]
    print('Updating:' + str(len(to_update)) + ' projects')
    print(to_update[:5])

    with multiprocessing.Pool(8, initializer=worker_init, initargs=(lambda x: update(x, extract_cols=extract_cols, copy=True),)) as p:
        p.map(worker, to_update)

    # update('1EW5nSMhDJ_f-5LSBIdI7Rz9Kg_ko2jKr4A9AUzSnHOI')
    # update('1dze4xB-vBMvPmis7qYJZLgnsb7MjPTVGUT9qdnBzWV8')
    # update('1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8')

    print('Done')
