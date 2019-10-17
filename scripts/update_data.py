from download_data_as_csv_no_lower import *
from gspread_pandas import Spread, Client
import os

update_list = ['1IeVtV4QwQMZTOZToDJBOyUjAyx2YHUZdBzQGV8J6f3M', '1pLz965wsTjOAgcE8M9FfkYKM6fXZ7_Gj0HIXKaTe2Jk', '1IMjqwEJtKfKLeWbRwN-aBH6kcFEh6cYf7Hw1angti4E', '1SpC1kusutR2JQefoOT27SX0SS7ri_EdfrXkq6x_Q-as', '1bU-H8QQH_aagCNTK7O5vK8t5WQwAxBNGDQTntqf8siA', '1ZW-7cEbbMpBXLgi2SHQqknQVU5iZRlbr8Vap78lmxB8', '1Hi_3SdtJEBLUxcUndGzQQ42-mX6oDovw6WDp4gCl-yE', '1PYalTH6ekx7se-3_E83fugqgb_S6t4OH12S3yA_RI30', '1ZZ3LBtkykE_tnmdUjfCyXY1utIYBnErR5uZRRH3u4No', '1ffgVA4whDuxra4I52QmHJT3ckVSFXtuLJwLlnuyfZuo', '11lmr8nrLsN8UIc-K7HVm24Wa7-qZaSiydY3MYxAy-ss', '1P-r4jd2YZciJ2sHc1SUJBtYZeju6V_XrmY1YJIcC54U', '1g2vIXZlYrMdglzjqy_jhw14UEZAgxKLm56X3kkDlYiU', '1r17gORLWGd8xy73OgKxt2CT0kJdh3Aqbz6DI6IqO9TY',
               '1OME0-WfjQWZK45v00PsOXRPo-A6X5Vvc0UXn4hZRFN8', '1IqR2G3R7-7RP3FfyaxsJHeg_29BKT1zY01ysACftwPw', '1en0Q8QywBRsux2WX5c32dZje61LWPAPlqgRMTY0fatA', '1OOI7lpvBAK4g_M308Xm4Z-m8qZM64APXA_a8PvFIKpY', '1QSFPJOIUuZtWIqxheiqhhsyS1Grp1mUubooFfFpMwUk', '1leYrrtIKHpMphjH-iMiSpkoM_2y-FkBM0NaJVBpABX8', '1Hivg2LWbkgy44J7eX7dVUcatum01geQ0XG3N9myGGdc', '1d9Hj48kYMVefn-F36goqiKv6tH3_QvPrkrBaLIVfIe0', '1rQQbCo0NoA04UjAL14Bl3prTLsWtbRV7yAnpCSL7dis', '14c_9NCvPyxU2zyXXKMF0LxJiy8s9YEmCjghpS6Sva_w', '13wMtsM6ZNEUPLy8h5uUhkCzM4Gu6a5M_6v_4ZClR5L8', '1i0B1UWjN-0-2Vj7ci_Z8XdF5LzRP9XE-tRyQJRmEYyI', '1sfBw-tB5Ar3dkLxpo5gV7vkFL5mC2x-_Ro41ikqQ21o', '1xuOM6aTNepo00KJ73CG0i7B75wT58ExeSFWHXf-oNHU', '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ', '1ABzEcRnffDCFOyXS9g806sLpwkTtBlY4BFxwe1wTXJM']

if __name__ == '__main__':
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    client = Client()
    for sheet_id in update_list:
        update_raw_data(spreadsheet=sheet_id, client=client)

    # Update single:
    # SPREADSHEET_ID = '1SRSN71c6nH8wZ4XTfXm_0Ea349wAhoQLxwv491POyvQ'
    # update_raw_data(spreadsheet=SPREADSHEET_ID, client=client)
    print('Done')
