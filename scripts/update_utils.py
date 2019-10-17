from pymongo import MongoClient
import json
import pandas as pd


def authorize():
    '''Load authorization'''

    pass


def update_db(df, keyword, db_field='_meta_sheetID', collection):
    '''
    Assuming we're to lookup the db to replace by a 'keyword' assigned in 'db_field'.
    This value is assigned when importing the dataframe into DB.
    Currently, it works in a way that drop the rows with the matching 'keyword' in 'db_field' and replace those with the corresponding df.

    Params:
        df: `DataFrame` Pandas dataframe for import.
        keyword: `str` Keyword to search.
        db_field: `str` Field name in the collection conducting the search. Default to '_meta_sheetID'. 
            Currently possible values: '_meta_sheetID', '_meta_projectName'. (Refer to 'get_data_to_mongodb_v2' to update)
        collection: `str` Connected collection using PyMongo.

    Returns:
        None
    '''
    pass
