from utils import *


CONF_FILE_PATH = '/Users/khanhnguyen/Documents/Projects/data-management/scripts/conf.json'
scopes = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]

if __name__ == '__main__':
    try:
        connection = connect_postgres(conf_file=CONF_FILE_PATH)
        cursor = connection.cursor()
        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        # closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
