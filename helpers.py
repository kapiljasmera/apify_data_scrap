from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json
from dotenv import load_dotenv
from os import getenv

load_dotenv('.env')

db_user = getenv('db_user')
db_password = getenv('db_password')
db_host = getenv('db_host')
db_database = getenv('db_database')

# Create a database connection
db_connection_str = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_database}'
db_connection = create_engine(db_connection_str)

Session = sessionmaker(db_connection)
sessionDB= Session()


def convert_lists_to_json(column_value):
    return json.dumps(column_value) if isinstance(column_value, list) else column_value


def data_to_sql(dataFrame, db_table):
    # Insert the DataFrame into the MySQL table
    dataFrame.to_sql(db_table, con=db_connection, if_exists='replace', index=False)
    db_connection.dispose()
    print(f"Data loaded into MySQL successfully!")

def get_url_from_sql(db_table):

    execute_query = text(f"SELECT webUrl FROM {db_table};")

    url = sessionDB.execute(execute_query)
    return url.fetchall()