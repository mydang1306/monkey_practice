import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

def get_redshift_connection(db_name=None):
    load_dotenv()  # take environment variables from .env.
    """
    This function establishes a connection to a Redshift database.
    
    Parameters:
    db_name (str, optional): The name of the database to connect to. If not provided, the default database name from the environment variables will be used.
    
    Returns:
    psycopg2.extensions.connection: A connection object to the Redshift database.
    """
    conn = psycopg2.connect(
        host=os.environ['REDSHIFT_HOST'],
        dbname=db_name if db_name else os.environ['REDSHIFT_DBNAME'], 
        user=os.environ['REDSHIFT_USER'],
        password=os.environ['REDSHIFT_PASSWORD'],
        port=os.environ['REDSHIFT_PORT']
    )        
    return conn

def execute_select_query(query: str, db=None) -> pd.DataFrame:
    """
    This function executes a SQL query on a Redshift database and returns a pandas DataFrame.

    Parameters:
    query (str): The SQL query to be executed.

    Returns:
    pd.DataFrame: A pandas DataFrame containing the results of the executed query.
    """
    redshift = get_redshift_connection(db)
    dataframe = pd.read_sql(query, redshift)
    redshift.close()
    return dataframe

def execute_query(query, db=None):
    redshift = get_redshift_connection(db)
    try:
        cursor = redshift.cursor()
        cursor.execute(query)
        redshift.commit()
        redshift.close()
    except Exception as e:
        redshift.rollback()
        redshift.close()
        raise e
