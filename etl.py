import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        This function iterates over all the copy tables queries in sql_queries.py and executes them.
        It loads the data(using COPY) from S3 bucket to the Staging tables
        INPUTS:
        * cur the cursor variable of the database as defined in the main function
        * conn the connection variable of the database as defined in the main function
    """    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        This function iterates over all the insert table queries in sql_queries.py and executes them.
        The data is loaded from the staging tables to the fact & dimension table in Redshift cluster
        INPUTS:
        * cur the cursor variable of the database as defined in the main function
        * conn the connection variable of the database as defined in the main function
    """    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The function connects the script to the database and creates a cursor.
    It then calls the load_staging_tables and insert_tables functions
    """        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()