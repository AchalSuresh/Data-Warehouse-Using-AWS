import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        This function iterates over all the drop table queries in sql_queries.py and executes them.
        INPUTS:
        * cur the cursor variable of the database as defined in the main function
        * conn the connection variable of the database as defined in the main function
    """        
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        This function iterates over all the create table queries in sql_queries.py and executes them.
        INPUTS:
        * cur the cursor variable of the database as defined in the main function
        * conn the connection variable of the database as defined in the main function
    """        
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The function connects the script to the database and creates a cursor.
    It then calls the drop_tables and create_tables functions
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()