import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
    "This code load the data from the log files into the staging tables"
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        "This code take the data from the staging into the tables that create the start scheme"
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    "This code reads from the config file that allows the log files into the redshift"
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
