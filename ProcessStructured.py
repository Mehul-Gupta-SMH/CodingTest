import psycopg2
import pandas as pd
import yaml

# Import custom modules
from TextCleaning import process_text
from Standardisation import process_category
from MailReport import generate_report


# Load configuration from YAML file
with open("config.YAML","r") as conf_fobj:
    conf_dict = yaml.full_load(conf_fobj)
    conf_dict = conf_dict['structured']

# Extract necessary configurations from the loaded dictionary
batch_size = conf_dict['batch_size']
conn_string = f"host='{conf_dict['connection_str']['host']}' dbname='{conf_dict['connection_str']['dbname']}' user='{conf_dict['connection_str']['username']}' password='{conf_dict['connection_str']['password']}'"
destination_table = conf_dict['destination_table']

# Dictionary specifying operations to be performed on different data types
ops_idtfr = {
    "Text" : {

    },

    "Mapping" : {
        "license" : "clean_license"
    },

    "Integer" : {

    },

    "Float": {

    }
}

def process_sqltable(table_name: str):
    """
    Data ingestion for the table. After data ingestion is completed, send over the results to prelisted stakeholders.
    :param table_name: Name of the table that needs to be ingested
    :return: Status Code (after Data Ingestion)
    """
    batch_cnt = 0

    # Establish connection to PostgreSQL database
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor_colnm = conn.cursor()

    # Query to fetch column names of the specified table
    column_sql = f"""
    SELECT column_name 
    FROM information_schema.columns
    WHERE table_catalog = 'postgres'
       AND table_schema = 'public'
       AND table_name   = '{table_name}';
    """

    # Execute query to fetch column names
    cursor_colnm.execute(column_sql)
    col_names = cursor_colnm.fetchall()
    col_names = [col[0] for col in col_names if col[0] != "id"]

    # Create a cursor for executing queries
    cursor = conn.cursor()
    sql = f'''Select {', '.join(col_names)} from postgres.public.{table_name};'''
    cursor.execute(sql)

    # Process data in batches
    while True:
        try:
            records = cursor.fetchmany(size=batch_size)
        except:
            print("Processing Completed")
            break

        batch_cnt += 1
        print(f"Processing Batch : {batch_cnt}")

        inter_df = pd.DataFrame(data=records , columns=col_names)

        # Apply operations according to data type
        for opsid,ops in ops_idtfr.items():

            if opsid == "Text":
                pass

            elif opsid == "Integer":
                pass

            elif opsid == "Float":
                pass

            elif opsid == "Mapping":
                for col, type in ops.items():
                    inter_df[col] = inter_df[col].map(lambda x: process_category(x,type))



        # Convert DataFrame to list of tuples for insertion
        inter_df = inter_df.to_records(index=False).tolist()

        # Generate and execute insertion query
        insert_query = f"""INSERT INTO {destination_table} ({', '.join(col_names)}) VALUES ({", ".join(["%s"]*len(col_names))})"""
        cursor.executemany(insert_query, inter_df)

    # Get reporting metrics for mailing
    query_dict = {
        "github_repo_url with the highest number of snippets in the 'Python' language":
            f"""
            SELECT github_repo_url, COUNT(*) AS snips_cnt
            FROM {destination_table}
            WHERE language = 'Python'
            GROUP BY github_repo_url
            ORDER BY snips_cnt DESC
            LIMIT 1
            """,

        "snippets with the same commit_hash but different github_repo_url":
            f"""
            SELECT COUNT(*) AS snips_cnt
            FROM {destination_table}
            WHERE commit_hash IN
                (SELECT commit_hash
                FROM snippets_dwh
                GROUP BY commit_hash 
                HAVING COUNT(DISTINCT github_repo_url) > 1)
            """,

        "language with the highest average chunk_size for snippets with a license starting with 'MIT'":
            f"""
            SELECT "language", AVG(chunk_size) AS avg_cs
            FROM {destination_table}
            WHERE license LIKE 'MIT%'
            GROUP BY "language"
            ORDER BY avg_cs DESC
            LIMIT 1
            """
    }

    # Dictionary to store query results
    query_res = {}

    # Cursor for executing SQL queries
    cursor_reporting = conn.cursor()

    # Execute each query and store the results
    for m_def, sql in query_dict.items():
        cursor_reporting.execute(sql)
        metric_res = cursor_reporting.fetchall()
        query_res[m_def] = metric_res[0][0]

    generate_report(query_res)

    # Commit changes and close connection
    conn.commit()
    conn.close()

# Invoke function to process the specified source table
process_sqltable(conf_dict['source_table'])
