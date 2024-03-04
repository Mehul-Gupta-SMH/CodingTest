import psycopg2
import pandas as pd
import yaml
import json
import datetime
from multiprocessing import Process, freeze_support

import warnings
warnings.filterwarnings("ignore")

# Import custom modules
from TextCleaning import process_text, process_date#, remove_irrelevant_characters, rectify_misspelling



# Load configuration from YAML file
with open("config.YAML","r") as conf_fobj:
    conf_dict = yaml.full_load(conf_fobj)
    conf_dict = conf_dict['json']

# Extract necessary configurations from the loaded dictionary
batch_size = conf_dict['batch_size']
conn_string = f"host='{conf_dict['connection_str']['host']}' dbname='{conf_dict['connection_str']['dbname']}' user='{conf_dict['connection_str']['username']}' password='{conf_dict['connection_str']['password']}'"
destination_table = conf_dict['destination_table']
max_concurrent_jobs = conf_dict['max_concurrent_jobs']

# Dictionary specifying operations to be performed on different data types
ops_idtfr = {
    "Text" : {
        "reviewText" : "character_cleaning",
        "summary" : "character_cleaning",
        "reviewerName" : "capping"
    },

    "Mapping" : {

    },

    "Integer" : {
        "vote" : "0",
        "overall" : "0"

    },

    "Float": {

    },

    "Date": {
        "reviewTime" : "%m %d, %Y",
    },

    "Flatten": {
        "style" : "text",
        "unixReviewTime" : "text"
    }

}

def process_batch(reviews_batch_df: pd.DataFrame, batch_num: int):

    col_names = reviews_batch_df.columns

    # Establish connection to PostgreSQL database
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()

    print(f"Processing Batch {batch_num} at {datetime.datetime.now()}")

    # Apply operations according to data type
    for opsid,ops in ops_idtfr.items():

        if not len(ops):
            continue

        if opsid == "Text":
            for col, type in ops.items():
                reviews_batch_df[col] = reviews_batch_df[col].map(lambda x: process_text(str(x),type))

        elif opsid == "Integer":
            for col, type in ops.items():
                try:
                    reviews_batch_df[col] = pd.to_numeric(reviews_batch_df[col], errors='coerce')
                    reviews_batch_df.fillna({col:-9999}, inplace=True)
                except:
                    continue

        elif opsid == "Float":
            pass

        elif opsid == "Mapping":
            pass

        elif opsid == "Date":
            for col, type in ops.items():
                try:
                    reviews_batch_df[col] = reviews_batch_df[col].map(lambda x: process_date(x,type))
                    reviews_batch_df[col] = reviews_batch_df[col].dt.date
                except:
                    continue

        elif opsid == "Flatten":
            for col, type in ops.items():
                try:
                    if type == "text":
                        reviews_batch_df[col] = reviews_batch_df[col].map(lambda x: str(x))
                except:
                    continue


    # reviews_batch_df.to_csv(file_path.replace(".json","_Processed.csv"), index=False)

    # Convert DataFrame to list of tuples for insertion
    reviews_batch_df = reviews_batch_df.to_records(index=False).tolist()

    # Generate and execute insertion query
    insert_query = f"""INSERT INTO {destination_table} ({', '.join(col_names)}) VALUES ({", ".join(["%s"]*len(col_names))})"""
    cursor.executemany(insert_query, reviews_batch_df)

    print(f"Completed Batch {batch_num} at {datetime.datetime.now()}")

    # Commit changes and close connection
    conn.commit()
    conn.close()



def process_custreviews(file_path: str):

    batch_num = 0

    with open(file_path,"r") as raw_data_fobj:
        while True:
            processes = []

            for jid in range(max_concurrent_jobs):
                batch_num+= 1
                reviews_batch = []

                for _ in range(batch_size):
                    try:
                        reviews_batch.append(json.loads(raw_data_fobj.readline()))
                    except:
                        break


                reviews_batch_df = pd.DataFrame.from_dict(reviews_batch)

                process = Process(target=process_batch, args=(reviews_batch_df, batch_num))
                processes.append(process)
                process.start()

            for process in processes:
                process.join()

            if not reviews_batch:
                break


if __name__ == '__main__':
    freeze_support()  # Only required for Windows when using multiprocessing
    process_custreviews(r"C:\Users\mehul\Downloads\BlackRock - Data Assignment\Electronics_5.json")