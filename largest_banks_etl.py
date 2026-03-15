# Code for ETL operations on largest banks data

# Importing the required libraries
import requests
import pandas as pd
import numpy as np
from datetime import datetime
import sqlite3
from bs4 import BeautifulSoup

# Known values
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
table_name = "Largest_banks"
db_name = "Banks.db"
csv_path = "./Largest_banks_data.csv"
exchange_rate_csv = "exchange_rate.csv"

# Functions
def log_progress(message):
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table = data.find("table", {"class": "wikitable"})
    rows = table.find_all("tr")

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            links = col[1].find_all("a")
            bank_name = links[1].text
            market_cap = float(col[2].contents[0].strip())

            data_dict = {"Name": bank_name, "MC_USD_Billion": market_cap}
            
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df


def transform(df, exchange_rate_csv):
    exchange_df = pd.read_csv(exchange_rate_csv)
    dict_exchange_rate = exchange_df.set_index("Currency").to_dict()["Rate"]

    gbp_rate = dict_exchange_rate['GBP']
    eur_rate = dict_exchange_rate['EUR']
    inr_rate = dict_exchange_rate['INR']
    
    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * gbp_rate).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * eur_rate).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * inr_rate).round(2)
    
    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



# ETL pipeline execution
log_progress("Preliminaries complete. Initiating ETL process.")
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process.")

df = transform(df, exchange_rate_csv)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, csv_path)
log_progress("Data saved to CSV file")

sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated.")
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries.")


# Example SQL queries
# Query 1
query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)


# Query 2
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)


# Query 3
query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress("Process Complete.")

sql_connection.close()
log_progress("Server Connection closed.")
