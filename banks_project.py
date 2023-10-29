# Code for ETL operations on Country-GDP data

# Importing the required libraries

from bs4 import BeautifulSoup
import csv
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing/transformation. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            anchors = col[1].find_all('a')
            name = anchors[1].get_text()
            #bank_name = col[1].find_all('a')[1]['title']
            #print(f'bank name ' + bank_name)
            mc = col[2].contents[0]
            #mcflex = float(col[2].contents[0][:-1])
            #mcc = mcflex + 50.33
            #print(f'mcc ' + str(mcc))
            mc_final = mc[:-1] #Remove the last \n character
            data_dict = {"Name": name,
                        "MC_USD_Billion": mc_final}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def convert_csv_to_dict(csv_file):
    '''This function converts the exchange rate csv file to 
    a map (dictionary)'''
    # Initialize an empty list to store the dictionaries
    data_dict = {}

    # Open and read the CSV file
    with open(csv_file, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader) # skip the header row
        for row in csv_reader:
            key = row[0]
            value = row[1]
            data_dict[key] = value
        
    return data_dict

def convert_df(df, column_name, currency):
    '''This function creates a new dataframe column which is a 
    currency conversion from USD_Billion to the given currency 
    and outputs the converted values to float of 2 decimal places'''

    conversion_dict = convert_csv_to_dict('exchange_rate.csv')
    df[column_name] = df['MC_USD_Billion'] * float(conversion_dict[currency])
    df[column_name] = df[column_name].round(2)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    # Convert MS_USD_Billion to float data type
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

    df = convert_df(df, 'MC_GBP_Billion', 'GBP') 
    df = convert_df(df, 'MC_EUR_Billion', 'EUR') 
    df = convert_df(df, 'MC_IND_Billion', 'INR') 
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
table_attribs_final = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
print(df)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT Name, MC_USD_Billion from {table_name} WHERE MC_USD_Billion >= 150"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()