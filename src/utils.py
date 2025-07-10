import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pymysql
import pyodbc
import pandas as pd
from datetime import date
from src.config import config

def get_tgt_myconnection():
    try:
        connection = pymysql.connect( 
            host= config['target_mysql']['host'], 
            user= config['target_mysql']['user'], 
            db= config['target_mysql']['db'],
            password= config['target_mysql']['password'])
        #print('Connection to target successful')
        return connection
    except Exception as e:
        print(f"Error: {e}")
        return None

def get_src_accessdb_connection():
    try:
        connection = pyodbc.connect(
            f"DRIVER={config['source_accessdb']['driver']};"
            f"DBQ={config['source_accessdb']['dbq']};"
            f"PWD={config['source_accessdb']['pwd']}"
        )
        #print('Connection to source successful')
        return connection
    except Exception as e:
        print(f"Error: {e}")
        return None

def get_src_accessdb2_connection():
    try:
        connection = pyodbc.connect(
            f"DRIVER={config['source_accessdb2']['driver']};"
            f"DBQ={config['source_accessdb2']['dbq']};"
            f"PWD={config['source_accessdb2']['pwd']}"
        )
        #print('Connection to source successful')
        return connection
    except Exception as e:
        print(f"Error: {e}")
        return None
    
def safe_value(value):
    if value is None or pd.isnull(value):
        return 'NULL'
    elif isinstance(value,str):
        if '"' in value:
            return '"' + value.replace('"',"'") + '"'
        elif "\\" in value:
            return '"' + value.replace("\\","\\\\") + '"'
        else:
            return '"' + value + '"'
    elif isinstance(value,date):
        return '"' + value.strftime('%Y-%m-%d') + '"'
    else:
        return value

if __name__ == "__main__":
    connection1 = get_src_accessdb_connection()
    if connection1:
        connection1.close()
    else:
        pass
    connection2 = get_tgt_myconnection()
    if connection2:
        connection2.close()
    else:
       pass

def getSourceFilePath():
    return config['source_file_path']['source_file_path']

def getTargetFilePath():
    return config['target_file_path']['target_file_path']

def getLogFilePath():
    return config['log_directory']['log_directory']