import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pyodbc
from src.utils import get_src_accessdb_connection,get_src_accessdb2_connection

# Get connection
conn = get_src_accessdb2_connection()
cursor = conn.cursor()

# List all tables
tables = cursor.tables(tableType='TABLE')
print("Available tables:")
for table in tables:
    print(f"- {table.table_name}")

conn.close()