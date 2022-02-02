#   This script is intended to read a directory of .csv files and output a file on the IBM i
#   Purpose is to accumulate a series of Sensi uploads from the supplier and output a MAC ID file

#   TODO - Add SQLite output to this
import os
import sys
import time
import pandas as pd
from datetime import datetime
import sqlite3

pd.options.mode.chained_assignment = None  # default='warn'

print(f'start: {datetime.now()}')
str_time = time.time()

os.chdir('\\Users\\WRA1523\\OneDrive - Emerson\\Varsity\\Python')
MAC_file = open("MACSensi.csv", "w+")

conn = sqlite3.connect('MAC_data.db')
c = conn.cursor()

try:
    c.execute("""CREATE TABLE MAC (
            MACaddress text,
            SerialNumber text,
            BuildDate text,
            TestDate text,
            file_name text
            )""")
    conn.commit()
except:
    pass

# Delete Records
c.execute("""DELETE FROM MAC""")
conn.commit()

os.chdir('//wrprod/wrdsup/supdata')
directory = os.path.join('//wrprod/wrdsup/supdata')
for root, dirs, files in os.walk(directory):
    for file in files:
        if file.endswith(".csv"):
            try:
                f = open(file, 'r')
                df = pd.read_csv(file)
                modDF = df.iloc[:, [0, 1, 2, 3]]
                if modDF.size > 0:
                    modDF.dropna(axis=0, how='all', inplace=True)
                    modDF["file_name"] = file
                    modDF.to_sql('MAC', conn, index=False, if_exists='append')
                else:
                    print(f"Empty Data File: {file} - {list(df.columns)}")
                f.close()
            except PermissionError as e:
                print(f"Can't access file: {file} : {e}")
            except:
                e = sys.exc_info()
                print(f"Unknown error: {file}: {e}")

print(f'done: {datetime.now()} - {(time.time() - str_time)/60}')
MAC_file.close()
c.close()
