#   This script is intended to read a directory of .csv files and output a file on the IBM i
#   Purpose is to accumulate a series of Sensi uploads from the supplier and output a MAC ID file

#   TODO - Add SQLite output to this *edit* change to csvs-to-sqlite
import os
import sys
import time
import pandas as pd
from datetime import datetime
import sqlite3
from pathlib import Path

pd.options.mode.chained_assignment = None  # default='warn'


# ....................................................................
def build_csv():
    print(f'start build_csv: {datetime.now()}')
    str_time = time.time()

    MAC_dir = Path(r'C:\Users\WRA1523\OneDrive - Emerson\Varsity\Python')
    os.chdir(MAC_dir)
    MAC_file = open("MACSensi.csv", "w+")
    # MAC_file = open("MACSensi.csv", "a+")
    hdr_parm = True
    ctr_invalids = 0
    ctr_unknown = 0
    ctr_empty = 0
    conn = sqlite3.connect('MAC.db')
    c = conn.cursor()

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
                        if hdr_parm:
                            modDF.to_csv(MAC_file, index=False, header=hdr_parm, line_terminator='\n')
                            hdr_parm = False
                        else:
                            modDF.to_csv(MAC_file, index=False, header=hdr_parm, line_terminator='\n')
                    else:
                        # print(f"Empty Data File: {file}")
                        ctr_empty += 1
                    f.close()
                except PermissionError as e:
                    # print(f"Can't access file: {file} : {e}")
                    ctr_invalids += 1
                except:
                    e = sys.exc_info()
                    print(f"Unknown error: {file}: {e}")
                    ctr_unknown += 1

    print(f'done  build_csv: {datetime.now()} - {(time.time() - str_time)/60:.2f} mins')
    print(f'{ctr_invalids} invalids')
    print(f'{ctr_unknown} unknowns')
    print(f'{ctr_empty} empties')
    MAC_file.close()
    os.chdir(MAC_dir)


def build_db():
    # Done with .csv build; start db build
    try:
        os.chdir('\\Users\\WRA1523\\OneDrive - Emerson\\Varsity\\Python')
    except:
        pass
    MAC_file = open("MACSensi.csv", "r")
    # MAC_file.seek(0)
    print(f'start build_db: {datetime.now()}')
    str_time = time.time()

    conn = sqlite3.connect('MAC.db')
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

    df = pd.read_csv(MAC_file)
    df.to_sql('MAC', conn, index=False, if_exists='replace')

    c.execute("""delete from MAC where SerialNumber like '%+%'""")
    conn.commit()

    c.execute("""select count(*) from MAC""")
    conn.commit()

    for row in c.fetchone():
        print(f'{row:,} records')

    print(f'done  build_db: {datetime.now()} - {(time.time() - str_time)/60:.2f} mins')

    MAC_file.close()
    c.close()
    conn.close()


if __name__ == "__main__":
    build_csv()
    build_db()
