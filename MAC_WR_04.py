#   another MAC output
import os
import sys
import time
import pandas as pd
from datetime import datetime
import sqlite3
from pathlib import Path

def build_csv():
    print(f'start build_csv: {datetime.now()}')
    str_time = time.time()

    MAC_dir = Path(r'C:\Users\WRA1523\OneDrive - Emerson\Varsity\Python')
    os.chdir(MAC_dir)
    MAC_file = open("MACSensi.csv", "w+")
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
            # sql_stmnt = "select * from MAC where '" + file + "'= '" + MAC[4] + "';'"
            sql_stmnt = """select * from MAC limit 10;"""
            print(f'{sql_stmnt}')
            # sys.exit()
            c.execute(sql_stmnt)
            rows = c.fetchall()
            for row in rows:
                print(row)
            sys.exit()


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
                    # print(f"Unknown error: {file}: {e}")
                    ctr_unknown += 1

    print(f'done  build_csv: {datetime.now()} - {(time.time() - str_time)/60:.2f} mins')
    print(f'{ctr_invalids} invalids')
    print(f'{ctr_unknown} unknowns')
    print(f'{ctr_empty} empties')
    MAC_file.close()
    os.chdir(MAC_dir)

if __name__ == "__main__":
    build_csv()