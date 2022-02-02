#   This script is intended to read a directory of .csv files and output a file on the IBM i
#   Purpose is to accumulate a series of Sensi uploads from the supplier and output a MAC ID file

#   TODO - Add SQLite output to this *edit* change to csvs-to-sqlite *edit* change to output to DB2
import os
import sys
import time
import pandas as pd
from datetime import datetime
# import sqlite3
import pyodbc as db2
from dotenv import DotEnv

pd.options.mode.chained_assignment = None  # default='warn'

# ....................................................................
def build_csv():
    print(f'start build_csv: {datetime.now()}')
    str_time = time.time()

    os.chdir('\\Users\\WRA1523\\OneDrive - Emerson\\Varsity\\Python')
    MAC_file = open("MACSensi.csv", "w+")
    hdr_parm = True
    ctr_invalids = 0
    ctr_unknown = 0
    ctr_empty = 0

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
                    # print(f"Unknown error: {file}: {e}")
                    ctr_unknown += 1

    print(f'done  build_csv: {datetime.now()} - {(time.time() - str_time)/60:.2f} mins')
    print(f'{ctr_invalids} invalids')
    print(f'{ctr_unknown} unknowns')
    print(f'{ctr_empty} empties')
    MAC_file.close()


# ....................................................................
def build_db():
    # Done with .csv build; start db build
    try:
        os.chdir('\\Users\\WRA1523\\OneDrive - Emerson\\Varsity\\Python')
    except:
        pass
    MAC_file = open("MACSensi.csv", "r")

    print(f'start build_db: {datetime.now()}')
    str_time = time.time()

    dotenv = DotEnv()

    ibm_profile = (dotenv.get('ibm_profile'))
    ibm_password = (dotenv.get('ibm_password'))

    connstring = 'Driver={IBM i Access ODBC Driver};System=WRSERV;Uid=' + ibm_profile + ';Pwd=' + ibm_password + ';CommitMode=0;'
    conn = db2.connect(connstring)
    c = conn.cursor()

    # with conn:
    try:
        c.execute("""CREATE TABLE ddblib.MAC (
                MACADDRESS char(14),
                SERIALNUMBER char(14),
                BUILDDATE char(14),
                TESTDATE char(14),
                FILE_NAME char(50)
                )""")
        conn.commit()
    except:
        pass

    # sys.exit()

# TODO need to output df to DB2
    df = pd.read_csv(MAC_file)

    print(f'{df.head()}')
    for row in df:
        print(f'{row}')

    # c.execute('''
    #         insert into ddblib.MAC values('123456789', '123456789', '123456789', '123456789', 'file.csv')
    #         ''')
    # conn.commit

    # for row in df:
        # sql_state = "insert into ddblib.MAC values + MACAddress + ', ' + SerialNumber + ', ' + BUILDDATE + ', ' + TESTDATE + ', ' FILE_NAME + ')'"
        # print(f'SerialNumber: {df[SerialNumber]}; BuildDate: {BuildDate}; TestDate: {TestDate}; File_Name: {File_Name}')
        # print(f'{sql_state}')
        # c.execute('''
        # insert into ddblib.MAC values('123456789', '123456789', '123456789', '123456789', 'file.csv')
        # ''')
        # conn.comit

    # df.to_sql('ddblib.MAC', conn, if_exists='replace', index=False)

    # c.execute("""delete from MAC where SerialNumber like '%+%'""")
    # conn.commit()

    c.execute("""select count(*) from ddblib.MAC""")
    conn.commit()
    for row in c.fetchone():
        print(f'{row:,} records')

    print(f'done  build_db: {datetime.now()} - {(time.time() - str_time)/60:.2f} mins')

    MAC_file.close()
    c.close()
    conn.close()


if __name__ == "__main__":
    # build_csv()
    build_db()
