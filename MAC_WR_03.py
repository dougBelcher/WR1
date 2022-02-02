#   This script is intended to read a directory of .csv files and output a file on the IBM i
#   Purpose is to accumulate a series of Sensi uploads from the supplier and output a MAC ID file

#   TODO - Add SQLite output to this *edit* change to csvs-to-sqlite *edit* change to output to DB2
import os
# import sys
import time
import pandas as pd
from datetime import datetime
# import sqlite3
# import sqlalchemy as sa
import pyodbc as db2
from dotenv import DotEnv


# ....................................................................
def build_db():
    try:
        os.chdir('\\Users\\WRA1523\\OneDrive - Emerson\\Varsity\\Python')
    except:
        pass
    MAC_file = open("MACSensi.csv", "r")

    db_ctr_invalid = 0
    db_ctr_error = 0

    print(f'start build_db: {datetime.now()}')
    str_time = time.time()

    dotenv = DotEnv()

    ibm_profile = (dotenv.get('ibm_profile'))
    ibm_password = (dotenv.get('ibm_password'))

    connstring = 'Driver={IBM i Access ODBC Driver};System=WRSERV;Uid=' + ibm_profile + ';Pwd=' + ibm_password + ';CommitMode=0;'
    conn = db2.connect(connstring)
    # connstring = 'ibmi://' + ibm_profile + ':' + ibm_password + '@WRSERV'
    # engine = sa.create_engine(connstring)
    # cnxn = engine.connect()
    c = conn.cursor()

    try:
        c.execute("""CREATE TABLE ddblib.MAC 
                (MACADDRESS char(15)        NOT NULL,
                SERIALNBR char(15)          NOT NULL,
                BUILDDATE char(30)          NOT NULL,
                TESTDATE char(30)           NOT NULL,
                FILE_NAME char(50)          NOT NULL
                )""")
        conn.commit()
    except:
        pass

    df = pd.read_csv(MAC_file)
    for row in df.itertuples(index=False, name='MAC'):
        try:
            if isinstance(row[0], str) and len(row[0]) <= 15 and isinstance(row[1], str) and len(row[1]) <= 15:
                sql_stmnt = "insert into ddblib.mac (MACADDRESS, SERIALNBR, BUILDDATE, TESTDATE, FILE_NAME) \
                    values('" + row[0] + "', '" + row[1] + "', '" + str(row[2]) + "', '" + str(row[3]) + "', '" + row[4] + "')"
                c.execute(sql_stmnt)
                conn.commit()
            else:
                # print(f'MACaddress: {row[0]} and SerialNbr: {row[1]}')
                db_ctr_invalid += 1
        except db2.Error as err:
            # print(f'Error: {err}')
            db_ctr_error += 1

    c.execute("""select count(*) from ddblib.MAC""")
    conn.commit()
    for row in c.fetchone():
        print(f'{row:,} records')

    print(f'done  build_db: {datetime.now()} - {(time.time() - str_time) / 60:.2f} mins')
    print(f'{db_ctr_invalid} invalids')
    print(f'{db_ctr_error} errors')

    MAC_file.close()
    c.close()
    conn.close()


if __name__ == "__main__":
    build_db()
