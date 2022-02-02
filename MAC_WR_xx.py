#TODO rename
# Count up the number of records added
from pathlib import Path
import os
import time
from datetime import datetime
import pandas as pd


def build_csv():
    print(f'start build_csv: {datetime.now()}')
    str_time = time.time()

    MAC_dir = Path(r'C:\Users\WRA1523\OneDrive - Emerson\Varsity\Python')
    os.chdir(MAC_dir)
    MAC_file = open("MACSensi.csv", "r")

    df = pd.read_csv(MAC_file)
    for row in df.itertuples(index=False, name='MAC'):
        try:
            if isinstance(row[0], str) and len(row[0]) <= 15 and isinstance(row[1], str) and len(row[1]) <= 15:
                sql_stmnt = "insert into ddblib.mac (MACADDRESS, SERIALNBR, BUILDDATE, TESTDATE, FILE_NAME) \
                    values('" + row[0] + "', '" + row[1] + "', '" + str(row[2]) + "', '" + str(row[3]) + "', '" + row[4] + "')"
            else:
                # print(f'MACaddress: {row[0]} and SerialNbr: {row[1]}')
                db_ctr_invalid += 1
        except pyodbc.Error as err:
            # print(f'Error: {err}')
            db_ctr_error += 1




if __name__ == "__main__":
    build_csv()
