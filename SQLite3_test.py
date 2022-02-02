# SQLite Databases with Python

import sqlite3
from pathlib import Path
import os
import time
from datetime import datetime


def main():
    print(f'start test: {datetime.now()}')
    str_time = time.time()

    MAC_dir = Path(r'C:\Users\WRA1523\OneDrive - Emerson\Varsity\Python')
    os.chdir(MAC_dir)

    conn = sqlite3.connect('MAC.db')

    # conn = sqlite3.connect(':memory:')  don't save the db

    # Create a cursor
    c = conn.cursor()

    # Execute a select
    # c.execute("""select * from MAC where SerialNumber like '710403421039%' order by SerialNumber;""")
    # c.execute("""SELECT sql FROM sqlite_master WHERE tbl_name = 'MAC' AND type = 'table'""")
    c.execute("select count(*) from MAC a;")
    # c.execute("select count(*) 'Tot' from MAC a;").fetchone
    item = c.fetchall()
    for items in item:
        print(f'{items[0]}')
    conn.commit()

    c.execute("select macaddress, file_name from MAC a where upper(file_name) like '%8156MPATE%'")
    item = c.fetchall()
    for count, items in enumerate(item):
        print(f'index: {count} {items[0]} and {items[1]} ')

    print(f'done test: {datetime.now()} - {(time.time() - str_time) / 60:.2f} mins')

    # Close our connection
    conn.close()


if __name__ == "__main__":
    main()
