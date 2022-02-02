# Use pandas to extract data from Excel and write to SQL text file
# Steps to take prior to running script
#   1)  Name column over Serial Numbers "Serial_Numbers"
#   2)  Make sure all the separation characters are the same in the file & script
#   3)  Make sure there are no embedded blank cells in the column
#   4)  Check for one and only one sheet in the workbook

import pandas as pd
from pathlib import Path
import os

Sensi_dir = Path(r'C:\Users\WRA1523\OneDrive - Emerson\Varsity\Python')
os.chdir(Sensi_dir)

# Sensi = pd.read_excel('09144164.xlsx')
Sensi = pd.read_excel('091539541.xlsx')

# Need an SQL file to output the SQL to execute on the IBM i
file1 = open("sensi_load.sql", "w+")
file1.write('-- Load temp file with Serial Nbrs\n')
file1.write('-- Select any Serial Nbr from temp not in Full file\n')
file1.write('set schema=ddblib;\n\n')
file1.write('truncate table sensidta;\n\n')

# This is were the separation characters has to match the spreadsheet
Sensi['SN'] = Sensi.Serial_Numbers.astype("string").str.split(pat="-")
for serial in Sensi.SN:
    for nbr in serial:
        file1.write('INSERT INTO SENSIDTA (SERIALNBR) VALUES(')
        file1.write("\'" + nbr.upper()[:14] + "\'")
        file1.write(");\n")

file1.write("\nselect a.serialnbr from sensidta a exception join wrddta/f5543203 b on b.mdsn= a.serialnbr")
file1.write("\n\twhere a.serialnbr<>' '")

file1.close()
