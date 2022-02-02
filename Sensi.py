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

Sensi = pd.read_excel('09144164.xlsx')
# Sensi = pd.read_excel('091539541.xlsx')

# Open the SQL text file
file1 = open("Sensi.sql", "w+")
file2 = open("Exist Sensi.txt", "w+")
a = 0

# Start writing SQL statements to file
file1.write('Select mdsn as "Serial Nbr"'
            ', mduser as "User", mdpid as "Pgm ID", mdjobn as "Job Nbr"'
            ', mdpsn as "Pick", mditm as "Shrt Item", mdlitm as "Long Item"\n\t'
            ', date(char(1900000+mdupmj)) as "Date Upd"'
            ',TIME(SUBSTR(RIGHT(\'0\' || mdtday, 6), 1, 2) || \':\' || SUBSTR(RIGHT(mdtday, 4), 1, 2) || \':\' ||'
            'RIGHT(mdtday, 2)) AS "Upd Time"\n\tfrom wrddta/f5543203 a'
            '\n\twhere mdsn in (')
#             df    col            change type          split on dash (-)
Sensi['SN'] = Sensi.Serial_Numbers.astype("string").str.split(pat="-")
for serial in Sensi.SN:
    if a == 0:
        a = 1
    else:
        file1.write(", ")
    for nbr in serial:
        file1.write("\'" + nbr.upper() + "\'")
        file1.write(", ")
        file2.write(nbr.upper() + "\n")
    a = 0
    file1.write("\n\t")

file1.write("\'\')")

file1.write("\n\tand not exists(select 1 from wrddta/f5842007 b where a.mdsn=b.assn)")
file1.write("\n\tand mdsn<>' '")

file1.close()
file2.close()
