import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import os
import pandas as pd
import time

start_time = time.time()

conn = pymysql.connect(user='debian-sys-maint', passwd='SoLcxvfgbzqU0ixI', host='10.6.66.160', database='drive_indexing')
cursor = conn.cursor()

engine = create_engine('mysql+pymysql://debian-sys-maint:SoLcxvfgbzqU0ixI@10.6.66.160/drive_indexing?charset=utf8')

paths = 'I:\\1. Projects'

query_drop_table = ("""DROP TABLE IF EXISTS I_dir_name""")
cursor.execute (query_drop_table)

query_create_dir_name = """CREATE TABLE I_dir_name(_id int(10) primary key AUTO_INCREMENT, 
                Client varchar (155))"""
cursor.execute(query_create_dir_name)

dirlist = os.listdir(paths)
counter = 0

for i in dirlist:
    if counter == 1:
        break
    else:
        dir_list = list(zip(dirlist))
        dir_name_query = ('INSERT INTO I_dir_name(Client) VALUES(%s)')
        cursor.executemany(dir_name_query, dir_list)
        counter +=1
conn.commit()


query_drop_I_drive = ("""DROP TABLE IF EXISTS I_drive""")
cursor.execute(query_drop_I_drive)

data = list()
for root, dirs, files in os.walk(paths):
    try:
        for filename in files:
            nm, ext = os.path.splitext(filename)
            # ctimes = time.ctime(os.stat(paths).st_ctime)
            # mtimes = time.ctime(os.stat(paths).st_mtime)
            fullpath = os.path.join(os.path.abspath(root))
            data.append((filename, fullpath))
            # print(data)
    except OSError:
        pass

df = pd.DataFrame(data, columns=['Filename', 'Fullpath'])
df.to_sql(con=engine, name='I_drive', if_exists='replace', index=True, index_label='id')

// dtype=sqlalchemy.types.NVARCHAR(length=255) converts columns to 'utf8_general_ci' if run into 
// '1366, "Incorrect string value: '\\xE1\\xBC\\x80\\xCE\\' issues (3 bits v 4 bits)
#df.to_sql(con=engine, name='I_drive', if_exists='replace', index=True, index_label='id', dtype=sqlalchemy.types.NVARCHAR(length=255))

print("--- %s seconds ---" % (time.time() - start_time))
