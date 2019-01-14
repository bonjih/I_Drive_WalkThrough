from sqlalchemy import create_engine
import mysql.connector
import os
import pandas as pd
import time

start_time = time.time()

conn = mysql.connector.connect(host='xxx', database='drive_indexing', user='xxxx', password = 'xxxx')
cursor = conn.cursor()

engine = create_engine('mysql+mysqldb://xxxx:xxxxx@xxxxx/drive_indexing?charset=utf8', encoding = 'utf-8')

paths = 'I:\\1. Projects'

query_drop_table = ("""DROP TABLE IF EXISTS I_dir_name""")
cursor.execute (query_drop_table)
conn.commit()

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
conn.close()

data = list()
for root, dirs, files in os.walk(paths):
    try:
        for filename in files:
            nm, ext = os.path.splitext(filename)
            ctimes = time.ctime(os.stat(paths).st_ctime)
            mtimes = time.ctime(os.stat(paths).st_mtime)
            fullpath = os.path.join(os.path.abspath(root), filename)
            data.append((filename, fullpath, ctimes, mtimes))
            # print(data)
    except OSError:
        pass

df = pd.DataFrame(data, columns=['Filename', 'Fullpath', 'Created', 'Modified'])
df.to_sql(con=engine, name='I_drive', if_exists='replace', index=True, index_label='id')

// dtype=sqlalchemy.types.NVARCHAR(length=255) converts columns to 'utf8_general_ci' if run into 
// '1366, "Incorrect string value: '\\xE1\\xBC\\x80\\xCE\\' issues (3 bits v 4 bits)
#df.to_sql(con=engine, name='I_drive', if_exists='replace', index=True, index_label='id', dtype=sqlalchemy.types.NVARCHAR(length=255))

print("--- %s seconds ---" % (time.time() - start_time))
