import pandas as pd
import psycopg2
import os
from time import sleep
from sqlalchemy import create_engine


# data = pd.read_csv("Genre.csv")
# df = data.to_csv(index=False)
# print(df)

host = os.environ.get("host")
database = os.environ.get("database")
user = os.environ.get("user")
password = os.environ.get("password")
conn = psycopg2.connect(
    host = host, database = "kafkastreaming", user = user, password = password
    )
cursor = conn.cursor()

db = "kafkastreaming"
port = "5432"

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

df = pd.read_csv("Genre.csv")
# print(df)
df.head()

for index, row in df.head().iterrows():
    mod = pd.DataFrame(row.to_frame().T)
    mod.to_sql(f"public.genre", engine, if_exists='append', index=False)
    print("Row inserted: ")
    conn.commit()
    sleep(0.2)

# sql = '''CREATE TABLE IF NOT EXISTS genre(genreid text NOT NULL, 
# songname char(50));'''
  
  
# cursor.execute(sql) 


# sql2 = '''COPY genre(genreid,songname) FROM 'F:\PostgreSQL-to-Snowflake-Streaming\etl-postgresqltosnowflake\Genre.csv' 
#     DELIMITER ',' 
#     CSV HEADER;'''
# cursor.execute(sql2)
# conn.commit()

# sql3 = '''select * from genre;'''
# cursor.execute(sql3) 
# for i in cursor.fetchall(): 
#     print(i) 