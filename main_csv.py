import sqlalchemy
from sqlalchemy import text
import pandas as pd
import time
import sys

start_time = time.time()

data = pd.read_csv(sys.argv[1], index_col=False, delimiter=",", lineterminator="\n")

#connection_string = "postgresql+psycopg2://postgres:reader@4.tcp.ngrok.io:10411"
connection_string = "postgresql+psycopg2://postgres:reader@localhost"
dbEngine = sqlalchemy.create_engine(connection_string, connect_args={'connect_timeout': 10}, echo=False)
conn = dbEngine.connect()

conn.execute(text("commit"))
conn.execute(text(f"CREATE DATABASE {sys.argv[2]}"))
conn.close()

#connection_string_2 = f"postgresql+psycopg2://postgres:reader@4.tcp.ngrok.io:10411/{sys.argv[2]}"
connection_string_2 = f"postgresql+psycopg2://postgres:reader@localhost/{sys.argv[2]}"
dbEngine_2 = sqlalchemy.create_engine(connection_string_2, connect_args={'connect_timeout': 10}, echo=False)
conn_2 = dbEngine_2.connect()

conn_2.execute(text("commit"))
conn_2.execute(text(f"CREATE SCHEMA {sys.argv[3]}"))
conn_2.close()

data.to_sql(con=dbEngine_2, schema=f"{sys.argv[3]}", name=f"{sys.argv[4]}", if_exists="replace", index=False)

print("--- %s seconds ---" % (time.time() - start_time))

#python .\main_csv.py "./hashtag_donaldtrump.csv" "database_name" "schema_name" "table_name"