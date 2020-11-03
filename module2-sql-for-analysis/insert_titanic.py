import psycopg2
import pandas as pd 

f = open('../words.txt', 'r')
host = f.readline()[:-1]
dbname = f.readline()[:-1]
user = f.readline()[:-1]
password = f.readline()[:-1]
f.close()

g_conn = psycopg2.connect(host=host,
                          dbname=dbname,
                          user=user,
                          password=password)
g_curs = g_conn.cursor()


df = pd.read_csv('titanic.csv')
print(df.head())