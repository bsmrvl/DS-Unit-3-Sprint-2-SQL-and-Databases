"""
Executing this module on its own will connect to a postgreSQL
database and insert the Titanic data into a new table called 
'titanic'. First create a file called creds.txt with the 
following:

host
dbname
user
password

No extra lines at the beginning or end. Put this file in the same
directory as insert_titanic.py. 

Once run, you may import elephant_connect() and query() from
the module to reconnect to the database and explore through 
simple queries. 
"""

import psycopg2
import pandas as pd 


# These three functions are used by create_titanic_table
# to turn DataFrame rows into one long string so we only
# need one INSERT statement. 
# 
# _fix_apos is necessary because the tuple() method decides 
# whether to use '' or "" depending on the presence of 
# apostrophes in given strings. This leads to the presense of 
# both '' strings and "" strings in the INSERT statement, which
# causes an error in psycopg2. Replacing apostrophes with ticks
# resolves this by ensuring all strings are contained by ''.

def _fix_apos(val):
    if isinstance(val, str) and "'" in val:
        return val.replace("'", '`')
    else:
        return val

def _row_to_string(row):
    lis = [_fix_apos(val) for val in list(row)]
    return str(tuple(lis))

def _whole_string(df):
    whole_string = ''
    for i in range(df.shape[0]):
        whole_string = whole_string + _row_to_string(df.iloc[i]) + ', '
    return whole_string[:-2]

# ---------------------------------

def elephant_connect():
    """Connects to postgreSQL database with credentials from creds.txt."""
    f = open('creds.txt', 'r')
    host = f.readline()[:-1]
    dbname = f.readline()[:-1]
    user = f.readline()[:-1]
    password = f.readline()
    f.close()

    conn = psycopg2.connect(host=host,
                            dbname=dbname,
                            user=user,
                            password=password)
    curs = conn.cursor()
    return conn, curs


def create_titanic_table(df, conn, curs):
    create_table = '''
        DROP TABLE IF EXISTS titanic;
        DROP TYPE IF EXISTS sex;
        CREATE TYPE sex AS ENUM ('male', 'female');
        CREATE TABLE titanic (
            id SERIAL PRIMARY KEY,
            survived SMALLINT,
            pclass SMALLINT,
            name VARCHAR(100),
            sex SEX,
            age REAL,
            siblings_spouses_aboard SMALLINT,
            parents_children_aboard SMALLINT,
            fare REAL
        );
        '''
    curs.execute(create_table)
    conn.commit()

    add_data = '''
        INSERT INTO titanic
        (survived, pclass, name, sex, age, siblings_spouses_aboard, parents_children_aboard, fare)
        VALUES
        ''' + _whole_string(df) + ';'
    curs.execute(add_data)
    conn.commit()


def query(curs, qry):
    """A simple query function which immediately prints nice results."""
    curs.execute(qry)
    print(pd.DataFrame(curs.fetchall()))


if __name__ == '__main__':
    df = pd.read_csv('titanic.csv')

    print('\nConnecting to Elephant...')
    conn, curs = elephant_connect()

    print('Creating table and inserting Titanic data...')
    create_titanic_table(df, conn, curs)

    print('''
Done! The Titanic dataset is now in your postgreSQL database.
To make queries from a repl:
    - from insert_titanic import elephant_connect, query
    - conn, curs = elephant_connect()
    - query(curs, "SELECT...")
    - ...or any standard connection/cursor methods
    ''')

    conn.close()