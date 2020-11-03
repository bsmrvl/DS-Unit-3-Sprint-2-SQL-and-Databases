import pandas as pd 
from rpg_queries import BetterSqlite

if __name__ == '__main__':
    df = pd.read_csv('buddymove_holidayiq.csv')
    
    inte = BetterSqlite('buddymove_holidayiq.sqlite3')
    inte.query('DROP TABLE IF EXISTS review')

    df.to_sql('review', inte.con)

    print('\nBuddyMove Summary')
    print('------------------\n')

    inte.result('Total count',
                'SELECT COUNT(*) FROM review',
                rows=0)

    inte.result('Count with 100+ nature and 100+ shopping',
                'SELECT COUNT(*) FROM review WHERE Nature > 100 AND Shopping > 100',
                rows=0)

    inte.result('Review averages',
                'SELECT AVG(Sports), AVG(Religious), AVG(Nature), AVG(Theatre), AVG(Shopping), AVG(Picnic) FROM review',
                rows=1,
                col_names=['Sports', 'Religious', 'Nature', 'Theatre', 'Shopping', 'Picnic'])

    print('------------------\n')