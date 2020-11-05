"""Answers a series of questions about the titanic data, using postgreSQL

First make a file 'p_creds.txt' with the following:

host
dbname
user
password

With no extra lines. Save this in the same directory as this file.
"""

import psycopg2

def creds(path):
    creds = {}
    f = open(path, 'r')
    creds['host'] = f.readline()[:-1]
    creds['dbname'] = f.readline()[:-1]
    creds['user'] = f.readline()[:-1]
    creds['password'] = f.readline()
    f.close()
    return creds

class ExplorePG:
    def __init__(self, creds):
        self.conn = psycopg2.connect(host=creds['host'],
                                     dbname=creds['dbname'],
                                     user=creds['user'],
                                     password=creds['password'])
        self.curs = self.conn.cursor()

    def question(self, q, query, lines=0):
        print(q)
        self.curs.execute(query)
        result = self.curs.fetchall()
        if lines == 0:
            print(result[0][0])
        else:
            print(result[:lines])
        print('')


if __name__ == '__main__':
    t = ExplorePG(creds('p_creds.txt'))

    print('\nTitanic Questions')
    print('---------------------------\n')

    t.question('How many passengers survived?',
               'SELECT COUNT(*) FROM titanic WHERE survived = 1')
    t.question('How many passengers died?',
               'SELECT COUNT(*) FROM titanic WHERE survived = 0')
    t.question('How many passengers survived (by class)?',
               'SELECT pclass, COUNT(*) FROM titanic WHERE survived = 1 GROUP BY pclass ORDER BY pclass',
               lines=3)
    t.question('How many passengers died (by class)?',
               'SELECT pclass, COUNT(*) FROM titanic WHERE survived = 0 GROUP BY pclass ORDER BY pclass',
               lines=3)
    t.question('What was the average age of survivors vs nonsurvivors?',
               'SELECT survived, AVG(age) FROM titanic GROUP BY survived ORDER BY survived DESC',
               lines=2)
    t.question('What was the average age of each passenger class?',
               'SELECT pclass, AVG(age) FROM titanic GROUP BY pclass ORDER BY pclass',
               lines=3)
    t.question('What was the average fare by passenger class?',
               'SELECT pclass, AVG(fare) FROM titanic GROUP BY pclass ORDER BY pclass',
               lines=3)
    t.question('What was the average fare by survival?',
               'SELECT survived, AVG(fare) FROM titanic GROUP BY survived ORDER BY survived DESC',
               lines=2)
    t.question('How many siblings/spouses aboard on average, by passenger class?',
               'SELECT pclass, AVG(siblings_spouses_aboard) FROM titanic GROUP BY pclass ORDER BY pclass',
               lines=3)
    t.question('How many siblings/spouses aboard on average, by survival?',
               'SELECT survived, AVG(siblings_spouses_aboard) FROM titanic GROUP BY survived ORDER BY survived DESC',
               lines=2)
    t.question('How many parents/children aboard on average, by passenger class?',
               'SELECT pclass, AVG(parents_children_aboard) FROM titanic GROUP BY pclass ORDER BY pclass',
               lines=3)
    t.question('How many parents/children aboard on average, by survival?',
               'SELECT survived, AVG(parents_children_aboard) FROM titanic GROUP BY survived ORDER BY survived DESC',
               lines=2)
    t.question('How many passengers have the same name as another passenger?',
               '''
               SELECT COUNT(*) FROM (
                   SELECT name, COUNT(*) AS name_count FROM titanic GROUP BY name
               ) AS name_counts 
               WHERE name_count > 1
               ''')
