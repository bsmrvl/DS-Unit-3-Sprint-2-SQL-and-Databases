import sqlite3
import pandas as pd

class BetterSqlite:
    def __init__(self, path):
        self.con = sqlite3.connect(path)
        self.cur = self.con.cursor()

    def query(self, qry, col_names=None):
        return pd.DataFrame(self.cur.execute(qry).fetchall(),
                            columns=col_names)

    def result(self, question, qry, rows, col_names=None):
        result = self.query(qry, col_names)
        print(question)
        if rows == 0:
            print(result.iloc[0,0])
        else:
            print(result.head(rows))
        print('')


if __name__ == '__main__':
    inte = BetterSqlite('rpg_db.sqlite3')

    print('\nRPG Summary')
    print('------------------\n')

    inte.result('How many total Characters are there?',
                'SELECT COUNT(*) FROM charactercreator_character',
                rows=0)

    print('How many of each specific subclass?')
    inte.result('Mage | Necromancer:',
                'SELECT COUNT(*) FROM charactercreator_necromancer',
                rows=0)
    inte.result('Mage | NOT Necromancer:',
                '''
                SELECT COUNT(*) FROM charactercreator_mage as mg
                LEFT JOIN charactercreator_necromancer as nc
                ON mg.character_ptr_id = nc.mage_ptr_id
                WHERE nc.mage_ptr_id IS NULL
                ''',
                rows=0) 
    inte.result('Thief:',
                'SELECT COUNT(*) FROM charactercreator_thief',
                rows=0)
    inte.result('Cleric:',
                'SELECT COUNT(*) FROM charactercreator_cleric',
                rows=0)
    inte.result('Fighter:',
                'SELECT COUNT(*) FROM charactercreator_fighter',
                rows=0)

    inte.result('How many total Items?',
                'SELECT COUNT(*) FROM armory_item',
                rows=0)
    inte.result('How many of the Items are weapons?',
                'SELECT COUNT(*) FROM armory_weapon',
                rows=0)
    inte.result('How many of the Items are NOT weapons?',
                '''
                SELECT COUNT(*) FROM armory_item as it
                LEFT JOIN armory_weapon as wp
                ON it.item_id = wp.item_ptr_id
                WHERE wp.item_ptr_id IS NULL
                ''',
                rows=0)

    inte.result('How many Items does each character have? (first 20 rows)',
                '''
                SELECT name, COUNT(inv.character_id) FROM charactercreator_character as ch
                LEFT JOIN charactercreator_character_inventory as inv
                ON ch.character_id = inv.character_id
                GROUP BY ch.character_id
                ''',
                rows=20)

    inte.result('How many Weapons does each character have? (first 20 rows)',
                '''
                SELECT name, COUNT(inv.character_id) FROM charactercreator_character as ch
                LEFT JOIN (charactercreator_character_inventory as inv
                INNER JOIN armory_weapon as wp
                ON inv.item_id = wp.item_ptr_id)
                ON ch.character_id = inv.character_id
                GROUP BY ch.character_id
                ''',
                rows=20)

    inte.result('On average, how many Items does each Character have?',
                '''
                SELECT AVG(cnt) FROM (
                    SELECT name, COUNT(inv.character_id) AS cnt FROM charactercreator_character as ch
                    LEFT JOIN charactercreator_character_inventory as inv
                    ON ch.character_id = inv.character_id
                    GROUP BY ch.character_id
                )
                ''',
                rows=0)

    inte.result('On average, how many Weapons does each Character have?',
                '''
                SELECT AVG(cnt) FROM (
                    SELECT name, COUNT(inv.character_id) AS cnt FROM charactercreator_character as ch
                    LEFT JOIN (charactercreator_character_inventory as inv
                    INNER JOIN armory_weapon as wp
                    ON inv.item_id = wp.item_ptr_id)
                    ON ch.character_id = inv.character_id
                    GROUP BY ch.character_id
                )
                ''',
                rows=0)

    print('------------------\n')