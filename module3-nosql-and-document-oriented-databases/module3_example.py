'''Inserts all data from rpg_db.sqlite3 into MongoDB. 

First make a creds.txt file with your connection string (including password)
and place it in the same directory as this file.

Inserts data into database 'rpg', collection 'rpg'.
'''

import sqlite3
import pymongo

class LiteToMong:
    def __init__(self, lite_db, mong_creds):
        self.l_conn = sqlite3.connect(lite_db)
        self.l_curs = self.l_conn.cursor()
        self.client = pymongo.MongoClient(mong_creds)
        self.db = self.client['rpg']

    def pipeline(self, table_name, col_names, skip_first=False):
        data = self.l_curs.execute('SELECT * FROM ' + table_name + ';').fetchall()
        if len(data) > 0:
            docs = []
            for row in data:
                doc = {}
                for j in range(1 if skip_first else 0, len(col_names)):
                    doc[col_names[j]] = row[j]
                docs.append(doc)
            print('inserting ' + table_name + '...')
            self.db['rpg'].insert_many(docs)
        else:
            print('no data in ' + table_name)


if __name__ == '__main__':
    f = open('creds.txt', 'r')
    mong_str = f.readline()
    f.close()

    print('connecting...')
    t = LiteToMong(lite_db='rpg_db.sqlite3', mong_creds=mong_str)

    # Character tables
    t.pipeline('charactercreator_character',
               ['character_id', 'name', 'level', 'exp', 'hp',
                'strength', 'intelligence', 'dexterity', 'wisdom'])
    t.pipeline('charactercreator_mage',
               ['character_id', 'has_pet', 'mana'])
    t.pipeline('charactercreator_necromancer',
               ['character_id', 'talisman_charged'])
    t.pipeline('charactercreator_thief',
               ['character_id', 'is_sneaking', 'energy'])
    t.pipeline('charactercreator_cleric',
               ['character_id', 'using_shield', 'mana'])
    t.pipeline('charactercreator_fighter',
               ['character_id', 'using_shield', 'rage'])

    # Item tables
    t.pipeline('armory_item',
               ['item_id', 'name', 'value', 'weight'])
    t.pipeline('armory_weapon',
               ['item_id', 'power'])
    t.pipeline('charactercreator_character_inventory',
               ['id', 'character_id', 'item_id'],
               skip_first=True)

    # Django/Auth tables
    t.pipeline('django_content_type',
               ['content_type_id', 'app_label', 'model'])
    t.pipeline('auth_group',
               ['group_id', 'name'])
    t.pipeline('auth_permission',
               ['permission_id', 'content_type_id', 'codename', 'name'])
    t.pipeline('auth_user',
               ['user_id', 'password', 'last_login', 'is_superuser',
                'username', 'first_name', 'email', 'is_staff', 'is_active',
                'date_joined', 'last_name'])
    t.pipeline('django_session',
               ['session_key', 'session_data', 'expire_date'])
    t.pipeline('django_migrations',
               ['id', 'app', 'name', 'applied'],
               skip_first=True)
    t.pipeline('auth_group_permissions',
               ['id', 'group_id', 'permission_id'],
               skip_first=True)
    t.pipeline('auth_user_groups',
               ['id', 'user_id', 'group_id'],
               skip_first=True)
    t.pipeline('auth_user_user_permissions',
               ['id', 'user_id', 'permission_id'],
               skip_first=True)
    t.pipeline('django_admin_log',
               ['id', 'object_id', 'object_repr', 'action_flag',
                'change_message', 'content_type_id', 'user_id',
                'action_time'],
               skip_first=True)

    print('\ndone!\n')
