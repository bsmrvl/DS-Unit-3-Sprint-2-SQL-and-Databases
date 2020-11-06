'''Makes a bunch of Mongo queries on the RPG database.

First make a m_creds.txt file with your connection string (including password)
and place it in the same directory as this file.

Assumes data is in Mongo database 'rpg', collection 'rpg'.
'''

import pymongo

def creds(path):
    f = open(path, 'r')
    mong_str = f.readline()
    f.close()
    return mong_str

class MongRpg:
    def __init__(self, creds_path):
        self.client = pymongo.MongoClient(creds(creds_path))
        self.db = self.client['rpg']
        self.coll = self.db['rpg']


if __name__ == '__main__':
    print('connecting...')
    t = MongRpg('m_creds.txt')
    print('connected')

    print('\nRPG Summary Questions')
    print('-----------------------')

    # Characters
    all_char = t.coll.distinct('character_id')
    mage_necro = t.coll.find({'talisman_charged': {'$exists': True}}).distinct('character_id')
    mage_not_necro = t.coll.find({'has_pet': {'$exists': True}, 
                                  'character_id': {'$nin': mage_necro}}).distinct('character_id')
    thief = t.coll.find({'is_sneaking': {'$exists': True}}).distinct('character_id')
    cleric = t.coll.find({'using_shield': {'$exists': True},
                          'mana': {'$exists': True}}).distinct('character_id')
    fighter = t.coll.find({'using_shield': {'$exists': True},
                           'rage': {'$exists': True}}).distinct('character_id')

    print('\nAll characters:', len(all_char))
    print('Mages | necromancer:', len(mage_necro))
    print('Mages | not necromancer:', len(mage_not_necro))
    print('Thieves:', len(thief))
    print('Clerics:', len(cleric))
    print('Fighters:', len(fighter))

    # Items
    all_item = t.coll.distinct('item_id')
    weapon = t.coll.find({'power': {'$exists': True}}).distinct('item_id')
    non_weapon = t.coll.find({'item_id': {'$nin': weapon}}).distinct('item_id')

    print('\nAll items:', len(all_item))
    print('Weapons:', len(weapon))
    print('Non weapons:', len(non_weapon))

    # Inventories
    def items_per_char(item_ids):
        n_items = t.coll.aggregate([
            {'$match': {'item_id': {'$in': item_ids}}}, 
            {'$group': {'_id': '$character_id', 'count': {'$sum': 1}}}, 
            {'$match': {'_id': {'$ne': None}}},
            {'$sort': {'_id': 1}},
            {'$limit': 20},
            {'$lookup': {'from':'rpg', 'localField':'_id', 'foreignField':'character_id', 'as':'names'}},
            {'$project': {'name':'$names.name', 'count':1}}
        ])
        return n_items

    print('\nNumber of items for each character (first 20):')
    for char in items_per_char(all_item):
        print(char)
    print('\nNumber of weapons for each character (first 20 who have any):')
    for char in items_per_char(weapon):
        print(char)

    all_inv_item_count = [x['count'] for x in t.coll.aggregate([
        {'$match': {'$and': [{'item_id': {'$exists': True}}, {'character_id': {'$exists': True}}]}},
        {'$count': 'count'},
    ])][0]

    all_inv_weapon_count = [x['count'] for x in t.coll.aggregate([
        {'$match': {'$and': [{'item_id': {'$in': weapon}}, {'character_id': {'$exists': True}}]}},
        {'$count': 'count'},
    ])][0]

    print('\nAverage items per character:', all_inv_item_count / len(all_char))
    print('Average weapons per character:', all_inv_weapon_count / len(all_char))

    print('')

    t.client.close()
