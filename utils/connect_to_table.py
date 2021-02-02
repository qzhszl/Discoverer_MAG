import pymongo


def connectTable(db, col):
    try:
        myclient = pymongo.MongoClient('mongodb://cssc:cssc#666@121.48.161.168:27017')
        database = myclient[db]
        # print(database.collection_names())
        # print()
        collection = database[col]
        # print(collection.database)
        # print()
        return collection
    except:
        print('数据库密码认证错误')
