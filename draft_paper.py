# -*- coding: utf-8 -*-
import pymongo

from utils.connect_to_table import connectTable

__author__ = "ZHIHAO QIU"

# mydb1 = connectTable("qiuzh","test")
# # mydb1.update({"n_pubs":{"$exists": True}},{"$unset":{"n_pubs":""}})
# a = mydb1.find({"orgs":"Centre national de la recherche scientifiqueaaa"}).count()
# print(a)




# mydb2 = connectTable("qiuzh","test1")
# insert_data = mydb.insert_one({ "name": "Google", "alexa": "2", "url": "https://www.google.com" })
# for i in mydb1.find({"$and":[{"year":{"$ne":None}},{"n_citation":{"$gte":4}}]}):
#     # citation = i.get("references")
#     mydb3.insert_one(i)
#
#     print(i)
# mydb1.insert_one({"title":"yes okay1111","year":2000,"n_citation":7})
# mydb1.insert_one({"title":"pick me up okay","n_citation":6})

# for i in mydb1.find({"$or":[{"n_citation":{"$not":{"$gte":6}}},{"id":None}]}):
#     print(i)

# result = mydb1.delete_many({"$or":[{"n_citation":{"$not":{"$gte":6}}}, {"$and":[{"id":None},{"year":None}]}]})
# print(result.deleted_count)  # 被删除的个数

# for i in mydb1.find():
#     _id= i.get("_id")
# for j in mydb1.find({"pubs.i": "1927789313"}):
#     print(j)
#     b=mydb1.update_one({"_id": _id},{"$set": {"new_pubs": 2,"test_year":0}})
#
# print("yes okay")


# coll = connectTable("oga_one", "mag_paper_plus2")
# coll3 = connectTable('qiuzh', "MAG_authors")
# b= coll3.find({"new_pubs":{"$exists":True}}).count()
# # for i in coll3.find().limit(1):
# print(b)
# for i in coll3.find({"new_pubs":{"$exists":True}}):
#     a=i
#     print(i)

# for i in coll3.find({"id": "1000130129"}):
#     print(i)

myclient = pymongo.MongoClient('mongodb://cssc:cssc#666@121.48.161.168:27017')
database = myclient['qiuzh']
papers=database.papers
# papers.insert({"accout":21,"username":"libing"})
# papers.delete_one({"accout":21})
print(database.collection_names())
for i in papers.find():
    print(i)
