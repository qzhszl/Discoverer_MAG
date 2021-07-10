# -*- coding: utf-8 -*-
import math
import matplotlib.pyplot as plt
import pymongo
import numpy as np
from scipy import stats

from utils.connect_to_table import connectTable

__author__ = "ZHIHAO QIU"

# mycol1 = connectTable("qiuzh","test1")
# mycol2 = connectTable("qiuzh","test")
# # mydb1.update({"n_pubs":{"$exists": True}},{"$unset":{"n_pubs":""}})
# # a = mydb1.find({"orgs":"Centre national de la recherche scientifiqueaaa"}).count()
# # # print(a)
# years = mycol1.find({"year":1981})
# mycol2.insert_many(years)




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

# myclient = pymongo.MongoClient('mongodb://cssc:cssc#666@121.48.161.168:27017')
# database = myclient['qiuzh']
# papers=database.papers
# # papers.insert({"accout":21,"username":"libing"})
# # papers.delete_one({"accout":21})
# print(database.collection_names())
# for i in papers.find():
#     print(i)


# mycol = connectTable("qiuzh", "mag_papers0510")
# print(mycol.index_information())
# for i in mycol.find():
#     a= i["testlist"]
#     for j in a:
#         c=1
# mycol2 = connectTable("qiuzh", "test")
# print(mycol2.find_one())
# a = ["10123","10125","10126"]
# mycol.update_many({"year": {"$exists": True}},{"$set": {"testlist": a}})
# paper = mycol.find_one({"year": 1981})
# mycol2.insert_one(paper)
# a = mycol.count({"year":1981})
# print(mycol.count({"year":1981}))
# mycol.update_many({"id":{"$exists":True}},{"$unset":{"aiqing":1,"title":1}})
# col.update_many({"coauthor_counts": {"$exists": True}}, {"$unset": {"coauthor_counts": 1, "coauthor": 1}})
#     cursor.close()

# coll = connectTable("qiuzh", "mag_authors0411")
# for i in coll.find({"_id":{"$gt":"999992564"}}).limit(10):
#     print(i)
# 1000009205
# 999992564

#
# col_author = connectTable("qiuzh", "mag_authors0510")
#
# cursor_count = col_author.find({"iftop": 1}, no_cursor_timeout=True).count()
# print(cursor_count)

# 27032 53631 0.0
# 33791 103309 0.0
# 118857 400493 0.0
# 9977 28595 0.0
# 159838 623608 0.0
# d_i = 27032
# k_i = 53631
# P = stats.binom.sf(d_i - 1, k_i, 0.23289631053626692)
# print(P)
# if P == 0:
#     S = 1
# # S = -math.log(P)
# print(P)
# print(S)

# d_i = np.random.binomial(5, 0.2, 20)
# print(d_i.mean())


# X= np.arange(d_i,k_i+1,1)
# print(d_i,k_i)
# P0_med = stats.binom.pmf(X,k_i,0.2)
# P0 = np.sum(P0_med)
# print(P0)
# S = -math.log(0)
# print(S)



col_author = connectTable("qiuzh", "mag_researchers0707")

# cursor_count = col_author.count_documents({"dn":-1})
# print(cursor_count)
#
# cursor_count = col_author.find({"bsur":{"$exists":False}}, no_cursor_timeout=True).count()
# print(cursor_count)

cursor_count = col_author.count_documents({})
print(cursor_count)
# for author in col_author.find({"sur":-1})[:5]:
#     print(author)

# 'con': 53631, 'dn': 27122,
# 'con': 623608, 'dn': 160299,

# d_i = 27122
# k_i = 53631
# P = stats.binom.sf(d_i - 1, k_i, 0.23)
#
# X= np.arange(d_i,k_i+1,1)
# print(X)
# print(d_i,k_i)
# P0_med = stats.binom.pmf(X,k_i,0.23)
# P1 = math.fsum(P0_med)
#
# print(P)
# print(P1)
#
# # print(math.log(0.23))
# for i in range(0,26000,100):
#     P2 = stats.binom.sf(i - 1, 53631, 0.23)
#     S = -math.log(P2)
#     print(i,P2,S)
