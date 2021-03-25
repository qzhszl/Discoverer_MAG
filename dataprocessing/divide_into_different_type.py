# -*- coding: utf-8 -*-
# @Time    : 2021/3/25 15:27
# @Author  : Zhihao Qiu
# @Description:
# 1.clone the paper collection into my database
# 2.divide into the paper into different type by venue.
from utils.connect_to_table import connectTable


def clone_collection():
    coll = connectTable("oga_one", "mag_paper_plus2")
    # col2 = connectTable("qiuzh","MAG_authors")
    col3 = connectTable("qiuzh","papers")
    # id_list = col2.distinct("new_pubs")

    for i in coll.find({"$and":[{"venue":{"$exists":True}}]}):
        # print(1)
        # if i.get("id") in id_list:
        col3.insert_one(i)
    print(col3.find().count())


if __name__ == '__main__':
    clone_collection()
    print("yes okay")
