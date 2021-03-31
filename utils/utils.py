# -*- coding: utf-8 -*-
from utils.connect_to_table import connectTable


def create_index():
    # col2 = connectTable("oga_one", "mag_paper_plus2")
    # col2.create_index([('new_authors.id', 1)])
    col2 = connectTable("qiuzh", "papers")
    col2.create_index([('id', 1)])

    # col2 = connectTable("qiuzh", "test")
    # col2.create_index([('pubs.i', 1),("n_citation", 1)])
    # # a = col2.index_information()
    # print(a)


def clone_collection():
    coll = connectTable("oga_one", "mag_paper_plus2")
    # col2 = connectTable("qiuzh","MAG_authors")
    col3 = connectTable("qiuzh", "papers")
    # id_list = col2.distinct("new_pubs")
    for i in coll.find({"$and": [{"venue": {"$exists": True}}]}):
        col3.insert_one(i)
    print(col3.find().count())


if __name__ == '__main__':
    create_index()


