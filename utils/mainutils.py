# -*- coding: utf-8 -*-
from utils.connect_to_table import connectTable


def create_index():
    col2 = connectTable("qiuzh", "mag_researchers0707")
    col2.create_index([('first_year', 1)])
    # ('coauthor_id', 1)

    # col2 = connectTable("qiuzh", "test")
    # col2.create_index([('pubs.i', 1),("n_citation", 1)])
    # # a = col2.index_information()
    # print(a)


def clone_collection():
    coll = connectTable("oga_one", "mag_paper_plus2")
    # col2 = connectTable("qiuzh","MAG_authors")
    col3 = connectTable("qiuzh", "papers")
    for i in coll.find({"$and": [{"venue": {"$exists": True}}]}):
        col3.insert_one(i)
    print(col3.find().count())


def clone_paper_collection():
    coll = connectTable("oga_one", "mag_paper")
    col2 = connectTable("qiuzh", "mag_papers")
    for i in coll.find({"id": {"$exists": True}}):
        if "new_authors" in i.keys() and "year" in i.keys() and "references" in i.keys():
            new_document = {}
            new_document["_id"] = i["id"]
            new_document["authors"] = i["new_authors"]
            new_document["venue"] = i["new_venue"]
            new_document["year"] = i["year"]
            new_document["references"] = i["references"]
            col2.insert_one(new_document)
    print(col2.find().count())


def clone_author_collection():
    coll = connectTable("academic", "mag_authors")
    col2 = connectTable("qiuzh", "mag_authors0409")
    for i in coll.find({"id": {"$exists": True}}):
        if "pubs" in i.keys():
            new_document = {}
            new_document["_id"] = i["id"]
            new_document["pubs"] = i["pubs"]
            col2.insert_one(new_document)
    print(col2.find().count())


def rename_collection():
    coll = connectTable("qiuzh", "mag_papers")
    rename = coll.rename("mag_papers0409")


def rename_column():
    col = connectTable("qiuzh", "mag_authors0510")
    col.update_many({}, {"$rename": {"Sur": "bsur"}})


if __name__ == '__main__':
    # clone_author_collection()
    create_index()
    # rename_collection()
    # rename_column()
