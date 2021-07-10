# coding = utf-8
# @Time: 2021/4/7 10:33
# Author: Zhihao Qiu
from utils.connect_to_table import connectTable
import multiprocessing
from time import time

import pymongo
'''
Functions in this .PY are used to get statistics and add them into collections.E.g. citation,coauthor_times,discovery_times 
'''

# paper:


def paper_field_number():
    '''
    :return: paper counts in each field
    '''

    mycol = connectTable("qiuzh", "filtered_papers")
    print(mycol.find({"field":{"$exists":True}}).count())
    for field in ["GEOGRAPHY", "ASTRONOMY", "ENGINEERING", "MANAGEMENT",
                  "CHEMISTRY", "ENVIRONMENTAL SCIENCES", "AGRONOMY",
                  "SOCIAL SCIENCE", "BIOLOGY", "MATHEMATICS", "PHYSICS",
                  "MEDICINE", "MULTIDISCIPLINARY SCIENCES"]:
        print(field)
        print(field, ":", mycol.find({"field": field}).count())


def paper_citation_number(begin, end, msg):
    '''
    this function is appropriate for citation_network0515 and mag_papers0510
    :return: add each papers' total citation in mag_papers0510
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_citation_network = connectTable("qiuzh", "citation_network0515")

    count = 0
    operation = []
    cursor = colpaper.find(no_cursor_timeout=True)[begin:end]
    for paper in cursor:
        count += 1
        paper_id = paper["_id"]
        citation_number = 0
        paper_citation_relations = col_citation_network.find({"id": paper_id}, no_cursor_timeout=True)
        if paper_citation_relations:
            for paper_citation_relation in paper_citation_relations:
                citation_number += len(paper_citation_relation["citation"])
        operation.append(pymongo.UpdateOne({"_id": paper_id}, {"$set": {"cn": citation_number}}))

        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            colpaper.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        colpaper.bulk_write(operation, ordered=False)
    cursor.close()



# author


def author_citation_number(begin,end,msg):
    '''
    this function is appropriate for mag_authors0510 and citation network0515
    :return:
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "mag_authors0510")

    count = 0
    operation = []
    cursor = col_author.find(no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        citation_number = 0
        for paper in author["new_pubs"]:
            p = colpaper.find_one({"_id": paper["pid"]}, no_cursor_timeout=True)
            citation_number+=p["cn"]

        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"cn": citation_number}}))

        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    cursor.close()


def author_citation_number_patch():
    '''
    this function is appropriate for mag_authors0510 and citation network0515
    :return:
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "mag_authors0510")

    count = 0
    operation = []
    cursor = col_author.find({"cn":{"$exists":False}},no_cursor_timeout=True)
    for author in cursor:
        count += 1
        author_id = author["_id"]
        citation_number = 0
        for paper in author["new_pubs"]:
            p = colpaper.find_one({"_id": paper["pid"]}, no_cursor_timeout=True)
            citation_number+=p["cn"]

        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"cn": citation_number}}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    cursor.close()


def author_coauthor_number(begin,end,msg):
    '''
    this function is appropriate for mag_authors0510 and citation network0515
    :return:
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "mag_researchers0707")

    count = 0
    operation = []
    cursor = col_author.find(no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        coauthor_number = 0
        for paper in author["new_pubs"]:
            p = colpaper.find_one({"_id": paper["pid"]}, no_cursor_timeout=True)
            coauthor_number += (len(p["authors"]) - 1)

        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"con": coauthor_number}}))

        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    cursor.close()


def author_coauthor_number_patch():
    '''
    this function is appropriate for mag_authors0510 and citation network0515
    :return:
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "mag_authors0510")

    count = 0
    operation = []
    cursor = col_author.find({"con":{"$exists":False}},no_cursor_timeout=True)
    for author in cursor:
        count += 1
        author_id = author["_id"]
        coauthor_number = 0
        for paper in author["new_pubs"]:
            p = colpaper.find_one({"_id": paper["pid"]}, no_cursor_timeout=True)
            coauthor_number += (len(p["authors"]) - 1)

        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"con": coauthor_number}}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    cursor.close()


def author_junior_coauthor_number(begin,end,msg):
    '''
    this function is appropriate for mag_authors0510 and citation network0515
    :return:
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "mag_authors0510")

    count = 0
    operation = []
    cursor = col_author.find(no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        author_year = author["first_year"]
        coauthor_number = 0
        for paper in author["new_pubs"]:
            if paper["year"] - author_year <= 3:
                p = colpaper.find_one({"_id": paper["pid"]}, no_cursor_timeout=True)
                coauthor_number += (len(p["authors"]) - 1)

        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"jcon": coauthor_number}}))

        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    cursor.close()


def author_be_discovered(begin, end, msg):
    '''
    this function is appropriate for mag_authors0510 and citation network0515
    authors are discovered by who
    2700636 top researchers in total
    relationships are saved in the
    :return:
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "mag_authors0510")
    col_be_discovered = connectTable("qiuzh", "be_discovered0622")

    count = 0
    operation = []

    # cursor_count = col_author.find({"iftop": 1}, no_cursor_timeout=True).count()
    # print(cursor_count)
    cursor = col_author.find({"iftop":1},no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        author_year = author["first_year"]
        discovery_list =[]
        aucount = 0
        for paper in author["new_pubs"]:
            if paper["year"] - author_year <= 3:
                p = colpaper.find_one({"_id": paper["pid"]}, no_cursor_timeout=True)
                p_authors = p["authors"]
                for au in p_authors:
                    if not au["id"] == author_id:
                        aucount += 1
                        discovery_list.append(au["id"])
                        if aucount == 2000:
                            operation.append(pymongo.InsertOne({"id": author_id, "be_discovered": discovery_list}))
                            discovery_list = []
                            aucount = 0
        if discovery_list:
            operation.append(pymongo.InsertOne({"id": author_id, "be_discovered": discovery_list}))

        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            col_be_discovered.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_be_discovered.bulk_write(operation, ordered=False)
    print(msg,count)
    cursor.close()


def initialize_discover_number():
    col_author = connectTable("qiuzh", "mag_authors0510")

    cursor = col_author.find(no_cursor_timeout=True)
    # researcher_number = cursor.count()
    # print(researcher_number)
    count = 0
    operation = []
    for author in cursor:
        count += 1
        operation.append(pymongo.UpdateOne({"_id": author["_id"]}, {"$set": {"dn": -1}}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
    if operation:
        col_author.bulk_write(operation, ordered=False)
    print("finished")
    cursor.close()
    print(count)
    print(col_author.find({"dn":-1},no_cursor_timeout=True).count())


def author_discover_number(begin, end, msg):
    '''
    this function is appropriate for mag_authors0510 and be_discovered0521
    How many times do an author discover others in his career life.
    :return:
    '''

    col_author = connectTable("qiuzh", "mag_authors0510")
    col_be_discovered = connectTable("qiuzh", "be_discovered0622")

    count = 0
    operation = []

    # cursor_count = col_author.find({"iftop": 1}, no_cursor_timeout=True).count()
    # print(cursor_count)
    cursor = col_author.find(no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        discovery_time = col_be_discovered.find({"be_discovered":author_id},no_cursor_timeout=True).count()
        operation.append(pymongo.UpdateOne({"_id": author_id},
                                                {"$set": {"dn": discovery_time}}))

        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    print(msg,count)
    cursor.close()


def author_discover_number_patch():
    '''
    this function is appropriate for mag_authors0510 and be_discovered0521
    How many times do an author discover others in his career life.
    :return:
    '''

    col_author = connectTable("qiuzh", "mag_authors0510")
    col_be_discovered = connectTable("qiuzh", "be_discovered0521")

    count = 0
    operation = []

    # cursor_count = col_author.find({"iftop": 1}, no_cursor_timeout=True).count()
    # print(cursor_count)
    cursor = col_author.find({"dn":{"$exists":False}},no_cursor_timeout=True)
    for author in cursor:
        count += 1
        author_id = author["_id"]
        discovery_time = col_be_discovered.find({"be_discovered":author_id},no_cursor_timeout=True).count()
        operation.append(pymongo.UpdateOne({"_id": author_id},
                                                {"$set": {"dn": discovery_time}}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    cursor.close()


def author_pubs_number():
    mycol = connectTable("qiuzh", "mag_authors0510")
    for i in mycol.find():
        author_id = i["_id"]
        pub_number = len(i["new_pubs"])
        mycol.update_one({"_id": author_id}, {"$set": {"pub_count": pub_number}})


if __name__ == '__main__':
    # initialize_discover_number()
    start = time()

    p1 = multiprocessing.Process(target=author_discover_number,
                                 args=(5401421 * 0, 5401421 * 0 + 5401421, 1))
    p2 = multiprocessing.Process(target=author_discover_number,
                                 args=(5401421 * 1, 5401421 * 1 + 5401421, 2))
    p3 = multiprocessing.Process(target=author_discover_number,
                                 args=(5401421 * 2, 5401421 * 2 + 5401421, 3))
    p4 = multiprocessing.Process(target=author_discover_number,
                                 args=(5401421 * 3, 5401421 * 3 + 5401421, 4))
    p5 = multiprocessing.Process(target=author_discover_number,
                                 args=(5401421 * 4, 5401421 * 4 + 5401421, 5))

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()

    end = time()
    print("run time: %s" % ((end - start)/60))


