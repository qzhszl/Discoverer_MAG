# coding = utf-8
# @Time: 2021/4/7 15:19
# Author: Zhihao Qiu
'''
first, we need to figure out two questions:
1. shall we find discoverers in different fields separately or in the whole network?: the latter one, because we want to
study the multi-discipline influence
2. the co-authorship network can be measured by paper-collection
'''
import multiprocessing
from time import time

import pymongo

from utils.connect_to_table import connectTable


def add_coauthor_relation(begin,end,msg):
    '''
    coauthor times and coauthor relationships
    :return:
    mag_authors0411:
    {coauthor_counts:n}
    {coauthor_list:[{year:1999,id:1000000},year:1998,id:1000001}]}
    '''
    start_time = time()
    print(start_time)
    col1 = connectTable("qiuzh", "mag_papers0415")
    col2 = connectTable("qiuzh", "mag_authors0411")
    operation = []
    cursor = col2.find(no_cursor_timeout=True)[begin:end]
    for i in cursor:
        author_id = i["_id"]
        coauthor_times = 0
        coauthor_list = []
        papers = i["new_pubs"]
        for paper in papers:
            paper_details = col1.find_one({"_id": paper})
            # if paper_details:
            coauthor_times += (len(paper_details["authors"]) - 1)
            for author in paper_details["authors"]:
                if author["id"] != author_id:
                    coauthor_list.append({"coauthor_id": author["id"], "coauthor_time": paper_details["year"]})
        if len(coauthor_list)>0:
            operation.append(pymongo.UpdateOne({"_id": author_id},
                                                {"$set": {"coauthor_counts": coauthor_times, "coauthor": coauthor_list}}))
    print(msg,"线程已完成",len(operation),flush=True)
    col2.bulk_write(operation, ordered=False)
    cursor.close()
    print(msg,time(), (time() - start_time))


def add_coauthor_relation2newcollection():
    '''
    coauthor times and coauthor relationships
    :return:
    mag_authors0411:
    {coauthor_counts:n}
    {coauthor_list:[{year:1999,id:1000000},year:1998,id:1000001}]}
    because some of the authors in the dataset have too many collaborations and exceed the maximum RAM of a document,
    we store the relation in a new collection
    _id:
    "author_id" :
    "coauthor_id":
    "coauthor_time":
    '''

    start_time = time()
    print(start_time,flush=True)
    col1 = connectTable("qiuzh", "mag_papers0415")
    col2 = connectTable("qiuzh", "mag_authors0411")
    col3 = connectTable("qiuzh", "coauthor_network0420")
    operation = []
    cursor = col2.find(no_cursor_timeout=True)[3790001:]
    count =0
    for i in cursor:
        count+=1
        author_id = i["_id"]
        # coauthor_times = 0
        # coauthor_list = []
        papers = i["new_pubs"]
        for paper in papers:
            paper_details = col1.find_one({"_id": paper})
            # if paper_details:
            # coauthor_times += (len(paper_details["authors"]) - 1)
            for author in paper_details["authors"]:
                if author["id"] != author_id:
                    # coauthor_list.append({"coauthor_id": author["id"], "coauthor_time": paper_details["year"]})
                    operation.append(pymongo.InsertOne(
                        {"author_id": author_id, "coauthor_id": author["id"], "coauthor_time": paper_details["year"],
                         }))
        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col3.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col3.bulk_write(operation, ordered=False)
    print("已完成",len(operation),flush=True)
    print(time(), (time() - start_time), flush=True)


def calculate_coauthor_times():
    start_time = time()
    print(start_time)
    col2 = connectTable("qiuzh", "mag_authors0411")
    col3 = connectTable("qiuzh", "coauthor_network0420")
    operation = []
    count=0
    cursor = col2.find(no_cursor_timeout=True)
    for i in cursor:
        count+=1
        author_id = i["_id"]
        coauthor_times = col3.count({"author_id": author_id})
        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"coauthor_times": coauthor_times}}))
        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col2.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col2.bulk_write(operation, ordered=False)
    cursor.close()
    print(time(), (time() - start_time))


def calculate_coauthor_times2():
    '''
    this version is appropriate for mag_authors0510
    this function is not finished
    :return:
    '''
    start_time = time()
    print(start_time)
    col1 = connectTable("qiuzh", "mag_authors0411")
    operation = []
    count=0
    cursor = col1.find(no_cursor_timeout=True)
    for i in cursor:
        count+=1
        author_id = i["_id"]
        coauthor_times = col1.count({"author_id": author_id})
        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"coauthor_times": coauthor_times}}))
        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col1.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col1.bulk_write(operation, ordered=False)
    cursor.close()
    print(time(), (time() - start_time))


if __name__ == '__main__':
    start = time()
    # add_coauthor_relation2newcollection()
    calculate_coauthor_times()


    # p1 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 0, 5132607 * 0 + 5132607, 1))
    # p2 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 1, 5132607 * 1 + 5132607, 2))
    # p3 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 2, 5132607 * 2 + 5132607, 3))
    # p4 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 3, 5132607 * 3 + 5132607, 4))
    # p5 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 4, 5132607 * 4 + 5132607, 5))
    # p6 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 5, 5132607 * 5 + 5132607, 6))
    # p7 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 6, 5132607 * 6 + 5132607, 7))
    # p8 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 7, 5132607 * 7 + 5132607, 8))
    # p9 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 8, 5132607 * 8 + 5132607, 9))
    # p10 = multiprocessing.Process(target=add_coauthor_relation2newcollection, args=(5132607 * 9, 5132607 * 9 + 5132607, 10))

    # p1.start()
    # p2.start()
    # p3.start()
    # p4.start()
    # p5.start()
    # p6.start()
    # p7.start()
    # p8.start()
    # p9.start()
    # p10.start()
    #
    # p1.join()
    # p2.join()
    # p3.join()
    # p4.join()
    # p5.join()
    # p6.join()
    # p7.join()
    # p8.join()
    # p9.join()
    # p10.join()

    end = time()
    print("run time: %s" % (end - start))


