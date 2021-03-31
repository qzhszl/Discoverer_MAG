# -*- coding: utf-8 -*-
# @Time    : 2021/1/10 14:46
# @Author  : Zhihao Qiu
# @Description: MAG_authors 去掉信息不全的author
import multiprocessing
from time import time

import pymongo

from utils.connect_to_table import connectTable


def filter_author_by_citation(begin, end,msg):
    '''
    :param msg: multi-process information
    :param begin: i-th
    :param end: i+1-th
    :return: pubs counts>=5
    '''
    col1 = connectTable("academic", "mag_authors")
    col2 = connectTable('qiuzh', "MAG_authors")
    opt = []
    # count = 0
    for i in col1.find({"n_pubs":{"$gte":5}})[begin: end]:
        a =i
        opt.append(pymongo.InsertOne(i))
    col2.bulk_write(opt, ordered=False)
    print("线程： %s, 遍历了 %s" % (msg, len(opt)))


def filter_author_by_citation2():
    '''
    :param msg:
    :param begin:
    :param end:
    :return: pubs>=10, org exist(affiliation)
    '''
    col2 = connectTable('qiuzh', "MAG_authors")
    result = col2.delete_many({"$or": [{"n_pubs": {"$not": {"$gte": 10}}}, {"$and": [{"org": None}, {"orgs": None}]}]})
    print(result.deleted_count)  # 被删除的个数


def filter_author_by_new_pubs():
    '''
    :param msg:
    :param begin:
    :param end:
    :return: new_pubs
    '''
    col2 = connectTable('qiuzh', "MAG_authors")
    result = col2.delete_many({"new_pubs": {"$exists":False}})
    print(result.deleted_count)  # 被删除的个数


def filter_papers_by_new_pubs():
    '''
    :param msg:
    :param begin:
    :param end:
    :return: new_pubs
    '''
    col1 = connectTable("qiuzh","papers")
    col2 = connectTable('qiuzh', "MAG_authors")
    medset =set()
    for i in col2.find():
        for j in i.get("new_pubs"):
            medset.add(j)
    print(len(medset))
    result=col1.delete_many({"id": {"$nin":list(medset)}})
    print(result.deleted_count)  # 被删除的个数


def delete_new_pubs( begin, end,msg):
    col = connectTable('qiuzh', "MAG_authors")
    cursor = col.find({"new_pubs":{"$exists": True}},no_cursor_timeout=True)[begin:end]
    for i in cursor:
        _id = i.get("_id")
        print(_id)
        col.update_one({"_id":_id}, {"$unset": {"new_pubs": 1}},False,True)
    cursor.close()
    print("yes okay")


if __name__ == "__main__":
    '''
    multipro 
    '''
    # delete_new_pubs(0,3,1)
    # start = time()
    # p1 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 0, 441324 * 0 + 441324, 1))
    # p2 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 1, 441324 * 1 + 441324, 2))
    # p3 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 2, 441324 * 2 + 441324, 3))
    # p4 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 3, 441324 * 3 + 441324, 4))
    # p5 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 4, 441324 * 4 + 441324, 5))
    # p6 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 5, 441324 * 5 + 441324, 6))
    # p7 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 6, 441324 * 6 + 441324, 7))
    # p8 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 7, 441324 * 7 + 441324, 8))
    # p9 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 8, 441324 * 8 + 441324, 9))
    # p10 = multiprocessing.Process(target=filter_papers_by_new_pubs, args=(441324 * 9, 441324 * 9 + 441324, 10))
    #
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
    #
    # end = time()
    # print("run time: %s" % (end - start))

    '''
    single process
    '''
    start_time = time()
    filter_papers_by_new_pubs()
    end_time = time()
    print("run times: %s" % (end_time - start_time))
