# -*- coding: utf-8 -*-
import multiprocessing
from time import time

import pymongo

from utils.connect_to_table import connectTable

__author__="ZHIHAO QIU"


"""
find author first and last published year
(the author career length)
"""


def researher_first_year():
    '''
    this function is appropriate for mag_papers0415 and mag_authors0411
    :return:
    '''
    # print()
    col1 = connectTable("qiuzh", "mag_papers0415")
    col2 = connectTable('qiuzh', "mag_authors0411")
    opt = []
    count =0
    cursor = col2.find(no_cursor_timeout=True)[4050001:]
    for i in cursor:
        count += 1
        researher_id = i["_id"]
        papers_list = i["new_pubs"]
        year_list=[]
        papers = col1.find({"_id": {"$in":papers_list}})
        # min_year = -1
        # max_year = 100000
        for paper in papers:
            year_list.append(paper["year"])
        opt.append(pymongo.UpdateOne({"_id": researher_id},
                                     {"$set": {"first_year": min(year_list),
                                               "last_year": max(year_list)}}
                                     ))
        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col2.bulk_write(opt, ordered=False)
            print("已写入:", count / 10000, flush=True)
            opt = []
            print(time(), flush=True)
    if opt:
        col2.bulk_write(opt, ordered=False)
    cursor.close()
    # print("线程： %s, 遍历了 %s" % (msg, len(opt)))


def researher_year2(begin,end,msg):
    '''
    this function is appropriate for mag_authors0510
    Add first year and last year into the documents
    :return:
    '''
    # print()
    col1 = connectTable('qiuzh', "mag_authors0510")
    opt = []
    count =0
    cursor = col1.find(no_cursor_timeout=True)[begin:end]
    for i in cursor:
        count += 1
        researher_id = i["_id"]
        papers_list = i["new_pubs"]
        first_year = papers_list[0]["year"]
        last_year = first_year
        for paper in papers_list:
            if paper["year"]<first_year:
                first_year = paper["year"]
            elif paper["year"]>last_year:
                    last_year = paper["year"]
        # last_year = max(papers_list, key=lambda dic: dic["year"])["year"]
        # first_year = min(papers_list, key=lambda dic: dic["year"])["year"]

        opt.append(pymongo.UpdateOne({"_id": researher_id},
                                     {"$set": {"first_year": first_year,
                                               "last_year": last_year}}
                                     ))
        if count % 10000 == 0:
            print(msg,"已处理:", count / 10000, flush=True)
            col1.bulk_write(opt, ordered=False)
            print(msg,"已写入:", count / 10000, flush=True)
            opt = []
            print(time(), flush=True)
    if opt:
        col1.bulk_write(opt, ordered=False)
    cursor.close()
    # print("线程： %s, 遍历了 %s" % (msg, len(opt)))


def researher_year2_patch():
    '''
    this function is appropriate for mag_authors0510
    Add first year and last year into the documents
    :return:
    '''
    # print()
    col1 = connectTable('qiuzh', "mag_authors0510")
    opt = []
    count =0
    cursor = col1.find({"first_year":None},no_cursor_timeout=True)
    for i in cursor:
        count += 1
        researher_id = i["_id"]
        papers_list = i["new_pubs"]
        first_year = papers_list[0]["year"]
        last_year = first_year
        for paper in papers_list:
            if paper["year"]<first_year:
                first_year = paper["year"]
            elif paper["year"]>last_year:
                    last_year = paper["year"]
        # last_year = max(papers_list, key=lambda dic: dic["year"])["year"]
        # first_year = min(papers_list, key=lambda dic: dic["year"])["year"]

        opt.append(pymongo.UpdateOne({"_id": researher_id},
                                     {"$set": {"first_year": first_year,
                                               "last_year": last_year}}
                                     ))
        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col1.bulk_write(opt, ordered=False)
            print("已写入:", count / 10000, flush=True)
            opt = []
            print(time(), flush=True)
    if opt:
        col1.bulk_write(opt, ordered=False)
    cursor.close()
    # print("线程： %s, 遍历了 %s" % (msg, len(opt)))


if __name__ == '__main__':
    start = time()
    print(start)
    researher_year2_patch()
    # p1 = multiprocessing.Process(target=researher_year2, args=(5388503 * 0, 5388503 * 0 + 5388503, 1))
    # p2 = multiprocessing.Process(target=researher_year2, args=(5388503 * 1, 5388503 * 1 + 5388503, 2))
    # p3 = multiprocessing.Process(target=researher_year2, args=(5388503 * 2, 5388503 * 2 + 5388503, 3))
    # p4 = multiprocessing.Process(target=researher_year2, args=(5388503 * 3, 5388503 * 3 + 5388503, 4))
    # p5 = multiprocessing.Process(target=researher_year2, args=(5388503 * 4, 5388503 * 4 + 5388503, 5))
    # # p6 = multiprocessing.Process(target=researher_first_year, args=(5132607 * 5, 5132607 * 5 + 5132607, 6))
    # # p7 = multiprocessing.Process(target=researher_first_year, args=(5132607 * 6, 5132607 * 6 + 5132607, 7))
    # # p8 = multiprocessing.Process(target=researher_first_year, args=(5132607 * 7, 5132607 * 7 + 5132607, 8))
    # # p9 = multiprocessing.Process(target=researher_first_year, args=(5132607 * 8, 5132607 * 8 + 5132607, 9))
    # # p10 = multiprocessing.Process(target=researher_first_year, args=(5132607 * 9, 5132607 * 9 + 5132607, 10))
    #
    # p1.start()
    # p2.start()
    # p3.start()
    # p4.start()
    # p5.start()
    # # p6.start()
    # # p7.start()
    # # p8.start()
    # # p9.start()
    # # p10.start()
    #
    # p1.join()
    # p2.join()
    # p3.join()
    # p4.join()
    # p5.join()
    # # p6.join()
    # # p7.join()
    # # p8.join()
    # # p9.join()
    # # p10.join()

    end = time()
    print("run time: %s" % (end - start))
