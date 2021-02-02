# -*- coding: utf-8 -*-
# @Time    : 2021/1/10 14:46
# @Author  : Zhihao Qiu
# @Description: MAG_authors 去掉信息不全的author
from time import time

import pymongo

from utils.connect_to_table import connectTable


def filter_author_by_citation(msg, begin, end):
    col1 = connectTable("academic", "mag_authors")
    col2 = connectTable('qiuzh', "MAG_authors")
    opt = []
    # count = 0
    for i in col1.find({"n_pubs":{"$gte":5}})[begin: end]:
        a =i
        opt.append(pymongo.InsertOne(i))
    col2.bulk_write(opt, ordered=False)
    print("线程： %s, 遍历了 %s" % (msg, len(opt)))


def filter_author_by_citation2(msg, begin, end):
    col2 = connectTable('qiuzh', "MAG_authors")
    result = col2.delete_many({"$or": [{"n_pubs": {"$not": {"$gte": 10}}}, {"$and": [{"org": None}, {"orgs": None}]}]})
    print(result.deleted_count)  # 被删除的个数


if __name__ == "__main__":
    start = time()
    col2 = connectTable("qiuzh", "MAG_authors")
    col2.create_index([('n_pubs', 1)])
    # pool = multiprocessing.Pool(processes=10)
    # for i in range(1):
    #     msg = "process %d" % (i)
    #     pool.apply_async(filter_author_by_citation2, (msg, 1317972 * i, 1317972 * (i + 1)))
    # pool.close()
    # pool.join()
    # print("--------Sub-process all done.-----------")
    # end = time()
    # print("main process run time: %s" % (end - start))
