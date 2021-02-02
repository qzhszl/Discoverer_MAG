#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/1/10 14:46
# @Author  : Zhihao Qiu
# @Description: 因为MAG_authors里面存的all_pub里面全是v2的文章id,现在需要用到参考文章，所以需要把v2的文章id替换为v1的
import multiprocessing
from time import time

import pymongo

from utils.connect_to_table import connectTable


def match_v1_v2_id(msg, begin, end):
    coll = connectTable("oga_one", "mag_paper")
    coll3 = connectTable('wangwenbin', "MAG_authors")
    opt = []
    count = 0
    for i in coll3.find()[begin: end]:
        _id = i.get("_id")
        new_pubs = []
        papers = coll.find({"new_authors.id": _id})
        for paper in papers:
            id = paper.get("id")
            new_pubs.append(id)
        opt.append(pymongo.UpdateOne({"_id": _id},
                                     {"$set": {"new_pubs": new_pubs}}
                                     ))
        # if len(opt) == 1000:
    #         count += 1
    #         print(count)
    #         coll3.bulk_write(opt, ordered=False)
    #         opt = []
    coll3.bulk_write(opt, ordered=False)
    print("线程： %s, 遍历了 %s" % (msg, len(opt)))

if __name__ == "__main__":
    start = time()
    pool = multiprocessing.Pool(processes=10)
    for i in range(10):
        msg = "process %d" % (i)
        pool.apply_async(match_v1_v2_id, (msg, 277643 * i, 277643 * (i + 1)))
    pool.close()
    pool.join()
    print("--------Sub-process all done.-----------")
    end = time()
    print("main process run time: %s" % (end - start))
