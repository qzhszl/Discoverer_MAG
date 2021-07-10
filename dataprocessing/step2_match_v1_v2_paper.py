#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/1/10 14:46
# @Author  : Zhihao Qiu
# @Description: 因为MAG_authors里面存的all_pub里面全是v2的文章id,现在需要用到参考文章，所以需要把v2的文章id替换为v1的
import multiprocessing
from time import time

import pymongo

from utils.connect_to_table import connectTable

'''
{"_id":{"$oid":"5cc6796e229dab2c727e1d35"},"id":"1000009205","name":"Roghayeh Abbasi Talarposhti","normalized_name":"roghayeh abbasi talarposhti","org":"University of Mazandaran","orgs":["University of Mazandaran"],"pubs":[{"i":"2085697574","r":{"$numberInt":"2"}},{"i":"2254023713","r":{"$numberInt":"2"}},{"i":"2014839741","r":{"$numberInt":"2"}},{"i":"2106279130","r":{"$numberInt":"2"}},{"i":"2033044137","r":{"$numberInt":"3"}},{"i":"2736322158","r":{"$numberInt":"1"}},{"i":"2736540094","r":{"$numberInt":"1"}},{"i":"2736668731","r":{"$numberInt":"1"}},{"i":"2736736302","r":{"$numberInt":"1"}},{"i":"2736968276","r":{"$numberInt":"1"}},{"i":"2737077665","r":{"$numberInt":"1"}},{"i":"2737333104","r":{"$numberInt":"1"}},{"i":"2737370843","r":{"$numberInt":"1"}},{"i":"2737584260","r":{"$numberInt":"1"}},{"i":"2738789735","r":{"$numberInt":"1"}}],"n_pubs":{"$numberInt":"15"},"n_citation":{"$numberInt":"151"},"tags":[{"t":"Fredholm integral equation"},{"t":"Heat transfer"},{"t":"Adomian decomposition method"},{"t":"Korteweg–de Vries equation"},{"t":"Homotopy perturbation method"},{"t":"Decomposition method"},{"t":"Diffusion equation"},{"t":"Reaction–diffusion system"},{"t":"Heat equation"},{"t":"Initial value problem"},{"t":"Exact solutions in general relativity"},{"t":"Nonlinear system"},{"t":"Thermal conductivity"},{"t":"Integral equation"}]}

'''


def match_v1_v2_id( begin, end, msg):
    coll = connectTable("qiuzh", "mag_papers")
    coll3 = connectTable('qiuzh', "mag_authors0409")
    opt = []
    count=0
    cursor = coll3.find(no_cursor_timeout=True)[begin:end]
    for i in cursor:
        if count % 100000 == 0:
            print("线程： %s, 已完成 %s 万条" % (msg, count / 100000),flush=True)
        count += 1
        v2author_id = i.get("_id")
        # print(v2author_id)
        new_pubs = []
        papers = coll.find({"authors.id": v2author_id})
        for paper in papers:
            id = paper.get("_id")
            new_pubs.append(id)
        opt.append(pymongo.UpdateOne({"_id": v2author_id},
                                     {"$set": {"new_pubs": new_pubs}}
                                     ))

    cursor.close()
    coll3.bulk_write(opt, ordered=False)
    print("线程： %s, 遍历了 %s" % (msg, len(opt)))


if __name__ == "__main__":
    # start = time()
    # pool = multiprocessing.Pool(processes=1)
    # for i in range(1):
    #     msg = "process %d" % (i)
    #     pool.apply_async(match_v1_v2_id, (msg, 44133 * i, 44133 * (i + 1)))
    # pool.close()
    # pool.join()
    # print("--------Sub-process all done.-----------")
    # end = time()
    # print("main process run time: %s" % (end - start))
    start = time()

    p1 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 0, 25314431 * 0 + 25314431, 1))
    p2 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 1, 25314431 * 1 + 25314431, 2))
    p3 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 2, 25314431 * 2 + 25314431, 3))
    p4 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 3, 25314431 * 3 + 25314431, 4))
    p5 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 4, 25314431 * 4 + 25314431, 5))
    p6 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 5, 25314431 * 5 + 25314431, 6))
    p7 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 6, 25314431 * 6 + 25314431, 7))
    p8 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 7, 25314431 * 7 + 25314431, 8))
    p9 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 8, 25314431 * 8 + 25314431, 9))
    p10 = multiprocessing.Process(target=match_v1_v2_id, args=(25314431 * 9, 25314431 * 9 + 25314431, 10))

    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p10.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()
    p10.join()

    end = time()
    print("run time: %s" % (end - start))

