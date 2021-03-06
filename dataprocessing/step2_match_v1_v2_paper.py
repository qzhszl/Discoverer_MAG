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
    coll = connectTable("oga_one", "mag_paper_plus2")
    coll3 = connectTable('qiuzh', "MAG_authors")
    opt = []
    count = 0
    cursor = coll3.find(no_cursor_timeout=True)[begin:end]
    for i in cursor:
    # for i in coll3.find():
        v2author_id = i.get("id")
        # print(v2author_id)
        new_pubs = []
        papers = coll.find({"new_authors.id": v2author_id})
        for paper in papers:
            id = paper.get("id")
            new_pubs.append(id)
        opt.append(pymongo.UpdateOne({"id": v2author_id},
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

    p1 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 0, 441324 * 0 + 441324, 1))
    p2 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 1, 441324 * 1 + 441324, 2))
    p3 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 2, 441324 * 2 + 441324, 3))
    p4 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 3, 441324 * 3 + 441324, 4))
    p5 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 4, 441324 * 4 + 441324, 5))
    p6 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 5, 441324 * 5 + 441324, 6))
    p7 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 6, 441324 * 6 + 441324, 7))
    p8 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 7, 441324 * 7 + 441324, 8))
    p9 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 8, 441324 * 8 + 441324, 9))
    p10 = multiprocessing.Process(target=match_v1_v2_id, args=(441324 * 9, 441324 * 9 + 441324, 10))

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

