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


def researher_first_year(msg, begin, end):
    col1 = connectTable("oga_one", "mag_paper_plus2")
    col2 = connectTable('qiuzh', "MAG_authors")
    opt = []
    for i in col2.find()[begin: end]:
        researher_id = i.get("id")
        papers_list = i.get("new_pubs")
        year_list=[]
        for paper_id in papers_list:
            year_list.append(col1.find({"id": paper_id}))
            opt.append(pymongo.UpdateOne({"id": researher_id},
                                         {"$set": {"paper_year": year_list, "first_year": min(year_list),
                                                   "last_year": max(year_list)}}
                                         ))
    col2.bulk_write(opt, ordered=False)
    print("线程： %s, 遍历了 %s" % (msg, len(opt)))


if __name__ == '__main__':
    start = time()

    p1 = multiprocessing.Process(target=researher_first_year, args=(441324 * 0, 441324 * 0 + 441324, 1))
    p2 = multiprocessing.Process(target=researher_first_year, args=(441324 * 1, 441324 * 1 + 441324, 2))
    p3 = multiprocessing.Process(target=researher_first_year, args=(441324 * 2, 441324 * 2 + 441324, 3))
    p4 = multiprocessing.Process(target=researher_first_year, args=(441324 * 3, 441324 * 3 + 441324, 4))
    p5 = multiprocessing.Process(target=researher_first_year, args=(441324 * 4, 441324 * 4 + 441324, 5))
    p6 = multiprocessing.Process(target=researher_first_year, args=(441324 * 5, 441324 * 5 + 441324, 6))
    p7 = multiprocessing.Process(target=researher_first_year, args=(441324 * 6, 441324 * 6 + 441324, 7))
    p8 = multiprocessing.Process(target=researher_first_year, args=(441324 * 7, 441324 * 7 + 441324, 8))
    p9 = multiprocessing.Process(target=researher_first_year, args=(441324 * 8, 441324 * 8 + 441324, 9))
    p10 = multiprocessing.Process(target=researher_first_year, args=(441324 * 9, 441324 * 9 + 441324, 10))

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
