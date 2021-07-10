# coding = utf-8
# @Time: 2021/4/7 17:51
# Author: Zhihao Qiu
import multiprocessing
from time import time

import pymongo

from utils.connect_to_table import connectTable


def add_paper_citation_relation(colpaper, begin, end, msg):
    '''
    the paper only contains the information of reference, and we want to know the paper was cited by which paper in which
    year:
    1.the paper was cited n times in total by others.(see the code in dataprocessing/calculate_citation)
    2.the paper was cited by aaaaa-1000000 in 1999 and by bbbbb-1000001 in 1998
    :return:
    {citation_counts:n}
    {citation_list:[{year:1999,id:aaaaa-1000000},year:1998,id:bbbbbb-1000001}]}
    '''
    start_time = time()
    print(start_time)
    # colpaper = connectTable("qiuzh", "filtered_papers")
    count = 0
    cursor = colpaper.find(no_cursor_timeout=True)[begin:end]
    for paper in cursor:
        operation = []
        paper_id = paper["_id"]
        reference_list = paper["references"]
        paper_year = paper["year"]
        for reference in reference_list:
            target_paper = colpaper.find_one({"_id": reference})
            if target_paper:
                operation.append(pymongo.UpdateOne({"_id": reference},
                                                   {"$push": {"was_cited": {"year": paper_year, "paper_id": paper_id}}}))
        # print(len(operation))
        colpaper.bulk_write(operation, ordered=False)
        count += 1
        if count % 10000 == 0:
            print("进程%s已完成%d" % (msg, (count / 10000)))
    cursor.close()
    end_time = time()
    print(msg, end_time - start_time)


def add_paper_citation_relation2(begin, end, msg):
    '''
    this function is appropriate for mag_papers0510
    version1 is too slow
    the paper only contains the information of reference, and we want to know the paper was cited by which paper in which
    year:
    1.the paper was cited n times in total by others.(see the code in dataprocessing/calculate_citation)
    2.the paper was cited by aaaaa-1000000 in 1999 and by bbbbb-1000001 in 1998
    :return:
    {citation_counts:n}
    {citation_list:[{year:1999,id:aaaaa-1000000},{year:1998,id:bbbbbb-1000001}}]}
    '''
    start_time = time()
    print(start_time)

    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_citation_network = connectTable("qiuzh", "citation_network0515")

    count = 0
    operation = []
    cursor = colpaper.find(no_cursor_timeout=True)[begin:end]
    for paper in cursor:
        count+=1
        paper_id = paper["_id"]
        citation_list = []
        cite_papers = colpaper.find({"references": paper_id})
        papercount=0
        for cite_paper in cite_papers:
            papercount += 1
            citation_list.append({"cpid": cite_paper["_id"], "cpyear": cite_paper["year"]})
            if papercount == 2000:
                operation.append(pymongo.InsertOne({"id": paper_id, "citation": citation_list}))
                citation_list=[]
                papercount=0
        if citation_list:
            operation.append(pymongo.InsertOne({"id": paper_id, "citation": citation_list}))

        if count % 10000 == 0:
            print(msg,"已处理:", count / 10000, flush=True)
            col_citation_network.bulk_write(operation, ordered=False)
            print(msg,"已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_citation_network.bulk_write(operation, ordered=False)
    cursor.close()
    end_time = time()
    print(msg, end_time - start_time)


def add_citation_relation(colpaper,colauthor,begin,end,msg):
    '''
    citation times and coauthor relationships
    author-new_pubs-paper-citation_list-paper-author
    1.the author was cited n times in total by others.
    2.the author was cited by 1000000 in 1999 and by 1000001 in 1998
    :return:
    {citation_counts:n}
    {citation_list:[{year:1999,id:1000000},year:1998,id:1000001}]}
    '''
    start_time = time()
    print(start_time)
    operation = []
    cursor = colauthor.find(no_cursor_timeout=True)[begin:end]
    for i in cursor:
        cited_author_id = i["_id"]
        # citation_times = 0
        citation_list = []
        papers = i["new_pubs"]
        for paper in papers:
            paper_details = colpaper.find_one({"_id": paper})
            cite_papers = paper_details["was_cited"]
            for cite_paper in cite_papers:
                cite_paper_id = cite_paper["paper_id"]
                cite_year = cite_paper["paper_year"]
                cite_authors = colpaper.find_one({"_id":cite_paper_id})
                for cite_author in cite_authors:
                    citation_list.append({"author_id": cite_author["id"], "cite_time": cite_year})
        operation.append(pymongo.UpdateOne({"_id": cited_author_id},
                                            {"$set": {"cite_author": citation_list}}))
    colauthor.bulk_write(operation, ordered=False)
    cursor.close()
    print(time(), (time() - start_time))


if __name__ == '__main__':
    # col1 = connectTable("qiuzh", "mag_papers0510")
    # col_citation = connectTable("qiuzh", "citation_network0515")
    # add_paper_citation_relation2(col1,col_citation,1,5,1)

    # colpaper = connectTable("qiuzh", "mag_papers0415")
    # colauthor = connectTable("qiuzh", "mag_authors0411")
    start = time()

    p1 = multiprocessing.Process(target=add_paper_citation_relation2,
                                 args=(4250601 * 0, 4250601 * 0 + 4250601, 1))
    p2 = multiprocessing.Process(target=add_paper_citation_relation2,
                                 args=(4250601 * 1, 4250601 * 1 + 4250601, 2))
    p3 = multiprocessing.Process(target=add_paper_citation_relation2,
                                 args=(4250601 * 2, 4250601 * 2 + 4250601, 3))
    p4 = multiprocessing.Process(target=add_paper_citation_relation2,
                                 args=(4250601 * 3, 4250601 * 3 + 4250601, 4))
    p5 = multiprocessing.Process(target=add_paper_citation_relation2,
                                 args=(4250601 * 4, 4250601 * 4 + 4250601, 5))

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
    print("run time: %s" % (end - start))


