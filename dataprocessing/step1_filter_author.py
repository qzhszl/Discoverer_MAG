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


def filter_author_by_publication():
    '''
    :param msg:
    :param begin:
    :param end:
    :return: pubs>=10, org exist(affiliation)
    '''
    col2 = connectTable('qiuzh', "MAG_authors")
    result = col2.delete_many({"$or": [{"n_pubs": {"$not": {"$gte": 10}}}, {"$and": [{"org": None}, {"orgs": None}]}]})
    print(result.deleted_count)  # 被删除的个数


def filter_author_by_careerlife(begin,end,msg):
    '''
    :param msg:
    :param begin:
    :param end:
    :return: pubs>=10, org exist(affiliation)
    '''
    col1 = connectTable('qiuzh', "mag_authors0421")
    col2 = connectTable('qiuzh', "mag_authors0411")
    cursor = col2.find(no_cursor_timeout=True)[begin:end]
    opt =[]
    for i in cursor:
        if i["first_year"]-i["last_year"]>=20:
            opt.append(pymongo.InsertOne({"_id":i["_id"],"new_pubs":i["new_pubs"],"pub_count":i["pub_count"],"first_year":i["first_year"],"last_year":i["last_year"]}))
    col1.bulk_write(opt,ordered=False)
    cursor.close()


def filter_authors_by_new_pubs():
    '''
    save authors who have new pubs in mag_authors0411
    :return:
    '''
    col1 = connectTable("qiuzh", "mag_authors0409")
    col2 = connectTable('qiuzh', "mag_authors0411")

    for i in col1.find():
        if i["new_pubs"]:
            col2.insert_one(i)


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


def filter_author_by_abstract():
    '''
    Some of the papers in the dataset are news and some of the authors are editor of journals, so we need to filter them by abstract
    :return:
    '''
    col1 = connectTable("qiuzh", "mag_papers0415")
    print("okay")


def filter_authors_by_papers():
    '''
    save authors who have new pubs in mag_authors0411
    :return:
    '''
    col1 = connectTable("qiuzh", "mag_authors0409")
    col2 = connectTable('qiuzh', "mag_authors0411")

    for i in col1.find():
        if i["new_pubs"]:
            col2.insert_one(i)


def filter_authors_by_papers2(begin, end, msg):
    '''
    from version 0411-->0510
    all the authors' publication must in the mag_papers0510
    _id-->_id
    new_pubs-->new_pubs
    :return:
    '''
    col1 = connectTable("qiuzh", "mag_authors0411")
    col2 = connectTable('qiuzh', "mag_authors0510")
    col3 = connectTable('qiuzh', "mag_papers0510")
    opt = []
    count = 0
    cursor = col1.find(no_cursor_timeout=True)[begin:end]
    for i in cursor:
        count += 1
        author_id = i["_id"]
        # print(v2author_id)
        new_pubs = []
        papers = i["new_pubs"]
        for paper_id in papers:
            paper = col3.find_one({"_id": paper_id})
            if paper:
                new_pubs.append({"pid": paper_id, "year": paper["year"]})
        if new_pubs:
            opt.append(pymongo.InsertOne({"_id": author_id, "new_pubs": new_pubs}))
        if count % 10000 == 0:
            print(msg,"已处理:", count / 10000, flush=True)
            col2.bulk_write(opt, ordered=False)
            print(msg,"已写入:", count / 10000, flush=True)
            opt = []
            print(time(), flush=True)
    if opt:
        col2.bulk_write(opt, ordered=False)
    cursor.close()

    print("线程： %s, 遍历了 %s" % (msg, len(opt)))


def filter_authors_by_papers2_patch(begin, end, msg):
    '''
    from version 0411-->0510
    all the authors' publication must in the mag_papers0510
    _id-->_id
    new_pubs-->new_pubs
    :return:
    '''
    col1 = connectTable("qiuzh", "mag_authors0411")
    col2 = connectTable('qiuzh', "mag_authors0510")
    col3 = connectTable('qiuzh', "mag_papers0510")
    opt = []
    count = 0
    cursor = col1.find(no_cursor_timeout=True)[begin:end]
    for i in cursor:
        count += 1
        author_id = i["_id"]
        if not col2.find_one({"_id":author_id}):
            # print(v2author_id)
            new_pubs = []
            papers = i["new_pubs"]
            for paper_id in papers:
                paper = col3.find_one({"_id": paper_id})
                if paper:
                    new_pubs.append({"pid": paper_id, "year": paper["year"]})
            if new_pubs:
                opt.append(pymongo.InsertOne({"_id": author_id, "new_pubs": new_pubs}))
        # if count % 10000 == 0:
        #     print(msg,"已处理:", count / 10000, flush=True)
        #     col2.bulk_write(opt, ordered=False)
        #     print(msg,"已写入:", count / 10000, flush=True)
        #     opt = []
        #     print(time(), flush=True)
    if opt:
        col2.bulk_write(opt, ordered=False)
    cursor.close()

    print("线程： %s, 遍历了 %s" % (msg, len(opt)))


def delete_new_pubs(begin, end, msg):
    """
    :param begin:
    :param end:
    :param msg:
    :return:
    """
    col = connectTable('qiuzh', "MAG_authors")
    cursor = col.find({"new_pubs":{"$exists": True}},no_cursor_timeout=True)[begin:end]
    for i in cursor:
        _id = i.get("_id")
        print(_id)
        col.update_one({"_id":_id}, {"$unset": {"new_pubs": 1}},False,True)
    cursor.close()
    print("yes okay")


def delete_coauthor_counts():
    col = connectTable('qiuzh', "mag_authors0411")
    # cursor = col.find({"coauthor_counts":{"$exists": True}},no_cursor_timeout=True)[begin:end]
    # for i in cursor:
    #     _id = i.get("_id")
    #     col.update_one({"_id":_id}, {"$unset": {"new_pubs": 1}},False,True)
    col.update_many({"coauthor_counts": {"$exists": True}}, {"$unset": {"coauthor_counts": 1, "coauthor": 1}})
    # cursor.close()
    print("yes okay")



# papers:


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


def filter_papers_by_new_pubs2():
    '''
    把col1中的paper过滤到col3中，所有的paper必须都在author中出现

    :return:
    '''
    col1 = connectTable("qiuzh", "mag_papers")
    col2 = connectTable('qiuzh', "mag_authors0411")
    col3 = connectTable("qiuzh", "mag_papers0415")
    medset =set()
    for i in col2.find():
        for j in i.get("new_pubs"):
            medset.add(j)
            if len(medset)%1000000==0:
                print("已完成%d万条"%(len(medset)/1000000))
    print(len(medset))
    medset = list(medset)

    for paper_id in medset:
        paper = col1.find_one({"_id":paper_id})
        col3.insert_one(paper)


def filter_papers_by_JCR():
    '''
    把col1中的paper过滤到col2中，所有的paper的期刊必须都在JCR中出现过，即有field字段
    :return:
    '''
    col1 = connectTable("qiuzh", "mag_papers0415")
    col2 = connectTable("qiuzh", "mag_papers0510")
    cursor = col1.find({"field": {"$exists": True}}, no_cursor_timeout=True)
    for i in cursor:
        col2.insert_one(i)


def filter_paper_by_reference():
    '''
    :param msg:
    :param begin:
    :param end:
    :return: pubs>=10, org exist(affiliation)
    '''
    col2 = connectTable('qiuzh', "filtered_papers")
    result = col2.delete_many({"references": {"$exists": False}})
    print(result.deleted_count)  # 被删除的个数


if __name__ == "__main__":
    '''
    multipro 
    '''
    start = time()
    filter_authors_by_papers2_patch(10265213 * 2+9900000, 10265213 * 2 + 9910000,1)

    # p1 = multiprocessing.Process(target=filter_authors_by_papers2, args=(10265213 * 0+2800000, 10265213 * 0 + 10265213, 1))
    # p2 = multiprocessing.Process(target=filter_authors_by_papers2, args=(10265213 * 1+8770000, 10265213 * 1 + 10265213, 2))
    # p3 = multiprocessing.Process(target=filter_authors_by_papers2_patch, args=(10265213 * 2+9900000, 10265213 * 2 + 9910000, 3))
    # p4 = multiprocessing.Process(target=filter_authors_by_papers2, args=(10265213 * 3+7780000, 10265213 * 3 + 10265213, 4))
    # p5 = multiprocessing.Process(target=filter_authors_by_papers2, args=(10265213 * 4+9670000, 10265213 * 4 + 10265213, 5))
    # p6 = multiprocessing.Process(target=filter_authors_by_papers2, args=(5132607 * 5, 5132607 * 5 + 5132607, 6))
    # p7 = multiprocessing.Process(target=filter_authors_by_papers2, args=(5132607 * 6, 5132607 * 6 + 5132607, 7))
    # p8 = multiprocessing.Process(target=filter_authors_by_papers2, args=(5132607 * 7, 5132607 * 7 + 5132607, 8))
    # p9 = multiprocessing.Process(target=filter_authors_by_papers2, args=(5132607 * 8, 5132607 * 8 + 5132607, 9))
    # p10 = multiprocessing.Process(target=filter_authors_by_papers2, args=(5132607 * 9, 5132607 * 9 + 5132607, 10))

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
    print("run time: %s" % ((end - start)/60))

    '''
    single process
    '''
    # start_time = time()
    # filter_papers_by_JCR()
    # end_time = time()
    # print("run times: %s" % ((end_time - start_time)/3600))
