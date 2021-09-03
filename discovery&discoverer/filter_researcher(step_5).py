# coding = utf-8
# @Time: 2021/7/7 16:45
# Author: Zhihao Qiu

'''
In the last step, we have identified discoverers successfully. However, not all the authors in the dataset can be seen as
a researchers -- they may have only one publication the whole life. And some of the papers has thousands of authors, which
against the conventional meaning of cooperation. Therefore, we want to filter the data further more.
We use function in the dataprocessing/step1
'''
import multiprocessing
from time import time

import pymongo
from _ast import Bytes

from utils.connect_to_table import connectTable


def filter_researcher_by_pn_and_careerlife():
    '''
    :param msg:
    :param begin:
    :param end:
    :return: pubs>=10, academic career life >=10
    '''
    # col2 = connectTable('qiuzh', "mag_researchers0707")
    # col2.drop()
    col1 = connectTable('qiuzh', "mag_authors0510")
    col2 = connectTable('qiuzh', "mag_researchers0707")
    cursor = col1.find({"pub_count": {"$gte": 10}}, no_cursor_timeout=True)
    opt = []
    count=0
    print(cursor.count())
    for i in cursor:
        if i["last_year"] - i["first_year"] >= 10:
            count += 1
            opt.append(pymongo.InsertOne({"_id": i["_id"], "new_pubs": i["new_pubs"], "pub_count": i["pub_count"],
                                          "first_year": i["first_year"], "last_year": i["last_year"], "cn": i["cn"]}))
            if count % 10000 == 0:
                print(len(opt))
                print(count)
                print("已处理:", count / 10000, flush=True)
                col2.bulk_write(opt, ordered=False)
                print("已写入:", count / 10000, flush=True)
                opt = []
    if opt:
        col2.bulk_write(opt, ordered=False)
        print("最终又完成", len(opt))
    print(count)
    cursor.close()


def filter_researchers_paper_by_authors():
    '''
    from mag_researchers0707(pubs>=10, academic career life >=10) to mag_researchers0810(only the author number of a
    paper less than 10 will be considered in the dataset)
    :param msg:
    :param begin:
    :param end:
    :return:
    this function is created in 2021.8.10
    '''
    # col2 = connectTable('qiuzh', "mag_researchers0707")
    # col2.drop()
    col1 = connectTable('qiuzh', "mag_researchers0707")
    col2 = connectTable('qiuzh', "mag_researchers0810")
    col_paper = connectTable("qiuzh", "mag_papers0510")
    cursor = col1.find(no_cursor_timeout=True)
    opt = []
    count=0
    print(cursor.count())
    for i in cursor:
        count += 1
        pubs = i["new_pubs"]
        new_pubs = []
        for pub in pubs:
            paper = col_paper.find_one({"_id":pub["pid"]})
            if len(paper["authors"])<=10:
                new_pubs.append(pub)
        opt.append(pymongo.InsertOne({"_id": i["_id"], "new_pubs": new_pubs, "pub_count": i["pub_count"],
                                      "first_year": i["first_year"], "last_year": i["last_year"], "cn": i["cn"]}))
        if count % 10000 == 0:
            print(len(opt))
            print(count)
            print("已处理:", count / 10000, flush=True)
            col2.bulk_write(opt, ordered=False)
            print("已写入:", count / 10000, flush=True)
            opt = []
    if opt:
        col2.bulk_write(opt, ordered=False)
        print("最终又完成", len(opt))
    print(count)
    cursor.close()


'''
We got 1082922 researchers in total
then we go back to data_analysis/data_analysis.py to calculate con,dn,and to
step 1--3 to identify discoverers
'''
# con：coauthor_number

def researchers_con():
    '''
    the coauthor times based on the mag_authors0510
    :return:
    '''
    col1 = connectTable('qiuzh', "mag_authors0510")
    col2 = connectTable('qiuzh', "mag_researchers0707")
    count = 0
    operation = []
    cursor = col2.find(no_cursor_timeout=True)
    for author in cursor:
        count += 1
        author_id = author["_id"]
        coauthor_number = col1.find_one({"_id":author_id})["con"]

        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"con": coauthor_number}}))

        if count % 10000 == 0:
            print( "已处理:", count / 10000, flush=True)
            col2.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
    if operation:
        col2.bulk_write(operation, ordered=False)
        print("又处理",len(operation))
    cursor.close()


def researchers_con_innewcollection(begin,end,msg):

    '''
    this function aim to find the co-author number in the new collection, i.e. for researcher i, only one of his coauthor
    in the same collection will be regarded as coauthor.
    i.e. The coauthor relationship in mag_researchers0707 is different from the one in the mag_authors0510.
    :return:
    in 2021.8.11 we used this function in mag_researchers0810
    '''

    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "mag_researchers0810")

    count = 0
    operation = []
    cursor = col_author.find(no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        coauthor_number = 0
        for paper in author["new_pubs"]:
            p = colpaper.find_one({"_id": paper["pid"]})
            for p_author in p["authors"]:
                if p_author["id"] != author_id and col_author.find_one({"_id":p_author["id"]},no_cursor_timeout=True):
                    coauthor_number += 1

        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"new_con": coauthor_number}}))

        if count % 1000 == 0:
            print(msg, "已处理:", count / 1000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 1000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
        print("msg共完成:",count)
    cursor.close()


def researchers_collaboration_network():
    '''
    there are some problems in researchers_con_innewcollection network, so we may use the other method to replace it
    i.e create a collaboration network first.
    :param begin:
    :param end:
    :param msg:
    :return:
    '''

    start_time = time()
    print(start_time, flush=True)
    col1 = connectTable("qiuzh", "mag_papers0510")
    col2 = connectTable("qiuzh", "mag_researchers0707")
    col3 = connectTable("qiuzh", "coauthor_network0722")
    operation = []
    cursor = col2.find(no_cursor_timeout=True)
    count = 0
    for i in cursor:
        count += 1
        author_id = i["_id"]
        # coauthor_times = 0
        # coauthor_list = []
        papers = i["new_pubs"]
        for paper in papers:
            paper_details = col1.find_one({"_id": paper},no_cursor_timeout=True)
            for author in paper_details["authors"]:
                if author["id"] != author_id and col2.find_one({"_id":author["id"]},no_cursor_timeout=True):
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
    print("已完成", len(operation), flush=True)
    print(time(), (time() - start_time), flush=True)





if __name__ == '__main__':
    # researchers_con_innewcollection(1,3,1)
    # 
    start = time()
    # researchers_con_innewcollection(360975 * 1+217000, 360975 * 1 + 360975, 1)
    p1 = multiprocessing.Process(target=researchers_con_innewcollection,
                                 args=(216585 * 0, 216585 * 0 + 216585, 1))
    p2 = multiprocessing.Process(target=researchers_con_innewcollection,
                                 args=(216585 * 1, 216585 * 1 + 216585, 2))
    p3 = multiprocessing.Process(target=researchers_con_innewcollection,
                                 args=(216585 * 2, 216585 * 2 + 216585, 3))
    p4 = multiprocessing.Process(target=researchers_con_innewcollection,
                                 args=(216585 * 3, 216585 * 3 + 216585, 4))
    p5 = multiprocessing.Process(target=researchers_con_innewcollection,
                                 args=(216585 * 4, 216585 * 4 + 216585, 5))

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
    print("run time: %s" % ((end - start)/60))

    # start = time()
    # # researchers_con_innewcollection(360975 * 1+208000, 360975 * 1 + 360975, 1)
    # p1 = multiprocessing.Process(target=researchers_collaboration_network,
    #                              args=(216585 * 0, 216585 * 0 + 216585, 1))
    # p2 = multiprocessing.Process(target=researchers_collaboration_network,
    #                              args=(216585 * 1, 216585 * 1 + 216585, 2))
    # p3 = multiprocessing.Process(target=researchers_collaboration_network,
    #                              args=(216585 * 2, 216585 * 2 + 216585, 3))
    # p4 = multiprocessing.Process(target=researchers_collaboration_network,
    #                              args=(216585 * 3, 216585 * 3 + 216585, 4))
    # p5 = multiprocessing.Process(target=researchers_collaboration_network,
    #                              args=(216585 * 4, 216585 * 4 + 216585, 5))
    #
    # p1.start()
    # p2.start()
    # p3.start()
    # p4.start()
    # p5.start()
    #
    # p1.join()
    # p2.join()
    # p3.join()
    # p4.join()
    # p5.join()
    #
    # end = time()
    # print("run time: %s" % ((end - start)/60))

    # researchers_con_innewcollection(1,3,1)
    # filter_researchers_paper_by_authors()