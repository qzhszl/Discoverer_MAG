# coding = utf-8
# @Time: 2021/7/7 16:45
# Author: Zhihao Qiu

'''
In the last step, we have identified discoverers successfully. However, not all the authors in the dataset can be seen as
a researchers -- they may have only one publication the whole life. And some of the papers has thousands of authors, which
against the conventional meaning of cooperation. Therefore, we want to filter the data further more.
We use function in the dataprocessing/step1
'''
import pymongo

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


'''
We got 1082922 researchers
then we go back to data_analysis.py to calculate con,dn,and to
step 1--3 to identify discoverers
'''
#con


def researchers_con():
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
    :return:
    '''

    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "mag_researchers0707")

    count = 0
    operation = []
    cursor = col_author.find(no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        coauthor_number = 0
        for paper in author["new_pubs"]:
            p = colpaper.find_one({"_id": paper["pid"]}, no_cursor_timeout=True)
            coauthor_number += (len(p["authors"]) - 1)

        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"con": coauthor_number}}))

        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    cursor.close()


if __name__ == '__main__':
    researchers_con()


