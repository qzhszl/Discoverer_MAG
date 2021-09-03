# coding = utf-8
# @Time: 2021/9/3 9:22
# Author: Zhihao Qiu

'''
in data processing, we divide the authors into training set and test set
in this .py, we will verify the collaboration
'''
from utils.connect_to_table import connectTable


def coauthor_with_discoverer_times():
    col_author = connectTable("qiuzh", "researchers0810_trainingset")
    cursor = col_author.find(no_cursor_timeout=True)
    count =0
    operation=[]
    for author in cursor:
        count += 1
        sur = author["sur"]
        author_id = author["_id"]
        if sur >=0 and sur < maxbsur:
            operation.append(pymongo.UpdateOne({"_id": author_id},
                                               {"$set": {"ifdis": 0}}))
        else:
            operation.append(pymongo.UpdateOne({"_id": author_id},
                                               {"$set": {"ifdis": 1}}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
        print("又写入并完成",len(operation))
    cursor.close()


if __name__ == '__main__':
    # add_paper_citation_relation2(1,300,1)
    # divide_researchers_into_2groups()
    # paper_citation_number(1,3,1)
    # author_citation_number(1,3,1)
    # new_pub_count(1,3,1)
    # researchers_con_innewcollection(1,3,1)

    # initialize_discover_number()

    # initialize_surprisal()
    # D = 275434
    # L= 8110703
    # P_D = D/L
    # # author_surprisal(P_D)
    # boot_strap(P_D)
    # find_discoverer(1.9969148477802172)
    col_author = connectTable("qiuzh", "researchers0810_trainingset")
    print(col_author.count_documents({"ifdis": 1}))



    # start = time()
    #
    # p1 = multiprocessing.Process(target=researchers_con_innewcollection,
    #                              args=(111962 * 0, 111962 * 0 + 111962, 1))
    # p2 = multiprocessing.Process(target=researchers_con_innewcollection,
    #                              args=(111962 * 1, 111962 * 1 + 111962, 2))
    # p3 = multiprocessing.Process(target=researchers_con_innewcollection,
    #                              args=(111962 * 2, 111962 * 2 + 111962, 3))
    # p4 = multiprocessing.Process(target=researchers_con_innewcollection,
    #                              args=(111962 * 3, 111962 * 3 + 111962, 4))
    # p5 = multiprocessing.Process(target=researchers_con_innewcollection,
    #                              args=(111962 * 4, 111962 * 4 + 111962, 5))
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
    # print("run time: %s" % (end - start))