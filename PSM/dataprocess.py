# coding = utf-8
# @Time: 2021/8/31 19:04
# Author: Zhihao Qiu
import math
import multiprocessing
import numpy as np
from time import time

import pymongo
from scipy import stats

from utils.connect_to_table import connectTable


def find_critical_year():
    '''
        2020.8.31: critical_year is 1996, with 559808 rexearchers in total(more than half of the dataset)
        :return:
        '''

    col_author = connectTable("qiuzh", "mag_researchers0810")
    year_list = [1802, 1803, 1810, 1814, 1815, 1816, 1819, 1823, 1825, 1827, 1828, 1829, 1830, 1832, 1833, 1834, 1836,
                 1838,
                 1839, 1841, 1842, 1843, 1844, 1845, 1846, 1847, 1848, 1849, 1850, 1851, 1852, 1853, 1854, 1855, 1856,
                 1857,
                 1858, 1859, 1860, 1861, 1862, 1863, 1864, 1865, 1866, 1867, 1868, 1869, 1870, 1871, 1872, 1873, 1874,
                 1875,
                 1876, 1877, 1878, 1879, 1880, 1881, 1882, 1883, 1884, 1885, 1886, 1887, 1888, 1889, 1890, 1891, 1892,
                 1893,
                 1894, 1895, 1896, 1897, 1898, 1899, 1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910,
                 1911,
                 1912, 1913, 1914, 1915, 1916, 1917, 1918, 1919, 1920, 1921, 1922, 1923, 1924, 1925, 1926, 1927, 1928,
                 1929,
                 1930, 1931, 1932, 1933, 1934, 1935, 1936, 1937, 1938, 1939, 1940, 1941, 1942, 1943, 1944, 1945, 1946,
                 1947,
                 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964,
                 1965,
                 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982,
                 1983,
                 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000,
                 2001,
                 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
    # year_list = [1957, 1987]
    sum = 0
    for year in year_list:
        researcher_number = col_author.count_documents({"first_year": year})
        sum += researcher_number
        print(researcher_number, sum)
        if sum >= 541461:
            print(year)


def divide_researchers_into_2groups():
    col_author = connectTable("qiuzh", "mag_researchers0810")
    col1 = connectTable("qiuzh", "researchers0810_trainingset")
    col2 = connectTable('qiuzh', "researchers0810_testset")
    opt1 = []
    opt2 = []
    count = 0
    cursor = col_author.find(no_cursor_timeout=True)
    for researcher in col_author.find():
        count +=1
        if researcher["first_year"] <=1996:
            opt1.append(pymongo.InsertOne(researcher))
        else:
            opt2.append(pymongo.InsertOne(researcher))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col1.bulk_write(opt1, ordered=False)
            print("已写入:", len(opt1), flush=True)
            col2.bulk_write(opt2, ordered=False)
            print("已写入:", len(opt2), flush=True)
            opt1 = []
            opt2 = []
    if opt1:
        col1.bulk_write(opt1, ordered=False)
        print("又写入:", len(opt1), flush=True)
    if opt2:
        col2.bulk_write(opt2, ordered=False)
        print("又写入:", len(opt2), flush=True)
    cursor.close()


# relation_in_trainingset():
def add_paper_citation_relation2(begin, end, msg):
    '''
    this function is appropriate for mag_papers0510
    version1 is too slow
    the paper only contains the information of reference, and we want to know the paper was cited by which paper in which
    year:
    1.the paper xxxxxxxxxxxxxx was cited n times in total by others.(see the code in dataprocessing/calculate_citation)
    2.the paper xxxxxxxxxxxxxx was cited by aaaaa-1000000 in 1999 and by bbbbb-1000001 in 1998
    :return:
    paperid:xxxxxxxxxxxxxx
    {citation_list:[{cpyear:1999,cpid:aaaaa-1000000},{cpyear:1998,cpid:bbbbbb-1000001}}]}

    2021.8.31, this function is used to recalculate cn in citation network0810 trainingset
    attention: there maybe a little problem for the citation, because this citation is calculated by all the papers in 0810
    not 0510.
    '''
    start_time = time()
    print(start_time)

    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_citation_network = connectTable("qiuzh", "citation_network0810_trainingset")

    count = 0
    operation = []
    cursor = colpaper.find(no_cursor_timeout=True)[begin:end]
    for paper in cursor:
        count+=1
        if paper["year"]<=1996:
            paper_id = paper["_id"]
            citation_list = []
            cite_papers = colpaper.find({"references": paper_id})
            papercount=0
            for cite_paper in cite_papers:
                if cite_paper["year"]<=1996:
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


def paper_citation_number(begin, end, msg):
    '''
    this function is appropriate for citation_network0515 and mag_papers0510
    :return: add each papers' total citation in mag_papers0510
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_citation_network = connectTable("qiuzh", "citation_network0810_trainingset")

    count = 0
    operation = []
    cursor = colpaper.find(no_cursor_timeout=True)[begin:end]
    for paper in cursor:
        count += 1
        paper_id = paper["_id"]
        citation_number = 0
        paper_citation_relations = col_citation_network.find({"id": paper_id}, no_cursor_timeout=True)
        if paper_citation_relations:
            for paper_citation_relation in paper_citation_relations:
                citation_number += len(paper_citation_relation["citation"])
        operation.append(pymongo.UpdateOne({"_id": paper_id}, {"$set": {"cn_before1996": citation_number}}))
        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            colpaper.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        colpaper.bulk_write(operation, ordered=False)
    cursor.close()


def author_citation_number(begin, end, msg):
    '''
    this function is appropriate for researchers0810_trainingset and citation network0515
    this function is used in 2021.8.31 to recalculate cn in training set.
    :return:
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "researchers0810_trainingset")

    count = 0
    operation = []
    cursor = col_author.find(no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        citation_number = 0
        for paper in author["new_pubs"]:
            if paper["year"]<=1996:
                p = colpaper.find_one({"_id": paper["pid"]}, no_cursor_timeout=True)
                citation_number += p["cn_before1996"]

        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"cn": citation_number}}))

        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    cursor.close()


def new_pub_count(begin, end, msg):
    col_author = connectTable("qiuzh", "researchers0810_trainingset")
    count = 0
    operation = []
    cursor = col_author.find(no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        pub_count = 0
        for paper in author["new_pubs"]:
            if paper["year"] <= 1996:
                pub_count += 1
        operation.append(pymongo.UpdateOne({"_id": author_id}, {"$set": {"pub_count": pub_count}}))

        if count % 10000 == 0:
            print(msg, "已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print(msg, "已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    cursor.close()

# we use data_analysis.top_scientist to calculate if top


def researchers_con_innewcollection(begin,end,msg):

    '''
    this function aim to find the co-author number in the new collection, i.e. for researcher i, only one of his coauthor
    in the same collection will be regarded as coauthor.
    i.e. The coauthor relationship in mag_researchers0707 is different from the one in the mag_authors0510.
    :return:
    in 2021.9.1 we used this function in researchers0810_trainingset
    '''

    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "researchers0810_trainingset")

    count = 0
    operation = []
    cursor = col_author.find(no_cursor_timeout=True)[begin:end]
    for author in cursor:
        count += 1
        author_id = author["_id"]
        coauthor_number = 0
        for paper in author["new_pubs"]:
            if paper["year"] <= 1996:
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


def author_be_discovered():
    '''
    this function is appropriate for mag_authors0510 and citation network0515
    authors are discovered by who
    2700636 top researchers in total
    relationships are saved in the be_discovered0622
    :return:

    in 2021.9.1 we used this function in researchers0810_trainingset
    '''
    colpaper = connectTable("qiuzh", "mag_papers0510")
    col_author = connectTable("qiuzh", "researchers0810_trainingset")
    col_be_discovered = connectTable("qiuzh", "be_discovered0810_trainingset")

    count = 0
    operation = []

    cursor_count = col_author.count_documents({"iftop": 1})
    print(cursor_count)
    cursor = col_author.find({"iftop":1},no_cursor_timeout=True)
    for author in cursor:
        count += 1
        author_id = author["_id"]
        author_year = author["first_year"]
        discovery_list =[]
        aucount = 0
        for paper in author["new_pubs"]:
            if paper["year"] - author_year <= 3 and paper["year"]<=1996:
                p = colpaper.find_one({"_id": paper["pid"]}, no_cursor_timeout=True)
                p_authors = p["authors"]
                for au in p_authors:
                    if not au["id"] == author_id:
                        aucount += 1
                        discovery_list.append(au["id"])
                        if aucount == 2000:
                            operation.append(pymongo.InsertOne({"id": author_id, "be_discovered": discovery_list}))
                            discovery_list = []
                            aucount = 0
        if discovery_list:
            operation.append(pymongo.InsertOne({"id": author_id, "be_discovered": discovery_list}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_be_discovered.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_be_discovered.bulk_write(operation, ordered=False)
    print(count)
    cursor.close()


def initialize_discover_number():
    '''
    this function is used in 2021.8.12 in mag_researchers0810
    in 2021.9.1 we used this function in researchers0810_trainingset
    :return:
    '''
    col_author = connectTable("qiuzh", "researchers0810_trainingset")

    cursor = col_author.find(no_cursor_timeout=True)
    # researcher_number = cursor.count()
    # print(researcher_number)
    count = 0
    operation = []
    for author in cursor:
        count += 1
        operation.append(pymongo.UpdateOne({"_id": author["_id"]}, {"$set": {"dn": -1}}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
    if operation:
        col_author.bulk_write(operation, ordered=False)
    print("finished")
    cursor.close()
    print(count)
    print(col_author.find({"dn":-1},no_cursor_timeout=True).count())


def author_discover_number():
    '''
    this function is appropriate for mag_authors0510 and be_discovered0521
    How many times do an author discover others in his career life.
    :return:
    this function is used in 2021.9.2 in mag_researchers0810
    in 2021.9.1 we used this function in researchers0810_trainingset
    '''

    col_author = connectTable("qiuzh", "researchers0810_trainingset")
    col_be_discovered = connectTable("qiuzh", "be_discovered0810_trainingset")

    count = 0
    operation = []

    # cursor_count = col_author.find({"iftop": 1}, no_cursor_timeout=True).count()
    # print(cursor_count)
    cursor = col_author.find(no_cursor_timeout=True)
    for author in cursor:
        count += 1
        author_id = author["_id"]
        discovery_time = col_be_discovered.find({"be_discovered":author_id},no_cursor_timeout=True).count()
        operation.append(pymongo.UpdateOne({"_id": author_id},
                                                {"$set": {"dn": discovery_time}}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
    print(count)
    print(col_author.count_documents({"dn":-1}))
    cursor.close()


# we use function in discovery&discoverer to calculate surprisal

def initialize_surprisal():
    col_author = connectTable("qiuzh", "researchers0810_trainingset")

    cursor = col_author.find(no_cursor_timeout=True)
    # researcher_number = cursor.count()
    # print(researcher_number)
    count = 0
    operation = []
    for author in cursor:
        count += 1
        operation.append(pymongo.UpdateOne({"_id": author["_id"]}, {"$set": {"sur": -6, "bsur": -6}}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
    if operation:
        col_author.bulk_write(operation, ordered=False)
    print("finished")
    cursor.close()
    print(count)
    print(col_author.find({"dn":-1},no_cursor_timeout=True).count())


def author_surprisal(P_d):
    '''
    this function is appropriate for mag_authors0510
    How many times do an author discover others in his career life.
    :return:
    '''
    col_author = connectTable("qiuzh", "researchers0810_trainingset")
    count = 0
    operation = []

    # cursor_count = col_author.find({"iftop": 1}, no_cursor_timeout=True).count()
    # print(cursor_count)
    cursor = col_author.find(no_cursor_timeout=True)
    for author in cursor:
        count += 1
        author_id = author["_id"]
        d_i = author["dn"]
        k_i = author["new_con"]
        P = stats.binom.sf(d_i - 1, k_i, P_d)
        if P == 0:
            surprisal = -1
        else:
            surprisal = -math.log(P)
        operation.append(pymongo.UpdateOne({"_id": author_id},
                                                {"$set": {"sur": surprisal}}))

        if count % 10000 == 0:
            print( "已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
            print(time(), flush=True)
    if operation:
        col_author.bulk_write(operation, ordered=False)
        print("最终又完成",len(operation))
    cursor.close()
    print(col_author.count_documents({"sur": -6}))
    print(col_author.count_documents({"dn": -1}))


def boot_strap(P_d):
    col_author = connectTable("qiuzh", "researchers0810_trainingset")
    cursor = col_author.find(no_cursor_timeout=True)
    count =0
    operation=[]
    for author in cursor:
        count += 1
        coauthor_times = author["new_con"]
        author_id = author["_id"]
        d_i_list = np.random.binomial(coauthor_times, P_d, 20)
        surprisal_list =[]
        for di in d_i_list:
            P0 = stats.binom.sf(di - 1, coauthor_times, P_d)
            surprisal_list.append(-math.log(P0))
        S = np.mean(surprisal_list)
        operation.append(pymongo.UpdateOne({"_id": author_id},
                                           {"$set": {"bsur": S}}))

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
    print(col_author.count_documents({"sur": -6}))
    print(col_author.count_documents({"dn": -1}))
    print(col_author.count_documents({"bsur": -6}))


def find_discoverer(maxbsur):
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
