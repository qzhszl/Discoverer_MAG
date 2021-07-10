from utils.connect_to_table import connectTable

'''
Functions in this .py are used to get the statistics of collections in this project.
They would not add or remove any information of the collections.
'''


def author_max_id():
    coll = connectTable("qiuzh", "mag_authors0411")
    # coll = connectTable("qiuzh", "test1")
    for i in coll.find().sort("_id", -1).limit(2):
        print(i)
    # 1000009205
    # 999992564
    # a = coll.aggregate([{"$group": {"_id": {},
    #                                 "max": {"$max": "$_id"},
    #                                 }}])
    # for i in a:
    #     print(i)


def author_pubs_count():
    '''
    author_pubs_count
    :return:
    '''
    coll = connectTable("qiuzh", "mag_authors0510")
    # coll = connectTable("qiuzh", "test1")
    a = coll.aggregate([{"$group": {"_id": {},
                                    "avg": {"$avg": "$pub_count"},
                                    "max": {"$max": "$pub_count"},
                                    "min": {"$min": "$pub_count"},
                                    "sum": {"$sum": "$pub_count"},
                                    }}])
    for i in a:
        print(i)


def author_bootstrap_count():
    '''
    author_pubs_count
    :return:
    '''
    coll = connectTable("qiuzh", "mag_authors0510")
    # coll = connectTable("qiuzh", "test1")
    a = coll.aggregate([{"$group": {"_id": {},
                                    "avg": {"$avg": "$bsur"},
                                    "max": {"$max": "$bsur"},
                                    "min": {"$min": "$bsur"}
                                    }}])
    for i in a:
        print(i)


def author_surprisal_count():
    '''
    author_pubs_count
    :return:
    '''
    coll = connectTable("qiuzh", "mag_authors0510")
    # coll = connectTable("qiuzh", "test1")
    a = coll.aggregate([{"$group": {"_id": {},
                                    "avg": {"$avg": "$sur"},
                                    "max": {"$max": "$sur"},
                                    "min": {"$min": "$sur"}
                                    }}])
    for i in a:
        print(i)


def author_discoverer_count():
    '''
    author_pubs_count
    :return:
    '''
    coll = connectTable("qiuzh", "mag_authors0510")
    # coll = connectTable("qiuzh", "test1")
    print(coll.count_documents({"iftop": 1}))
    print(coll.count_documents({"ifdis": 1}))
    print(coll.count_documents({"iftop": 1,"ifdis": 1}))


def author_first_year_distribution():
    '''

    :return:
    '''
    col = connectTable("qiuzh", "mag_authors0510")
    yearlist = col.distinct("first_year")
    print(yearlist)


def paper_citation_count():
    '''
        author_pubs_count
        :return:
        '''
    coll = connectTable("qiuzh", "mag_papers0510")
    # coll = connectTable("qiuzh", "test1")
    a = coll.aggregate([{"$group": {"_id": {},
                                    "avg": {"$avg": "$cn"},
                                    "max": {"$max": "$cn"},
                                    "min": {"$min": "$cn"},
                                    "sum": {"$sum": "$cn"},
                                    }}])
    for i in a:
        print(i)


def paper_field_count():
    colpaper = connectTable("qiuzh", "mag_papers0510")
    field_list = ["GEOGRAPHY", "ASTRONOMY", "ENGINEERING", "MANAGEMENT", "CHEMISTRY", "ENVIRONMENTAL SCIENCES",
                  "AGRONOMY", "SOCIAL SCIENCE", "BIOLOGY", "MATHEMATICS", "PHYSICS", "MEDICINE",
                  "MULTIDISCIPLINARY SCIENCES"]
    for field in field_list:
        n = colpaper.find({"field":field},no_cursor_timeout=True).count()
        print(field,n)


if __name__ == '__main__':
    author_surprisal_count()
    # coll = connectTable("qiuzh", "mag_authors0411")
    # col2 = connectTable("qiuzh", "mag_papers")
    # coll = connectTable("qiuzh", "test1")
    # author = coll.find({"pub_count":{"$gte":1500}})
    # print(author.count())
    # for au in author:
    #     papers = col2.find({"authors.id":au["_id"]})
    #
    #     print(au["_id"])
    #     venue_list = []
    #     for i in papers:
    #         if i["venue"]:
    #             venue_list.append(i["venue"])
    #     print(venue_list)
