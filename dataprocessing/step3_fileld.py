# coding = utf-8
# @Time: 2021/3/31 16:09
# Author: Zhihao Qiu
import json

import pymongo

from utils.connect_to_table import connectTable


def add_field():
    col1 = connectTable("qiuzh","mag_papers")

    for field in ["地学", "地学天文", "工程技术", "管理科学",
                  "化学", "环境科学与生态学", "农林科学",
                  "社会科学", "生物", "数学", "物理",
                  "医学", "综合性期刊"]:
        operation = []
        print(field)
        journal_detail = open("C://Users//qzh//PycharmProjects//MAG//JournalDetailsWithID//"+field+".txt","r",encoding="gbk")
        for line in journal_detail:
            a_journal_detail = json.loads(line)
            journal_ID = a_journal_detail[8]
            journal_field = a_journal_detail[6][0].replace("地学", "GEOGRAPHY").replace("地学天文", "ASTRONOMY").replace(
                "工程技术", "ENGINEERING").replace(
                "管理科学", "MANAGEMENT").replace("化学", "CHEMISTRY").replace("环境科学与生态学", "ENVIRONMENTAL SCIENCES").replace(
                "农林科学",
                "AGRONOMY").replace(
                "社会科学", "SOCIAL SCIENCE").replace("生物", "BIOLOGY").replace("数学", "MATHEMATICS").replace("物理",
                                                                                                        "PHYSICS").replace(
                "医学", "MEDICINE").replace("综合性期刊", "MULTIDISCIPLINARY SCIENCES")
            journal_level = a_journal_detail[6][1]
            operation.append(pymongo.UpdateMany({"venue.id": journal_ID},
                                                {"$set": {"field": journal_field, "level": journal_level}}))
        print(len(operation))
        col1.bulk_write(operation, ordered=False)


if __name__ == '__main__':
    add_field()
