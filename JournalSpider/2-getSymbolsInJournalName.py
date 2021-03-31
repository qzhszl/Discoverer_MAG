# coding=utf-8
import json
from time import sleep

import requests
from bs4 import BeautifulSoup


# 期刊名称中的特殊字符集{'-', '&'}
def print_symbols_in_journal_name():
    for filed in ["地学", "地学天文", "工程技术", "管理科学",
                  "化学", "环境科学与生态学", "农林科学",
                  "社会科学", "生物", "数学", "物理",
                  "医学", "综合性期刊"]:
        details_file = open("JournalDetails\\" + filed + ".txt", "r", encoding="gbk")
        symbols = set()
        for line in details_file:
            name = json.loads(line)
            name[0] = name[0].replace("%26", "&")
            for char in name[0]:
                if (not char.isalpha()) and char != " ":
                    symbols.add(char)
                    print(filed, name[0])
        details_file.close()
    print(symbols)


if __name__ == "__main__":
    print_symbols_in_journal_name()
