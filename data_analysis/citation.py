# -*- coding: utf-8 -*-
from utils.connect_to_table import connectTable

__author__ = "ZHIHAO QIU"
import numpy as np
import pandas as pd


def recalculate_cnpn():
    '''
    we need to recalculate the citation/publication according to the new reference data

    :return:
    '''
    col1 = connectTable('qiuzh', "MAG_authors")
    col2 = connectTable("oga_one", "mag_paper_plus2")
    for i in col1.find():
        id = i.get("id")
        paper_list = i.get("new_pubs")
        pub_n = len(paper_list)
        citation = 0
        for paper_id in paper_list:
            paper_citation = col2.find({"references":paper_id}).count
        citation += paper_citation
        col1.update_one({"id":id},{"new_citation":citation,"new_pub_number":pub_n})

if __name__ == '__main__':
    recalculate_cnpn()
