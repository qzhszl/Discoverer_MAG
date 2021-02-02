# -*- coding: utf-8 -*-
from utils.connect_to_table import connectTable

# col2 = connectTable("oga_one", "mag_paper_plus2")
# col2.create_index([('new_authors.id', 1)])

col2 = connectTable("qiuzh", "MAG_authors")
col2.create_index([('id', 1)])


# col2 = connectTable("qiuzh", "test")
# col2.create_index([('pubs.i', 1),("n_citation", 1)])
# # a = col2.index_information()
# print(a)