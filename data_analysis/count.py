from utils.connect_to_table import connectTable

coll3 = connectTable('qiuzh', "MAG_authors")
b= coll3.find({"new_pubs":{"$exists":True}}).count()
print(b)