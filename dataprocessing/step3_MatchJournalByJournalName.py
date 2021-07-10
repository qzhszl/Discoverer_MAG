# encoding="utf-8"
import json

from utils.connect_to_table import connectTable


def get_academic_table(a):
    coll = connectTable("academic", a)
    return coll

# 地学 381 21
# 地学天文 59 9
# 工程技术 1770 189
# 管理科学 86 4
# 化学 338 21
# 环境科学与生态学 342 21
# 农林科学 452 14
# 社会科学 36 4
# 生物 1098 84
# 数学 507 64
# 物理 343 27
# 医学 3133 242
# 综合性期刊 59 10
# 8604-710=7894
def match_journal():
    not_matched_journals = []
    not_matched_file = open("JournalDetailsWithID\\_NotMatchedJournals.txt", "a")
    for filed in ["地学", "地学天文", "工程技术", "管理科学",
                  "化学", "环境科学与生态学", "农林科学",
                  "社会科学", "生物", "数学", "物理",
                  "医学", "综合性期刊"]:
        details_file = open("..\\JournalSpider\\JournalDetails\\" + filed + "_v2.txt", "r", encoding="gbk")
        journal_id = open("JournalDetailsWithID\\" + filed + ".txt", "a")
        journal_count = 0
        not_match_count = 0
        for line in details_file:
            journal_count += 1
            name = json.loads(line)
            journal_name = name[0].replace("%26", "&")
            journal_name = " ".join(journal_name.replace("-", " - ").replace("&", " & ").split()).lower()
            journal = get_academic_table("mag_venues").find_one(
                    {"NewNormalizedName": journal_name, "JournalId": {"$exists": True}},
                    {"JournalId": 1})
            if journal:
                name.append(journal["JournalId"])
                journal_id.write(json.dumps(name, ensure_ascii=False) + "\n")
            else:
                not_matched_journals.append([filed, name[0].replace("%26", "&")])
                not_match_count += 1
                not_matched_file.write(json.dumps([filed, journal_name]) + "\n")
        details_file.close()
        journal_id.close()
        print(filed, journal_count, not_match_count)
    not_matched_file.close()


# # 去除“简称-全称”的命名规则，只获取全程去匹配
# def get_real_name_for_journal_with_a_short_name(name):
#     name = name.replace("-", " - ").replace("&", " & ").split()
#     if len(name) > 2 and name[1] == "-":
#         flag = True
#         for char in name[0]:
#             sub_flag = False
#             for word in name[1:]:
#                 if word[0].lower() == char.lower():
#                     sub_flag = True
#                     break
#             if not sub_flag:
#                 flag = False
#                 break
#         if flag:
#             name = name[2:]
#             return " ".join(name).lower()


if __name__ == "__main__":
    match_journal()
