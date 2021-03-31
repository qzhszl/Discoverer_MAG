import json


# 8615 8604
def exclude_dup():
    total_count = 0
    count_without_dup = 0
    details = set()
    for filed in ["地学", "地学天文", "工程技术", "管理科学",
                  "化学", "环境科学与生态学", "农林科学",
                  "社会科学", "生物", "数学", "物理",
                  "医学", "综合性期刊"]:
        details_file = open("JournalDetails\\" + filed + ".txt", "r", encoding="gbk")
        new_details_file = open("JournalDetails\\" + filed + "_v2.txt", "a", encoding="gbk")

        for i in details_file:

            total_count += 1
            i = json.loads(i)
            details.add(i[0])
            if i[6][0] != filed:
                print(filed, i[6][0])
                print(i)
                print("--------------------------------")
            else:
                count_without_dup += 1
                new_details_file.write(json.dumps(i, ensure_ascii=False) + "\n", )
        details_file.close()
        new_details_file.close()
    print(total_count, count_without_dup)
    print(len(details))


if __name__ == "__main__":
    exclude_dup()