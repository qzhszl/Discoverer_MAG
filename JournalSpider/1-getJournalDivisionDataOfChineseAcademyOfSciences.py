import json
from time import sleep

import requests
from bs4 import BeautifulSoup


def _get_journal_detail(file, journal_name, year, headers):
    journal_name = journal_name.replace("&", "%26")
    uri = "http://www.fenqubiao.com/Core/JournalDetail.aspx?y=" + str(year) + "&t=" + journal_name
    response = requests.get(url=uri, timeout=50, headers=headers)
    try:
        soup = BeautifulSoup(response.content, features="lxml")
        table = soup.find("table")
        base_info = table.find_all("tr", limit=3)
    except Exception as e:
        sleep(1)
        print(journal_name)
        response = requests.get(url=uri, timeout=20, headers=headers)
        soup = BeautifulSoup(response.content, features="lxml")
        table = soup.find("table")
        base_info = table.find_all("tr", limit=3)
    full_name = journal_name
    short_name = base_info[1].find_all("td")[1].text
    issn = base_info[1].find_all("td")[3].text
    is_review = base_info[2].find_all("td")[3].text
    bodies = table.find_all("table")
    small_filed_info = []
    big_filed_info = []
    fields = bodies[0].find_all("tr")
    for idx in range(1, len(fields)):
        field = fields[idx].find_all("td")
        field_name = field[1].text
        field_level = field[2].text.strip()
        field_is_top = field[3].text
        if idx == len(fields) - 1:
            big_filed_info = [field_name, field_level, field_is_top]
        else:
            small_filed_info.append([field_name, field_level, field_is_top])

    index_infos = bodies[1].find_all("td")
    index_info = [index_infos[9].text, index_infos[10].text, index_infos[11].text, index_infos[13].text,
                  index_infos[14].text]
    result = [full_name, short_name, issn, year, is_review, small_filed_info,
              big_filed_info, index_info]
    # print(result)
    file.write(json.dumps(result, ensure_ascii=False) + "\n", )


def get_journal_details():
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;"
                  "q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Cookie": "ASP.NET_SessionId=k01v21ew1aedbjnici41zm2f; Hm_lvt_0dae59e1f85da1153b28fb5a2671647f=1602469663; __AntiXsrfToken=164ba9f7071a43c2af173a20af39e2b6; Hm_lpvt_0dae59e1f85da1153b28fb5a2671647f=1602470995",
        "Host": "www.fenqubiao.com",
        "Referer": "http://www.fenqubiao.com/Core/CategoryList.aspx",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; "
                      "x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36"
    }
    for filed in ["地学", "地学天文", "工程技术", "管理科学",
                  "化学", "环境科学与生态学", "农林科学",
                  "社会科学", "生物", "数学", "物理",
                  "医学", "综合性期刊"]:
        file = open("JournalList\\" + filed + ".txt", "r")
        details_file = open("JournalDetails\\" + filed + ".txt", "a")
        for line in file:
            journal_name = json.loads(line)[1]
            _get_journal_detail(details_file, journal_name, 2015, headers)
        file.close()
        details_file.close()
        print(filed)


def _get_journal_list(file, year, page, field):
    url = "http://www.fenqubiao.com/Core/CategoryList.aspx"
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        # "Content-Length": "3093",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "ASP.NET_SessionId=xkfppuhbiumdurzz4g2044yl; Hm_lvt_0dae59e1f85da1153b28fb5a2671647f=1616670590,1616670823; __AntiXsrfToken=c714059868634465884772b666eb53a8; Hm_lpvt_0dae59e1f85da1153b28fb5a2671647f=1616670986",
        "Host": "www.fenqubiao.com",
        "Origin": "http://www.fenqubiao.com",
        "Referer": "http://www.fenqubiao.com/Core/CategoryList.aspx",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36",
        "X-MicrosoftAjax": "Delta=true",
        "X-Requested-With": "XMLHttpRequest"
    }
    data = {
        "ctl00$ContentPlaceHolder1$ajaxManager": "ctl00$ContentPlaceHolder1$udpSelectItem|ctl00$ContentPlaceHolder1$btnSearch",
        "_TSM_HiddenField_": "r8mJnfb49_1ZuIQsipsHX-ZDZDri_bTBCAGx3HICehk1",
        "ctl00$ContentPlaceHolder1$dplYear": str(year),
        "ctl00$ContentPlaceHolder1$dplCategoryType": "0",
        "ctl00$ContentPlaceHolder1$dplCategory": field,
        "ctl00$ContentPlaceHolder1$dplSection": "0",
        "ctl00$ContentPlaceHolder1$dplSort": "0",
        "ctl00$ContentPlaceHolder1$AspNetPager1_input": "3",
        "__EVENTTARGET": "ctl00$ContentPlaceHolder1$AspNetPager1",
        "__EVENTARGUMENT": str(page),
        "__LASTFOCUS": "",
        "__VIEWSTATE": "WOOTWCrr0T/U3IPdSvahiMOegxIrBNDYUVBdoV+AU7Ub8mPfrox4RYyyXNNwkfPxUVdDkRY9d9Vbel6ErTLmuXX6r76cmqsQHbeJALJGIuR/MKLCVGqWZp6P3p4nmCseW0EWyuvH1t+FFMHb+xeyzuEJGIdoDwgEMqcXGvWNClQ1e7K0qn/fCldS1CaUzQaMm/buH3l5ApS0uJHM0ImVWXSwtTadzcEw6I3YCKmvjtPHzobvkYUEqX+Mm6S1qle9hlfJJgeteGAaJWxrc2HkeH9dTf4f5PqlGih17ERd8RsvMNnSpHjCX3FMhA5h6dy9BmYl1adFPMS8FphqKmOGjM942K8hHNCj1I+PUxNBBi/tw+nDHQwRTjDGtCFCfW42Y7ED21yhxLNTuddzSV1/2sDKvFNjN7R0cXDtpzgirywop9qhINulcG3yf6bI7E/rfxJ9vJ1N1cmzJoARZJqUzCbbcceGgffR9vmOm7tAjFUelJMpqAnZhP3LBlFuEmZkSQ+32OvLAN8nbaf10F9PEKj/Ba+ctTCJtfIvSFK+qjoZtF8TzWQtLgiO2yDIW3xr5DMFnT+bA0/1XE4Abg47at7dLaXZCkG/bkzcAjQ2hxurYs7NmyS6Lu+NpLm1NKVnBWD4GVNbuB8tS7S2l+XVPQj2lNT86vWSRAnJIZZHJRsKVOWElOE2h6MYrs7atd+DW74adRP4K3SGl4Gu7VxPS27hEywzpEmyqWOFApqfCAzSCn5tu3SfvbvEhnSZrlL89Ri0SwVaKD6Axs4L9aX71RoZ0kFrHMiipYKJXtdI9bdhoxNXXCNZav/K5wfLgtR+RpbNNksYz87Nq4l7gOq8d8Up5Te+Yqacm5bP/rW5xgRaFr8+KIopcYOIkfXFIHX1at3HF3QP7/brFk4WKgTdXvvK9dlrGtH8hi64irnd0hUjefu680x81HBR7gZugUsKl2RqawDIWFO0LYKeF6NtUo5y4hzLGF/iAQD6fpg+gPnhPCyUc/E4uGUKJnfG0zx6uSa9HnZb6B49EiLMdQYDLWUOj9uJy+O6KkJule4gbBrYL3jzh9UfCkC8dluX3YlMmw74EfyWAYZhpMjmJn2OppZg9uGqjH6XN/qpFCgCkfnBRSYWuqync94NocsmaL0JgIvm6uJHVL6bVcZpuJXJ0PUjrYuou6n927FncMfk5JA/cCGcZqm9kY83oriPf8Yc2lok+aQlTAs2E7vaIuZ8dQpxswtneMlt88RVklZAztizDgiEFPSxft8JF2pgSs0rqhvR2qKZHpbGVJQozvee4sR07AGOKY9/tBbzWi7pWhSqN+oEoZN9uDaKDNvO6Zvh",
        "__VIEWSTATEGENERATOR": "F775F8E1",
        "__EVENTVALIDATION": "QmQ5v5SMeXJM4TFH69p+x6ILCTUnLL0ifvko2LRvt1u/zQGApO+KW90vvo0PXsGmzVXFORu41KpMn7BCCuVLUztQxSRJNaal5o98L4mFt9LdwGPf28mTIvNi+aJ0iBJEI4T4ueGsHF6gmeGQ+uV/nKrjQcfFU86uiNfmdCLvUZkpO0foVjwxk/AA0Gpj1aEhPjAaSpw7+RP+XaoHOu2YnL5WT1GCCa9256E7NwmRaPWJDsisbnIoo1ZPAWr62NVL29Px4mmJHfLVObQtr4BPn9OHbzaKe3IAAY5kTQCHGOXMhv/922FYLIE50YwmS4CMghT19g+EfXOxfHtSxUbSzcHPjdNYIN4sa2BlLGIZ2YSeqoB9Lkkmv0wbEUFbAfDNiVMczZHori4bAKWg6AaE2Irwi0A/4FBXf7hRIvqMDwdLc8mMjQNNDjc0GbSbyHlPuIbJJV4Vl0W92qrD5uu/2defPL1JsMGBaoGzQdMhLl7FBqB5IKaKFAEoUvW6Bpc+/bxQxOF89HYVMiu5E6Epu3wnDBFsu7rP37d5Zri2sV5j6xYmY490LxlC6ntFVB8uMvTbvEhQH8mbqV7TYK5fFaI9kDjmGNwxHeFUHYDhLeggr2N37AmWRu1QDkH8G0hRzttTgIIMaQh5mImTlqHhkFUc3pEMMYk41Ew5jOi5G6sixta1FUohgkn2bXSEp7EljzzxfCYJwinpxZ4u0mW5+PrBK3mYzt4C/Np2jMLxxbr9K7Zm3nIaeAoGx73Jnl7m1SJT2odhUBvMrXg0N+MLsiuFs+dasmGOK7QpdMm9IKC5ZSmB8XrWumSVHkYgYdb77QBr6fhjf8NeUkgfEw587+D+z4nm1+tfs4528pe0c32YGdtwi5UNKyiE2fLQkDriLfSi7SuJ5SuSMzIoP46lmhKMOQxepJP5WORqZk90ksMXp+xmdAac4AcaQaehWWS6",
        "__ASYNCPOST": "true",
        # "ctl00$ContentPlaceHolder1$btnSearch": "浏览期刊"
    }
    # print("start")
    response = requests.post(url=url, data=data, headers=headers)
    # print("end")
    # print(response.content)
    a = BeautifulSoup(response.content, features="lxml").find_all("table")
    a = a[0].find_all("tr")
    for i in range(1, len(a)):
        text = a[i].text.strip().split("\n")
        text[1] = text[1].strip()
        text[3] = text[3].strip()
        file.write(json.dumps(text) + "\n")


def get_journal_list():
    pages = {"地学": 20,
             "地学天文": 3,
             "工程技术": 89,
             "管理科学": 5,
             "化学": 18,
             "环境科学与生态学": 18,
             "农林科学": 23,
             "社会科学": 2,
             "生物": 55,
             "数学": 26,
             "物理": 18,
             "医学": 157,
             "综合性期刊": 3}
    for filed in ["地学", "地学天文", "工程技术", "管理科学",
                  "化学", "环境科学与生态学", "农林科学",
                  "社会科学", "生物", "数学", "物理",
                  "医学", "综合性期刊"]:

        file = open("JournalList\\" + filed + ".txt", "a")
        for i in range(1, pages[filed] + 1):
            _get_journal_list(file, 2015, i, filed)
        file.close()
        print(filed)


if __name__ == "__main__":
    # get_journal_list()
    get_journal_details()
