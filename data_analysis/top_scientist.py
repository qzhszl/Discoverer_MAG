# coding = utf-8
# @Time: 2021/5/18 8:23
# Author: Zhihao Qiu

'''
top scientist is the one whose total citation number is top X of all the researchers with the same first year

'''
import math
import multiprocessing
from time import time

import pymongo

from utils.connect_to_table import connectTable


def initialize_top_scientist():
    col_author = connectTable("qiuzh", "mag_authors0510")

    cursor = col_author.find(no_cursor_timeout=True)
    # researcher_number = cursor.count()
    # print(researcher_number)
    count = 0
    operation = []
    for author in cursor:
        count += 1
        operation.append(pymongo.UpdateOne({"_id": author["_id"]}, {"$set": {"iftop": 0}}))

        if count % 10000 == 0:
            print("已处理:", count / 10000, flush=True)
            col_author.bulk_write(operation, ordered=False)
            print("已写入:", count / 10000, flush=True)
            operation = []
    if operation:
        col_author.bulk_write(operation, ordered=False)
    print("finished")
    cursor.close()


def top_scientist(topx,begin,end,msg):
    col_author = connectTable("qiuzh","mag_authors0510")
    year_list = [1802, 1803, 1810, 1814, 1815, 1816, 1819, 1823, 1825, 1827, 1828, 1829, 1830, 1832, 1833, 1834, 1836, 1838,
                 1839, 1841, 1842, 1843, 1844, 1845, 1846, 1847, 1848, 1849, 1850, 1851, 1852, 1853, 1854, 1855, 1856, 1857,
                 1858, 1859, 1860, 1861, 1862, 1863, 1864, 1865, 1866, 1867, 1868, 1869, 1870, 1871, 1872, 1873, 1874, 1875,
                 1876, 1877, 1878, 1879, 1880, 1881, 1882, 1883, 1884, 1885, 1886, 1887, 1888, 1889, 1890, 1891, 1892, 1893,
                 1894, 1895, 1896, 1897, 1898, 1899, 1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910, 1911,
                 1912, 1913, 1914, 1915, 1916, 1917, 1918, 1919, 1920, 1921, 1922, 1923, 1924, 1925, 1926, 1927, 1928, 1929,
                 1930, 1931, 1932, 1933, 1934, 1935, 1936, 1937, 1938, 1939, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947,
                 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965,
                 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983,
                 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001,
                 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
    # year_list = [1957,1987]
    for year in year_list[begin:end]:
        cursor = col_author.find({"first_year": year}, no_cursor_timeout=True)
        researcher_number = cursor.count()
        print(msg, year, researcher_number)
        count = 0
        operation = []
        for author in cursor.sort("cn",pymongo.DESCENDING).limit(math.ceil(researcher_number * topx)):
            count += 1
            operation.append(pymongo.UpdateOne({"_id": author["_id"]}, {"$set": {"iftop": 1}}))

            if count % 10000 == 0:
                print(msg, "已处理:", count / 10000, flush=True)
                col_author.bulk_write(operation, ordered=False)
                print(msg, "已写入:", count / 10000, flush=True)
                operation = []
        if operation:
            col_author.bulk_write(operation, ordered=False)
        print(msg,year,"finished")
        cursor.close()


def VERIFY_top_scientist():
    col_author = connectTable("qiuzh","mag_authors0510")
    year_list = [1802, 1803, 1810, 1814, 1815, 1816, 1819, 1823, 1825, 1827, 1828, 1829, 1830, 1832, 1833, 1834, 1836, 1838,
                 1839, 1841, 1842, 1843, 1844, 1845, 1846, 1847, 1848, 1849, 1850, 1851, 1852, 1853, 1854, 1855, 1856, 1857,
                 1858, 1859, 1860, 1861, 1862, 1863, 1864, 1865, 1866, 1867, 1868, 1869, 1870, 1871, 1872, 1873, 1874, 1875,
                 1876, 1877, 1878, 1879, 1880, 1881, 1882, 1883, 1884, 1885, 1886, 1887, 1888, 1889, 1890, 1891, 1892, 1893,
                 1894, 1895, 1896, 1897, 1898, 1899, 1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910, 1911,
                 1912, 1913, 1914, 1915, 1916, 1917, 1918, 1919, 1920, 1921, 1922, 1923, 1924, 1925, 1926, 1927, 1928, 1929,
                 1930, 1931, 1932, 1933, 1934, 1935, 1936, 1937, 1938, 1939, 1940, 1941, 1942, 1943, 1944, 1945, 1946, 1947,
                 1948, 1949, 1950, 1951, 1952, 1953, 1954, 1955, 1956, 1957, 1958, 1959, 1960, 1961, 1962, 1963, 1964, 1965,
                 1966, 1967, 1968, 1969, 1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979, 1980, 1981, 1982, 1983,
                 1984, 1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000, 2001,
                 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
    # year_list = [1957, 1987]
    for year in year_list:
        cursor = col_author.find({"first_year": year}, no_cursor_timeout=True)
        researcher_number = cursor.count()
        print("理论值：", year, researcher_number,math.ceil(researcher_number * 0.1))
        print("实际值：",col_author.find({"first_year": year,"iftop":1}, no_cursor_timeout=True).count())
        cursor.close()


if __name__ == '__main__':
    VERIFY_top_scientist()

    # initialize_top_scientist()

    # # author_citation_count(1,5,1)
    # start = time()
    # # top_scientist(0.1,0,2,1)
    #
    # p1 = multiprocessing.Process(target=top_scientist,
    #                              args=(0.1, 0, 60, 1))
    # p2 = multiprocessing.Process(target=top_scientist,
    #                              args=(0.1, 60, 100, 2))
    # p3 = multiprocessing.Process(target=top_scientist,
    #                              args=(0.1, 100, 135, 3))
    # p4 = multiprocessing.Process(target=top_scientist,
    #                              args=(0.1, 135, 166, 4))
    # p5 = multiprocessing.Process(target=top_scientist,
    #                              args=(0.1, 166, 198, 5))
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
    # print("run time: %s" % ((end - start)/60))




