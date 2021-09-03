# coding = utf-8
# @Time: 2021/7/1 11:34
# Author: Zhihao Qiu
import math

from scipy import stats

from utils.connect_to_table import connectTable


def print_basic_statistics_into_pc():
    col1 = connectTable('qiuzh', "mag_researchers0810")
    DI = []
    KI = []
    SI = []
    CI = []
    for author in col1.find():
        d_i = author["dn"]
        k_i = author["new_con"]
        s_i = author["sur"]
        # if s_i == -1:
        #     s_i = -k_i*math.log(0.23354505591260138)
        c_i = author["cn"]
        DI.append(d_i)
        KI.append(k_i)
        SI.append(s_i)
        CI.append(c_i)

    data = open("Discoverer0810.txt", "w+")
    for j in range(len(DI)):
        print(DI[j], file=data)
    data.close()

    data = open("Coauthor0810.txt", "w+")
    for j in range(len(KI)):
        print(KI[j], file=data)
    data.close()

    data = open("Surprisal0810.txt", "w+")
    for j in range(len(SI)):
        print(SI[j], file=data)
    data.close()

    data = open("Citation0810.txt", "w+")
    for j in range(len(CI)):
        print(CI[j], file=data)
    data.close()


def print_bsur_into_pc():
    '''
    in 8.30 we used this function to print the data in the txt, However, we do save the data into Bsur rather than Bsur0810
    by mistake.
    :return:
    '''
    col1 = connectTable('qiuzh', "mag_researchers0810")
    DI = []
    KI = []
    SI = []
    for author in col1.find():
        d_i = author["bsur"]
        k_i = author["ifdis"]
        s_i = author["iftop"]
        DI.append(d_i)
        KI.append(k_i)
        SI.append(s_i)

    print("list has loaded")
    data = open("C:/Users/qzh/PycharmProjects/MAG/datafile/Bsur0810.txt", "w+")
    for j in range(len(DI)):
        print(DI[j], file=data)
    data.close()

    data = open("C:/Users/qzh/PycharmProjects/MAG/datafile/Ifdis0810.txt", "w+")
    for j in range(len(KI)):
        print(KI[j], file=data)
    data.close()

    data = open("C:/Users/qzh/PycharmProjects/MAG/datafile/Iftop0810.txt", "w+")
    for j in range(len(SI)):
        print(SI[j], file=data)
    data.close()


if __name__ == '__main__':
    print_bsur_into_pc()