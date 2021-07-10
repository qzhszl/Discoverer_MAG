import matplotlib.pyplot as plt
import math
import numpy as np

from utils.connect_to_table import connectTable


def draw_picture():
    '''
    this is a picture template from wenlong
    :return:
    '''
    font2 = {'family': 'Times New Roman',
             'weight': 'normal',
             'size': 10,
             }
    fig, ax = plt.subplots(2, 3)
    contents = {'1': [8041, 2125, 974, 631, 397, 210, 179, 111, 122, 122],
                '2': [30731, 17992, 12084, 9780, 8451, 6080, 5703, 5375, 4774, 6468],
                '3': [202489, 85264, 49241, 36119, 28306, 19345, 16981, 14584, 11831, 14052],
                '4': [57512, 10794, 6290, 4879, 4062, 3041, 2575, 2286, 2009, 2386],
                '5': [2705, 1814, 1217, 1065, 909, 631, 588, 495, 416, 488],
                '6': [16732, 10357, 7021, 5898, 5178, 3807, 3423, 3088, 2622, 3187]}
    for k, content in contents.items():
        title = "nicai"
        k = int(k) - 1
        ax1 = ax[int(k / 3)][k % 3]
        ax1.set_title(title)
        total_authors = sum(content)
        base = 1
        while base < max(content):
            base *= 10
        base /= 10
        accumulate_rate = []
        x_bar = []
        y_bar = []
        pre_sum = 0
        for i in range(len(content)):
            x_bar.append(i / 10 + 0.05)
            y_bar.append(content[i])
            accumulate_rate.append(pre_sum / total_authors)
            pre_sum += content[i]
        accumulate_rate.append(pre_sum / total_authors)
        # ax2.legend(loc="best", bbox_to_anchor=(1, 0.75))
        # ax2.set_yscale("log")
        ax1.plot([i / 10 for i in range(11)], accumulate_rate, c="black",
                 label="${P({\leq}N_{authors}^{*}/N_{authors}})$", zorder=1)
        ax1.scatter([i / 10 for i in range(11)],
                    accumulate_rate,
                    s=20, c="r", marker="*", zorder=2, alpha=0.7)
        ax1.grid()
        if int(k / 3) == 1:
            ax1.set_xlabel("${N_{authors}^{*}/N_{authors}}$", fontdict=font2)
        if k % 3 == 0:
            ax1.set_ylabel("${P({\leq}N_{authors}^{*}/N_{authors}})$", fontdict=font2)

            ax1.set_yticks([i * 2 / 10 for i in range(6)])
        else:
            ax1.set_yticks((0.0, 0.2, 0.4, 0.6, 0.8, 1.0), )
        # ax1.set_xticks([i * 2 / 10 for i in range(6)])
        ax1.set_aspect(1)

        ax2 = ax1.twinx()

        def formatnum(x, pos):
            return '$%.1f$' % (x)

        formatter = plt.FuncFormatter(formatnum)
        ax2.yaxis.set_major_formatter(formatter)
        ax2.bar(x_bar, [index / base for index in y_bar], 0.1, alpha=0.4, label="$N_{authors}$")
        base = math.log(base, 10)
        ax2.set_ylabel("$N_{authors}(×10^{%s})$" % (round(base)), fontdict=font2)
        # ax1.legend(loc="best", bbox_to_anchor=(0.45, 0.8))
    # ax1.set_aspect(1)
    # plt.yscale("log")
    # ax1.axis('equal')
    # plt.xticks([i/10 for i in range(11)])
    # plt.title("Accumulative Distribution of First L1 Paper")
    # fig.subplots_adjust(top=0.9)
    # plt.legend()
    plt.subplots_adjust(wspace=0.9, hspace=0.3)
    plt.savefig("3-distributionOfFirstL1PaperLocation(fields).png", dpi=600)
    plt.show()


def distribution_di():
    '''
    the distribution of di
    :return:
    '''

    DI = []
    with open("Discoverer.txt","r") as f:
        for line in f:
            DI.append(int(line))
    plt.rc('font', family='times new roman')
    X = [x for x in range(1,len(DI)+1)]
    DI.sort(reverse=True)
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    plt.xlabel("${i}$", fontsize='16')
    plt.ylabel("${d_i}$", fontsize='16')

    ax.scatter(X, DI, s=8, c='c', alpha=0.5)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.xlim(10**-1, 10 ** 8)
    plt.ylim(10**-1, 10 ** 6)
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/dis_di.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/dis_di.pdf", dpi=600, bbox_inches="tight")
    plt.show()
    print("图1 ok")


def distribution_Si():
    '''
    the distribution of di
    :return:
    '''

    DI = []
    with open("Surprisal.txt","r") as f:
        for line in f:
            DI.append(float(line))
    plt.rc('font', family='times new roman')
    X = [x for x in range(1,len(DI)+1)]
    DI.sort(reverse=True)
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    plt.xlabel("${i}$", fontsize='16')
    plt.ylabel("${S_i}$", fontsize='16')

    ax.scatter(X, DI, s=8, c='c', alpha=0.5)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.xlim(10**0, 10 ** 8)
    plt.ylim(10**-8, 10 ** 4)
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/dis_Si.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/dis_Si.pdf", dpi=600, bbox_inches="tight")
    plt.show()
    print("图1 ok")


def correlation_di_ki_ci_si():
    '''
    the correlation between d_i(discovery_times) with k_i(coauthor times)
    s_i(surprisal) and c_i(citation counts)
    :return:
    '''
    col1 = connectTable('qiuzh', "mag_authors0510")
    DI = []
    KI = []
    SI = []
    CI = []
    for author in col1.find():
        d_i = author["dn"]
        k_i = author["con"]
        s_i = author["sur"]
        c_i = author["cn"]
        DI.append(d_i)
        KI.append(k_i)
        SI.append(s_i)
        CI.append(c_i)

    print("data loaded successfully")
    plt.rc('font', family='times new roman')
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    plt.xlabel("${d_i}$", fontsize='16')
    plt.ylabel("${k_i}$", fontsize='16')
    ax.scatter(DI, KI, s=8, c='c', alpha=0.5)
    # plt.xlim(10 ** -1, 10 ** 5)
    # plt.ylim(10 ** -3, 10 ** 3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/di_ki.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/di_ki.pdf", dpi=600, bbox_inches="tight")

    print("图1 ok")
    correlation = np.corrcoef(DI, KI)
    print("d_i,k_i",correlation)

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    plt.xlabel("${d_i}$", fontsize='16')
    plt.ylabel("${c_i}$", fontsize='16')
    ax.scatter(DI, CI, s=8, c='c', alpha=0.5)
    # plt.xlim(10 ** -1, 10 ** 5)
    # plt.ylim(10 ** -3, 10 ** 3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/di_ci.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/di_ci.pdf", dpi=600, bbox_inches="tight")

    print("图2 ok")
    correlation = np.corrcoef(DI, CI)
    print("d_i,c_i", correlation)


    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    plt.xlabel("${S_i}$", fontsize='16')
    plt.ylabel("${k_i}$", fontsize='16')
    ax.scatter(SI, KI, s=8, c='c', alpha=0.5)
    # plt.xlim(10 ** -1, 10 ** 5)
    # plt.ylim(10 ** -3, 10 ** 3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/si_ki.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/si_ki.pdf", dpi=600, bbox_inches="tight")

    print("图3 ok")
    correlation = np.corrcoef(SI, KI)
    print("SI,KI", correlation)


    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    plt.xlabel("${S_i}$", fontsize='16')
    plt.ylabel("${c_i}$", fontsize='16')
    ax.scatter(SI, CI, s=8, c='c', alpha=0.5)
    # plt.xlim(10 ** -1, 10 ** 5)
    # plt.ylim(10 ** -3, 10 ** 3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/si_ci.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/si_ci.pdf", dpi=600, bbox_inches="tight")

    print("图4 ok")
    correlation = np.corrcoef(SI, CI)
    print("s_i,c_i", correlation)


def correlation_di_ki_ci_si2():
    '''
    the correlation between d_i(discovery_times) with k_i(coauthor times)
    s_i(surprisal) and c_i(citation counts)
    this function is created in 2021.7.1, we save the list in the PC to save time
    :return:
    '''

    DI = []
    KI = []
    SI = []
    CI = []
    with open("Discoverer.txt","r") as f:
        for line in f:
            DI.append(int(line))
    with open("Coauthor.txt","r") as f:
        for line in f:
            KI.append(int(line))

    with open("Surprisal.txt","r") as f:
        for line in f:
            SI.append(float(line))

    with open("Citation.txt", "r") as f:
        for line in f:
            CI.append(int(line))
    print("data loaded successfully",len(DI),len(KI))
    plt.rc('font', family='times new roman')
    # fig = plt.figure()
    # ax = fig.add_subplot(1,1,1)
    # plt.xlabel("${k_i}$", fontsize='16')
    # plt.ylabel("${d_i}$", fontsize='16')
    # ax.scatter(KI, DI, s=8, c='c', alpha=0.5)
    # plt.xlim(10**-1, 10 ** 6)
    # plt.ylim(10**-1, 10 ** 6)
    # ax.set_xscale("log")
    # ax.set_yscale("log")
    # plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/di_ki.png", dpi=600, bbox_inches="tight")
    # plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/di_ki.pdf", dpi=600, bbox_inches="tight")
    # plt.show()
    # print("图1 ok")
    # correlation = np.corrcoef(DI, KI)
    # print("d_i,k_i",correlation)

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    plt.xlabel("${d_i}$", fontsize='16')
    plt.ylabel("${c_i}$", fontsize='16')
    ax.scatter(DI, CI, s=8, c='c', alpha=0.5)
    # plt.xlim(10 ** -1, 10 ** 5)
    # plt.ylim(10 ** -3, 10 ** 3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/di_ci.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/di_ci.pdf", dpi=600, bbox_inches="tight")

    print("图2 ok")
    correlation = np.corrcoef(DI, CI)
    print("d_i,c_i", correlation)


    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    plt.xlabel("${S_i}$", fontsize='16')
    plt.ylabel("${k_i}$", fontsize='16')
    ax.scatter(SI, KI, s=8, c='c', alpha=0.5)
    # plt.xlim(10 ** -1, 10 ** 5)
    # plt.ylim(10 ** -3, 10 ** 3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/si_ki.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/si_ki.pdf", dpi=600, bbox_inches="tight")

    print("图3 ok")
    correlation = np.corrcoef(SI, KI)
    print("SI,KI", correlation)


    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    plt.xlabel("${S_i}$", fontsize='16')
    plt.ylabel("${c_i}$", fontsize='16')
    ax.scatter(SI, CI, s=8, c='c', alpha=0.5)
    # plt.xlim(10 ** -1, 10 ** 5)
    # plt.ylim(10 ** -3, 10 ** 3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/si_ci.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/si_ci.pdf", dpi=600, bbox_inches="tight")

    print("图4 ok")
    correlation = np.corrcoef(SI, CI)
    print("s_i,c_i", correlation)


def surprisal_bootstrapsurprisal():
    S = []
    with open("Surprisal.txt","r") as f:
        for line in f:
            S.append(float(line))

    BS = []
    with open("Bsur.txt","r") as f:
        for line in f:
            BS.append(float(line))

    plt.rc('font', family='times new roman')
    X = [x for x in range(1,len(S)+1)]
    S.sort(reverse=True)
    BS.sort(reverse=True)
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    plt.xlabel("${i}$", fontsize='16')
    plt.ylabel("${S_i}$", fontsize='16')

    print(len(S))
    print(len(BS))

    plt.ylim(10 ** -8, 10 ** 4)
    ax.loglog(X, S, c='purple', label='Realdata', linewidth=5)
    ax.loglog(X, BS, '--', c='orange', label='Bootstrap',linewidth=5)

    plt.xlabel('$i$', fontsize='16')
    plt.ylabel('$S_i$', fontsize='16')
    plt.legend(fontsize='20')
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/S_BS.png", dpi=600, bbox_inches="tight")
    plt.savefig("C:/Users/qzh/PycharmProjects/MAG/figure/S_BS.pdf", dpi=600, bbox_inches="tight")
    plt.show()
    print("图1 ok")


if __name__ == '__main__':
    distribution_Si()

