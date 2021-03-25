import matplotlib.pyplot as plt
import math

def draw_picture():
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
        title = get_area_name(k)
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