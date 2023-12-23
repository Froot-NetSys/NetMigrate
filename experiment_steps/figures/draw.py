import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as mtick
from matplotlib.ticker import ScalarFormatter
import re
import json


class ScalarFormatterForceFormat(ScalarFormatter):
    def _set_format(self):  # Override function that finds format to use.
        self.format = "%1.1f"  # Give format here


baseline = ["rocksteady", "source", "fulva", "netmigrate"]
write_ratio = ["0", "5", "10", "20", "30"]
cpu_limit = ["100", "70", "40"]
extra_exp = ["", "bf-19-17-", "bf-18-16-", "bf-18-16-3-", "bf-17-15-",
             "bf-16-14-", "bf-15-13-", "bf-14-12-", "160Mb-", "250Mb-"]

colors = {"rocksteady": "salmon", "fulva": "darkorange",
          "source": "darkseagreen", "netmigrate": "skyblue"}

f = open('time.json')
mg_time = json.load(f)

# set plt parameters
# plt.style.use('seaborn-deep')
plt.rcParams['font.family'] = 'Times New Roman'
plt.rcParams['font.size'] = 36  # 48
plt.rcParams['axes.labelsize'] = 36  # 48
plt.rcParams['legend.fontsize'] = 36  # 55
plt.rcParams["figure.figsize"] = (12, 5)
plt.rcParams['pdf.fonttype'] = 42

for prefix in extra_exp:
    for proto in baseline:
        for write_perc in write_ratio:
            for cpu in cpu_limit:

                baseline_color = "darkorange"
                baseline_color = colors[proto]

                if prefix != "" or write_perc != "5" or cpu_limit == "100":
                    continue

                '''
				if prefix == "":
					continue
				'''
                if write_perc != "5":
                    continue
                if prefix != "":
                    if proto != "netmigrate" or write_perc != "5" or cpu != "100":
                        continue
                file_name = "kv_log/" + prefix + proto + "-" + write_perc + "-" + cpu + ".txt"
                data = open(file_name, 'r').read()
                result_50 = re.findall(r'latency_50=(\d+)', data)
                latency_50 = list(map(float, result_50))  # us
                latency_50 = [x / 1000 for x in latency_50]  # ms
                result_99 = re.findall(r'latency_99=(\d+)', data)
                latency_99 = list(map(float, result_99))  # us
                latency_99 = [x / 1000 for x in latency_99]  # ms
                result_throughput = re.findall(r'sec: (\d+)', data)
                throughput = list(map(int, result_throughput))  # OPS
                throughput = [x / 1000 for x in throughput]  # KOPS
                result_extra_bandwidth_cumulated = re.findall(
                    r'extra_bandwidth_cumulated=(\d+)', data)
                extra_bandwidth_cumulated = list(
                    map(int, result_extra_bandwidth_cumulated))  # Bytes
                result_total_bandwidth = re.findall(
                    r'original_total_bandwidth=(\d+)', data)
                total_bandwidth = list(
                    map(int, result_total_bandwidth))  # Bytes

                if throughput[0] == 0:
                    throughput.pop(0)
                if latency_50[0] == 0:
                    latency_50.pop(0)
                if latency_99[0] == 0:
                    latency_99.pop(0)
                if extra_bandwidth_cumulated[0] == 0:
                    extra_bandwidth_cumulated.pop(0)
                if total_bandwidth[0] == 0:
                    total_bandwidth.pop(0)

                start_sec = mg_time[prefix][proto][write_perc][cpu]["start_sec"] - 1
                finish_sec = mg_time[prefix][proto][write_perc][cpu]["finish_sec"] - 1

                # calculate extra bandwidth usage
                original_total_bandwidth = 0
                extra_bandwidth = 0
                start_bw_sec = start_sec
                finish_bw_sec = finish_sec
                while start_bw_sec > 0 and extra_bandwidth_cumulated[start_bw_sec] != 0:
                    start_bw_sec -= 1

                start_sec = start_bw_sec
                extra_bw_percentage = extra_bandwidth_cumulated[finish_bw_sec] / (
                    total_bandwidth[finish_bw_sec] - total_bandwidth[start_bw_sec])

                avg_latency_50 = 0
                for i in range(start_sec, finish_sec):
                    avg_latency_50 += latency_50[i]
                avg_latency_50 = avg_latency_50 / (finish_sec - start_sec)

                avg_latency_99 = 0
                for i in range(start_sec, finish_sec):
                    avg_latency_99 += latency_99[i]
                avg_latency_99 = avg_latency_99 / (finish_sec - start_sec)

                avg_throughput = 0
                for i in range(start_sec, finish_sec):
                    avg_throughput += throughput[i]
                avg_throughput = avg_throughput / (finish_sec - start_sec)

                print(prefix+"{}-{}-{}%".format(proto, write_perc, cpu))
                # print("extra_bw_usage ={}%".format(extra_bw_percentage * 100))
                print("avg_throughput =", avg_throughput)
                print("avg_latency_50 =", avg_latency_50)
                print("avg_latency_99 =", avg_latency_99)
                # print(avg_throughput, avg_latency_50, avg_latency_99)
                # print(finish_sec - start_sec)
                # print(extra_bw_percentage * 100)
                # print(avg_throughput)
                # print(avg_latency_50)
                # print(avg_latency_99)

                # draw latency
                time = [x for x in range(len(latency_50))]  # second
                if len(time) <= 2000:
                    time_plot = [
                        int(x * 5) for x in range(int(len(latency_50) / 300) + 1)]  # min
                    time_plot_x = [x * 60 for x in time_plot]  # min
                else:
                    time_plot = [
                        int(x * 10) for x in range(int(len(latency_50) / 600) + 1)]  # min
                    time_plot_x = [x * 60 for x in time_plot]  # min

                # draw 50%-latency
                plt.figure()

                label = proto.capitalize()
                if proto == "netmigrate":
                    label = "NetMigrate"
                plt.scatter(time, latency_50, label="{}".format(
                    label), color=baseline_color)
                plt.xlabel("Time (Min)")
                plt.ylabel("50%-Latency (ms)")
                ax = plt.gca()
                yfmt = ScalarFormatterForceFormat()
                yfmt.set_powerlimits((0, 0))
                ax.ticklabel_format(style='sci', axis='y')
                # ax.yaxis.set_major_formatter(yfmt)
                plt.xticks(time_plot_x, time_plot)
                plt.xlim([0, 2700])
                if proto == "netmigrate":
                    plt.ylim([0, 20])
                    plt.yticks([x for x in range(0, 24, 4)],
                               [x for x in range(0, 24, 4)])
                elif proto == "rocksteady":
                    plt.ylim([0, 20])
                    plt.yticks([x for x in range(0, 24, 4)],
                               [x for x in range(0, 24, 4)])
                else:
                    plt.ylim([0, 20])
                    plt.yticks([x for x in range(0, 24, 4)],
                               [x for x in range(0, 24, 4)])
                plt.text(finish_sec, avg_latency_50, round(
                    avg_latency_50, 2), ha="left", va="center", rotation=0, fontsize=36)
                # plt.text((start_sec + finish_sec) / 2, avg_latency_50, "avg="+str(round(avg_latency_50, 2)), ha="center", va="top", rotation=0, fontsize=45)
                plt.axvline(x=start_sec, linestyle="dotted",
                            color="black", dashes=[3, 3], linewidth=5)
                plt.axvline(x=finish_sec, linestyle="dotted",
                            color="black", dashes=[3, 3], linewidth=5)
                plt.hlines(y=avg_latency_50, xmin=start_sec, xmax=finish_sec,
                           color="dimgray", linestyle="-", linewidth=5)
                plt.legend()
                plt.savefig('latency_fig/{}{}-{}-{}-50.pdf'.format(prefix,
                            proto, write_perc, cpu), bbox_inches='tight')

                # draw 99%-latency
                plt.figure()

                label = proto.capitalize()
                if proto == "netmigrate":
                    label = "NetMigrate"
                plt.scatter(time, latency_99, label="{}".format(
                    label), color=baseline_color)
                plt.xlabel("Time (Min)")
                plt.ylabel("99%-Latency (ms)")
                ax = plt.gca()
                yfmt = ScalarFormatterForceFormat()
                yfmt.set_powerlimits((0, 0))
                ax.ticklabel_format(style='sci', axis='y')
                # ax.yaxis.set_major_formatter(yfmt)
                plt.xticks(time_plot_x, time_plot)
                plt.xlim([0, 2700])
                if proto == "rocksteady" or proto == "netmigrate" and prefix == "250Mb-":
                    plt.ylim([0, 1800])
                    plt.yticks([x for x in range(0, 2000, 400)],
                               [x for x in range(0, 2000, 400)])
                elif proto == "fulva" or proto == "netmigrate" and prefix == "160Mb-":
                    plt.ylim([0, 100])
                    plt.yticks([x for x in range(0, 120, 20)],
                               [x for x in range(0, 120, 20)])
                else:
                    plt.ylim([0, 100])
                    plt.yticks([x for x in range(0, 120, 20)],
                               [x for x in range(0, 120, 20)])
                plt.text(finish_sec, avg_latency_99, round(
                    avg_latency_99, 2), ha="left", va="center", rotation=0, fontsize=36)
                # plt.text((start_sec + finish_sec) / 2, avg_latency_99, "avg="+str(round(avg_latency_99, 2)), ha="center", va="top", rotation=0, fontsize=45)
                plt.axvline(x=start_sec, linestyle="dotted",
                            color="black", dashes=[3, 3], linewidth=5)
                plt.axvline(x=finish_sec, linestyle="dotted",
                            color="black", dashes=[3, 3], linewidth=5)
                plt.hlines(y=avg_latency_99, xmin=start_sec, xmax=finish_sec,
                           color="dimgray", linestyle="-", linewidth=5)
                plt.legend()
                plt.savefig('latency_fig/{}{}-{}-{}-99.pdf'.format(prefix,
                            proto, write_perc, cpu), bbox_inches='tight')

                # draw throughput
                plt.figure()
                label = proto.capitalize()
                if proto == "netmigrate":
                    label = "NetMigrate"
                plt.scatter(time, throughput, label="{}".format(
                    label), color=baseline_color)
                plt.xlabel("Time (Min)")
                plt.ylabel("Throughput (KQPS)")
                ax = plt.gca()
                # yfmt = ScalarFormatterForceFormat()
                # yfmt.set_powerlimits((0,0))
                ax.ticklabel_format(style='sci', axis='y')
                # ax.yaxis.set_major_formatter(yfmt)
                plt.xticks(time_plot_x, time_plot)
                plt.xlim([0, 2700])
                plt.ylim([0, 1000])
                plt.yticks([x for x in range(0, 1200, 200)],
                           [x for x in range(0, 1200, 200)])
                plt.text(finish_sec, avg_throughput, str(
                    int(avg_throughput)), ha="left", va="center", rotation=0, fontsize=36)
                plt.text((finish_sec + start_sec) / 2, 900, str(round((finish_sec - start_sec) /
                         60, 1)) + "(min)", ha="center", va="center", rotation=0, fontsize=36)
                plt.axvline(x=start_sec, linestyle="dotted",
                            color="black", dashes=[3, 3], linewidth=5)
                plt.axvline(x=finish_sec, linestyle="dotted",
                            color="black", dashes=[3, 3], linewidth=5)
                plt.hlines(y=avg_throughput, xmin=start_sec, xmax=finish_sec,
                           color="dimgray", linestyle="-", linewidth=5)
                plt.legend()
                plt.savefig('throughput_fig/{}{}-{}-{}.pdf'.format(prefix,
                            proto, write_perc, cpu), bbox_inches='tight')

plt.close('all')
f.close()
