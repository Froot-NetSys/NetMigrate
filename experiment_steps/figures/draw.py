import matplotlib.pyplot as plt
import numpy as np
import matplotlib.ticker as mtick
from matplotlib.ticker import ScalarFormatter
import re
import json


class ScalarFormatterForceFormat(ScalarFormatter):
    def _set_format(self):  # Override function that finds format to use.
        self.format = "%1.1f"  # Give format here


# baseline = ["fulva", "rocksteady", "netmigrate", "source"]
# write_ratio = ["b", "c", "a", "10", "20", "30"]
baseline = ["netmigrate"]
write_ratio = ["b"]
cpu_limit = ["100"]
extra_exp = [""]

colors = {"rocksteady": "salmon", "fulva": "darkorange",
          "source": "darkseagreen", "netmigrate": "skyblue"}

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

                file_name = "./" + prefix + proto + "-" + write_perc + "-" + cpu + ".txt"
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

                print(prefix+"{}-{}-{}%".format(proto, write_perc, cpu))

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
                plt.figure(dpi=80)

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
                plt.legend()
                plt.savefig('latency_fig/{}{}-{}-{}-50.pdf'.format(prefix,
                            proto, write_perc, cpu), bbox_inches='tight', dpi='figure')

                # draw 99%-latency
                plt.figure(dpi=80)

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
                plt.legend()
                plt.savefig('latency_fig/{}{}-{}-{}-99.pdf'.format(prefix,
                            proto, write_perc, cpu), bbox_inches='tight', dpi='figure')

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
                ax.ticklabel_format(style='sci', axis='y')
                plt.xticks(time_plot_x, time_plot)
                plt.xlim([0, 2700])
                plt.ylim([0, 1000])
                plt.yticks([x for x in range(0, 1200, 200)],
                           [x for x in range(0, 1200, 200)])
                plt.legend()
                plt.savefig('throughput_fig/{}{}-{}-{}.pdf'.format(prefix,
                            proto, write_perc, cpu), bbox_inches='tight', dpi='figure')

plt.close('all')
