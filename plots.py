"""
script that generates plots based on timing data that I collected
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn
import os

os.makedirs("plots", exist_ok=True)

seaborn.set()

decimation = [8000, 400, 20, 1]
ptcount = [133249, 2664978, 53299550, 1065990989]
kilobytes = [1596, 25460, 428268, 4574416]
simple_times = [
    0.5183737808838487,
    8.733441576361656,
    170.44923543836921,
    4414.038417331874,
]
parallel_times = [
    8.284420046024024,
    15.76900921575725,
    40.71772013325244,
    174.48272293899208,
]


plt.plot(ptcount, simple_times)
plt.plot(ptcount, parallel_times)
plt.xlabel("Number of input points")
plt.ylabel("Running time (seconds)")
plt.xscale("log")
plt.yscale("log")
plt.legend(["simple", "parallel"])
plt.tight_layout()
plt.savefig("plots/ptcount_log.png")
plt.clf()

plt.plot(ptcount, simple_times)
plt.plot(ptcount, parallel_times)
plt.xlabel("Number of input points")
plt.ylabel("Running time (seconds)")
plt.xscale("log")
# plt.yscale("log")
plt.legend(["simple", "parallel"])
plt.tight_layout()
plt.savefig("plots/ptcount.png")
plt.clf()

plt.plot(kilobytes, simple_times)
plt.plot(kilobytes, parallel_times)
plt.xlabel("Size of input file (kilobytes)")
plt.ylabel("Running time (seconds)")
plt.xscale("log")
plt.yscale("log")
plt.legend(["simple", "parallel"])
plt.tight_layout()
plt.savefig("plots/kilobytes_log.png")
plt.clf()


plt.plot(kilobytes, simple_times)
plt.plot(kilobytes, parallel_times)
plt.xlabel("Size of input file (kilobytes)")
plt.ylabel("Running time (seconds)")
plt.xscale("log")
# plt.yscale("log")
plt.legend(["simple", "parallel"])
plt.tight_layout()
plt.savefig("plots/kilobytes.png")
plt.clf()
speedup = np.array(simple_times) / np.array(parallel_times)

plt.plot(ptcount, speedup, c="green")
plt.xlabel("Number of input points")
plt.ylabel("Speedup (simple / parallel)")
plt.xscale("log")
# plt.yscale("log")
plt.axhline(1, c="black", linestyle="--", label="equal runtime")
plt.tight_layout()
plt.savefig("plots/speedup.png")
plt.legend()
plt.clf()

plt.plot(ptcount, speedup, c="green")
plt.xlabel("Number of input points")
plt.ylabel("Speedup (simple / parallel)")
plt.xscale("log")
plt.yscale("log")
plt.axhline(1, c="black", linestyle="--", label="equal runtime")
plt.tight_layout()
plt.savefig("plots/speedup_log.png")
plt.legend()
plt.clf()