import laspy

# i dunno why, but importing spark before laspy breaks things
from pyspark import SparkConf
from pyspark.context import SparkContext
import numpy as np
import pandas as pd
import argparse
import psutil
import time
import os

from utils import BACKEND, COMPRESS


parser = argparse.ArgumentParser()
parser.add_argument("--file","-f", required=True)
parser.add_argument("--outfile", default="out_simple.laz")
parser.add_argument("--points-per-chunk","--ppc", type=int, default=1_000_000)
ARGS = parser.parse_args()

if os.path.exists(ARGS.outfile):
    os.remove(ARGS.outfile)

# init globals
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

points_per_chunk = 1_000_000

t1 = time.perf_counter()
with laspy.open(ARGS.file, "r") as reader:
    with laspy.open(ARGS.outfile, "w", header=reader.header, do_compress=BACKEND, laz_backend=BACKEND) as writer:
        for pts in reader.chunk_iterator(points_per_chunk):
            writer.write_points(pts)

print(time.perf_counter() - t1)

with laspy.open(ARGS.outfile, "r") as reader:
    print("final npoints:", reader.header.point_count)
