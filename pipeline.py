import laspy

import os, sys, re
import argparse
import psutil
import time
import numpy as np
import pandas as pd
import ilock
import abc

from utils import BACKEND, COMPRESS, np_to_laspy_pts, laspy_to_np_pts, copy_header, timethis

DONE_FLAG = "-DONE-"


def run_spark_pipeline(filename, outfile, points_per_chunk):
    # get file metadata
    with laspy.open(filename, "r") as reader:
        total_pts = reader.header.point_count
        header = reader.header
    
    print("{} total points: {}".format(filename, total_pts))

    from pyspark import SparkConf
    from pyspark.context import SparkContext
    # get spark contenxt
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
    # parallelize shards
    shards = list(range(0, total_pts, points_per_chunk))
    rdd = sc.parallelize(shards)
    # load data
    rdd = rdd.map(lambda x: shard_loader(filename, x, points_per_chunk))


    rdd.foreach(lambda x: locked_writer(outfile, header, x))
    # rdd.foreachPartition(lambda x: locked_partition_writer(outfile, header, x))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file","-f", required=True)
    parser.add_argument("--outfile", default="out_pipeline.laz")
    parser.add_argument("--overwrite",action="store_true")
    parser.add_argument("--points-per-chunk","--ppc", type=int, default=1_000_000)
    ARGS = parser.parse_args()

    if os.path.exists(ARGS.outfile):
        if ARGS.overwrite:
            os.remove(ARGS.outfile)
        else:
            raise FileExistsError("Outfile exists, supply --overwrite flag to overwrite: '{}'".format(ARGS.outfile))

    with timethis():
        run_spark_pipeline(ARGS.file, ARGS.outfile, ARGS.points_per_chunk)

    with laspy.open(ARGS.outfile, "r") as reader:
        print("final npoints:", reader.header.point_count)



if __name__ == "__main__":
    main()
