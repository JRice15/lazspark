from pyspark import SparkConf
from pyspark.context import SparkContext
import numpy as np
import pandas as pd
import argparse
import laspy
import psutil
import time

# init globals
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))


def shard_loader(filename, seek_index):
    with laspy.open(filename, "r") as reader:
        reader.seek(seek_index)
        return reader.read_points(2).x

def write_out(outfile, header, pts):
    with laspy.open(outfile, "a", header=header, do_compress=True) as writer:
        writer.append_points(pts)


def make_file_rdd(filename, outfile, points_per_chunk):
    with laspy.open(filename, "r") as reader:
        # open outfile
        print(reader.header)
        print(laspy.header.LasHeader())
        print("B", str(laspy.header.LasHeader().version), type(laspy.header.LasHeader().version))
        with laspy.open(outfile, "w", header=reader.header) as writer:
            pass
        total_pts = reader.header.point_count
        shards = list(range(0, total_pts, points_per_chunk))
        rdd = sc.parallelize(shards)
        rdd = rdd.map(lambda x: shard_loader(filename, x))
        rdd = rdd.map(lambda x: write_out(outfile, None, x))
    return rdd



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file","-f", required=True)
    parser.add_argument("--outfile", required=True)
    parser.add_argument("--points-per-chunk","--ppc", type=int, default=1_000_000)
    ARGS = parser.parse_args()

    t = time.perf_counter()
    rdd = make_file_rdd(ARGS.file, ARGS.outfile, ARGS.points_per_chunk)
    x = rdd.collect()
    print(time.perf_counter() - t)
    print(x, type(x))

    # t = time.perf_counter()
    # with laspy.open(ARGS.file, "r") as reader:
    #     total_pts = reader.header.point_count
    #     pts_per_shard = total_pts // ARGS.max_workers + 1
    #     reader.seek(total_pts - 100_000)
    #     for pts in reader.chunk_iterator(100):
    #         print(pts)
    # print(time.perf_counter() - t)



if __name__ == "__main__":
    main()
