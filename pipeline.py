import laspy

import os, sys, re
import argparse
import psutil
import time
import numpy as np
import pandas as pd
import ilock

from utils import BACKEND, COMPRESS, np_to_laspy_pts, laspy_to_np_pts


class FixedVersion(laspy.header.Version):
    """fixes some strange bug in deepcopying Versions"""

    def __deepcopy__(self, memo):
        result = laspy.header.Version.from_str(str(self))
        memo[id(self)] = result
        return result

def copy_header(header):
    version = FixedVersion.from_str(str(header.version))
    header = laspy.header.LasHeader(version=version, point_format=header.point_format)
    return header


def queue_writer(q, outfile, header, key):
    multiprocessing.current_process().authkey = key
    with laspy.open(outfile, "w", header=header, do_compress=COMPRESS, laz_backend=BACKEND) as writer:
        while True:
            pts = q.get()
            if pts == DONE_FLAG:
                return
            writer.write_points(pts)

# def normal_writer(outfile, header, pts):
#     header.version = FixedVersion.from_str(str(header.version))
#     mode = "a" if os.path.exists(outfile) else "w"
#     with laspy.open(outfile, mode, header=header, do_compress=COMPRESS, laz_backend=BACKEND) as writer:
#         data = laspy.point.record.PackedPointRecord.zeros(len(pts), header.point_format)
#         data.x = pts[:,0]
#         data.y = pts[:,1]
#         data.z = pts[:,2]
#         if mode == "a":
#             writer.append_points(data)
#         else:
#             writer.write_points(data)


def locked_writer(outfile, header, pts):
    # reimporting laspy helps things
    with ilock.ILock("jr-laz-output"):
        if os.path.exists(outfile):
            # appending
            with laspy.open(outfile, "a", do_compress=COMPRESS, laz_backend=BACKEND) as writer:
                header = writer.header
                pts = format_pts(pts, header.point_format)
                writer.append_points(pts)
        else:
            # writing
            header = copy_header(header)
            pts = format_pts(pts, header.point_format)
            with laspy.open(outfile, "w", header=header, do_compress=COMPRESS, laz_backend=BACKEND) as writer:
                writer.write_points(pts)


def locked_partition_writer(outfile, header, pts_iter):
    # reimporting laspy helps things
    import laspy
    with ilock.ILock("jr-laz-output"):
        if os.path.exists(outfile):
            # appending
            with laspy.open(outfile, "a", do_compress=COMPRESS, laz_backend=BACKEND) as writer:
                header = writer.header
                for pts in pts_iter:
                    pts = np_to_laspy_pts(pts, header.point_format)
                    writer.append_points(pts)
        else:
            # writing
            header.version = FixedVersion.from_str(str(header.version))
            # header.partial_reset()
            with laspy.open(outfile, "w", header=header, do_compress=COMPRESS, laz_backend=BACKEND) as writer:
                for pts in pts_iter:
                    pts = np_to_laspy_pts(pts, header.point_format)
                    writer.write_points(pts)


def shard_loader(filename, seek_index, points_per_chunk):
    with laspy.open(filename, "r") as reader:
        reader.seek(seek_index)
        pts = reader.read_points(points_per_chunk)
        return laspy_to_np_pts(pts)


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

    # q = multiprocessing.Manager().Queue()

    # key = os.urandom(20)
    # multiprocessing.current_process().authkey = key
    # proc = multiprocessing.Process(target=queue_writer, args=(q, outfile, header, key))

    # def enqueuer(x):
    #     multiprocessing.current_process().authkey = key
    #     q.put(x)

    # rdd.foreach(q.put)
    # proc.start()
    # q.put(None)
    # proc.join()

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

    t = time.perf_counter()
    run_spark_pipeline(ARGS.file, ARGS.outfile, ARGS.points_per_chunk)
    print(time.perf_counter() - t)

    with laspy.open(ARGS.outfile, "r") as reader:
        print("final npoints:", reader.header.point_count)



if __name__ == "__main__":
    main()
