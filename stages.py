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
NORESULT_FLAG = "-NO-RESULT-"


"""
Building blocks
"""


class Stage(abc.ABC):
    """
    base stage class. override `execute`, and optionally `update_header` and `__init__`
    """

    _cached_result = NORESULT_FLAG

    def __call__(self, *rdds):
        if self._cached_result == NORESULT_FLAG:
            self._cached_result = self.execute(*rdds)
        return self._cached_result

    @abc.abstractmethod
    def execute(self, rdd):
        """
        apply operations to an RDD
        """
        ...


class StartStage(Stage):
    """
    stage that begins computation
    """

    def __call__(self, *args):
        """
        returns
            rdd, header
        """
        rdd, header = super().__call__(*args)
        return rdd, header

    @abc.abstractmethod
    def execute(self):
        ...


class EndStage(Stage):
    """
    stage that ends a computation, and cannot be further linked from
    """

    def __repr__(self):
        return "{}".format(self.__class__.__name__)

    def __call__(self, *rdds):
        print("Executing EndStage", self)
        return super().__call__(*rdds)




"""
Concrete stages
"""


class Lambda(Stage):
    """
    stage that utilizes an arbitrary (stateless) function
    the function must accept an RDD as the first argument, and may optionally accept
    the `args` and `kwargs` provided following that
    """

    def __init__(self, f, args=None, kwargs=None):
        self.f = f
        self.args = () if args is None else args
        self.kwargs = {} if kwargs is None else kwargs
    
    def execute(self, rdd):
        f = self.f
        args = self.args
        kwargs = self.kwargs
        return rdd.map(lambda x: f(x, *args, **kwargs))



class Reader(StartStage):

    def __init__(self, filename, points_per_chunk):
        self.filename = filename
        self.points_per_chunk = points_per_chunk
        with laspy.open(self.filename, "r") as reader:
            self.total_pts = reader.header.point_count
            self.header = reader.header
    
        from pyspark import SparkConf
        from pyspark.context import SparkContext
        # get spark contenxt
        self.sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

    def execute(self):
        # parallelize shards
        shards = list(range(0, self.total_pts, self.points_per_chunk))
        rdd = self.sc.parallelize(shards)

        filename = self.filename
        ppc = self.points_per_chunk
        # load data
        def _shard_loader(seek_index, filename, points_per_chunk):
            with laspy.open(filename, "r") as reader:
                reader.seek(seek_index)
                pts = reader.read_points(points_per_chunk)
                return laspy_to_np_pts(pts)
        rdd = rdd.map(lambda x: _shard_loader(x, filename, ppc))
        return rdd, self.header


class Writer(EndStage):

    def __init__(self, outfile, header, overwrite=False):
        self.outfile = outfile
        self.header = header
        if os.path.exists(outfile):
            if overwrite:
                os.remove(outfile)
            else:
                raise FileExistsError("File already exists, supply `overwrite=True` to Writer to overwrite: {}".format(outfile))

    def __repr__(self):
        return "Writer('{}')".format(self.outfile)

    def execute(self, rdd):
        # writer function
        def locked_writer(pts, outfile, header):
            with ilock.ILock("jr-laz-output"):
                if os.path.exists(outfile):
                    # appending
                    with laspy.open(outfile, "a", do_compress=COMPRESS, laz_backend=BACKEND) as writer:
                        header = writer.header
                        pts = np_to_laspy_pts(pts, header.point_format)
                        writer.append_points(pts)
                else:
                    # writing
                    header = copy_header(header)
                    pts = np_to_laspy_pts(pts, header.point_format)
                    with laspy.open(outfile, "w", header=header, do_compress=COMPRESS, laz_backend=BACKEND) as writer:
                        writer.write_points(pts)
        
        outfile = self.outfile
        header = self.header
        rdd.foreach(lambda x: locked_writer(x, outfile, header))


class Collect(EndStage):

    def execute(self, rdd):
        return rdd.collect()


def queue_writer(q, outfile, header, key):
    multiprocessing.current_process().authkey = key
    with laspy.open(outfile, "w", header=header, do_compress=COMPRESS, laz_backend=BACKEND) as writer:
        while True:
            pts = q.get()
            if pts == DONE_FLAG:
                return
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
            header = copy_header(header)
            with laspy.open(outfile, "w", header=header, do_compress=COMPRESS, laz_backend=BACKEND) as writer:
                for pts in pts_iter:
                    pts = np_to_laspy_pts(pts, header.point_format)
                    writer.write_points(pts)

