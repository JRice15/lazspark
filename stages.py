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


"""
Building blocks
"""

class Pipeline():

    def __init__(self, *outputs):
        self.outputs = outputs
        if not all([isinstance(s, AbstractOutput) for s in self.outputs]):
            raise TypeError("All end stages passed to the Pipeline must be instances of EndStage")

    def iter_outputs(self):
        for abstract_output in self.outputs:
            print("Executing EndStage", abstract_output.operation)
            yield abstract_output.evaluate()

    def run_all(self):
        for abstract_output in self.outputs:
            print("Executing EndStage", abstract_output.operation)
            abstract_output.evaluate()


class Stage(abc.ABC):
    """
    base stage class. override `execute`, and optionally `update_header` and `__init__`
    """

    def __call__(self, inpt):
        if not isinstance(inpt, AbstractOutput):
            raise TypeError("Can only link a Stage to the result of __call__ing another stage. For example, `r = Reader(...); w = Writer(...)(r)` fails. `r = Reader(...)(); w = Writer(...)(r)` succeeds")
        return AbstractOutput(self, inpt)

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

    def __call__(self):
        """
        returns
            abstractoutput, header
        """
        return AbstractOutput(self, None), self.header

    @abc.abstractmethod
    def execute(self):
        ...


class EndStage(Stage):
    """
    stage that ends a computation, and cannot be further linked from
    """
    ...


class AbstractOutput():
    """
    placeholder representing an operation applied to an input
    """
    
    def __init__(self, operation: Stage, inpt: "AbstractOutput|None"):
        self.operation = operation
        self.input = inpt
        self._cache = {}
    
    def evaluate(self):
        if "result" in self._cache:
            return self._cache["result"]
        # start stages have no input
        if isinstance(self.operation, StartStage):
            assert self.input is None
            result = self.operation.execute()
        # get input, apply operation
        else:
            rdd = self.input.evaluate()
            result = self.operation.execute(rdd)
        # save to cache and return
        self._cache["result"] = result
        return result




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
        return rdd


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
        return rdd.take(5)


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

