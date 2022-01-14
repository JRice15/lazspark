"""
API interface
"""

import laspy

import os, sys, re
import argparse
import psutil
import time
import numpy as np
import pandas as pd
import ilock
import abc

import pyspark
from pyspark import SparkConf
from pyspark.context import SparkContext

from utils import *

CACHE_EMPTY_FLAG = "-NO-RESULT-"


"""
Building blocks
"""

def is_rdd(x):
    return isinstance(x, pyspark.rdd.RDD)

class Stage(abc.ABC):
    """
    base stage class. override `execute`, and optionally `update_header` and `__init__`
    """

    _cached_result = CACHE_EMPTY_FLAG
    _check_output = True

    def __init__(self, name=None, **kwargs):
        self.name = self.__class__.__name__ if name is None else name
        if kwargs:
            raise ValueError("Extra args: {}".format(kwargs))
        self._save_vars_cache = {}

    @abc.abstractmethod
    def __call__(self, *rdds):
        ...

    def __repr__(self):
        return self.name

    def _save_vars(self, *args, name=None):
        if name is None:
            raise TypeError("Must supply name to _save_vars")
        self._save_vars_cache[name] = args

    def _pop_vars(self, name):
        return self._save_vars_cache.pop(name)

    @abc.abstractmethod
    def execute(self, rdd):
        """
        apply operations to an RDD
        """
        ...



class OpStage(Stage):
    """
    stages that operates on an RDD
    """

    def __call__(self, *rdds):
        if not all(is_rdd(x) for x in rdds):
            raise ValueError(f"All inputs to {self.name} must be RDDs")
        result = self.execute(*rdds)
        if self._check_output and not is_rdd(result):
            raise ValueError(f"{self.name} must return an RDD")
        return result

class StartStage(Stage):
    """
    stage that begins computation
    """

    def __call__(self, *args):
        """
        returns
            rdd, header
        """
        rdd, header = self.execute(*args)
        if self._check_output and not is_rdd(rdd):
            raise ValueError(f"{self.name} must return an RDD")
        return rdd, header

    @abc.abstractmethod
    def execute(self):
        ...


class EndStage(Stage):
    """
    stage that ends a computation, and cannot be further linked from
    """

    def __call__(self, *rdds):
        print("Executing EndStage", self)
        if not all(is_rdd(x) for x in rdds):
            raise ValueError(f"All inputs to {self.name} must be RDDs")
        return self.execute(*rdds)




"""
Beginning and End stages
"""

class FakeReader(StartStage):
    """
    simulate a reader, but just use a predefined np.array as the source
    """

    def __init__(self, points_per_chunk, **kwargs):
        super().__init__(**kwargs)
        self.points_per_chunk = points_per_chunk
        self.sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))


    def execute(self, array):
        ppc = self.points_per_chunk
        shards = list(range(0, len(array), ppc))
        rdd = self.sc.parallelize(shards)
        def get_chunk(start):
            return array[start:start+ppc]
        return rdd.map(get_chunk), None


class Reader(StartStage):
    """
    Read `filename` in parallel, with `points_per_chunk` points read into each worker
    target ppc: total_points / ppc == number of cores available
    """

    def __init__(self, filename, points_per_chunk, **kwargs):
        super().__init__(**kwargs)
        self.filename = filename
        self.points_per_chunk = points_per_chunk
        with laspy.open(self.filename, "r") as reader:
            self.total_pts = reader.header.point_count
            self.header = reader.header

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
    """
    Write points to `outfile`, with `header` (usually provided by the Reader at 
    the start of the pipeline)
    This stage is a bottleneck, because writing cannot be parallelized. This is
    ensured by a global lock implemented in iLock.
    """

    def __init__(self, outfile, header, overwrite=False, **kwargs):
        super().__init__(**kwargs)
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
    """
    collect all chunks from this pipeline
    WARNING: This may exhaust memory
    """

    def execute(self, rdd):
        arrays = rdd.collect()
        return np.concatenate(arrays, axis=0)


class Take(EndStage):
    """
    take the first N chunks from this pipeline
    """

    def __init__(self, n=2, **kwargs):
        super().__init__(**kwargs)
        self.n = n

    def execute(self, rdd):
        n = self.n
        arrays = rdd.take(n)
        return np.concatenate(arrays, axis=0)



"""
intermediate operations

For many of these, there is a strange design pattern in the execute() method,
when we do something like `f = self.f`. Why not just pass self.f? Because
inside rdd.map, any references used are serialized to the workers, so the
`self` reference causes the stage to try and serialize its whole class, which
we don't need to do at the very least, and usually fails as well
"""


class Lambda(OpStage):
    """
    stage that utilizes an arbitrary (stateless) function
    the function must accept an np.array as the first argument.
    If you want to supply additional arguments to `f`, pass them in  `f_args` 
    and/or `f_kwargs`
    """

    def __init__(self, f, f_args=None, f_kwargs=None, **kwargs):
        super().__init__(**kwargs)
        self.f = f
        self.f_args = () if f_args is None else f_args
        self.f_kwargs = {} if f_kwargs is None else f_kwargs
    
    def execute(self, rdd):
        f = self.f
        args = self.f_args
        kwargs = self.f_kwargs
        return rdd.map(lambda x: f(x, *args, **kwargs))


class Reproject(OpStage):
    """
    reproject from coordinate system A to B
    """

    def __init__(self, from_crs, to_crs, **kwargs):
        super().__init__(**kwargs)
        self.from_crs = from_crs
        self.to_crs = to_crs
    
    def execute(self, rdd):
        def reproject(xyz, from_crs, to_crs):
            """
            transfrom coordinate reference system for an np.array of shape (n,3)
            """
            transformer = pyproj.Transformer.from_crs(from_crs, to_crs)
            projected = transformer.transform(xyz[:,0], xyz[:,1], xyz[:,2])
            return np.stack(projected, axis=-1)
        from_crs = self.from_crs
        to_crs = self.to_crs
        return rdd.map(lambda x: reproject(x, from_crs, to_crs))


class Decimate(OpStage):
    """
    keep every Nth point (subject to fewer points than expected when the N is 
    a substantial fraction of the reader's points_per_chunk)
    """

    def __init__(self, n, **kwargs):
        super().__init__(**kwargs)
        self.n = n
    
    def execute(self, rdd):
        n = self.n
        return rdd.map(lambda x: x[::n])

class Filter(OpStage):
    """
    filter points based on X, Y, and/or Z conditions. Conditions not given
    will default to no filtering in that dimension and direction
    """

    def __init__(self, min_x=np.NINF, min_y=np.NINF, min_z=np.NINF, 
            max_x=np.inf, max_y=np.inf, max_z=np.inf, **kwargs):
        super().__init__(**kwargs)
        self._save_vars(min_x, min_y, min_z, max_x, max_y, max_z, name="minmax")
    
    def execute(self, rdd):
        min_x, min_y, min_z, max_x, max_y, max_z = self._pop_vars("minmax")
        def filterer(pts):
            return pts[
                (pts[:,0] >= min_x) & \
                (pts[:,0] <= max_x) & \
                (pts[:,1] >= min_y) & \
                (pts[:,1] <= max_y) & \
                (pts[:,2] >= min_z) & \
                (pts[:,2] <= max_z)
            ]
        return rdd.map(filterer)

class Translate(OpStage):
    """
    move points by a constant factor in each dimension
    """

    def __init__(self, x=0, y=0, z=0, **kwargs):
        super().__init__(**kwargs)
        self._save_vars(x, y, z, name="xyz")
    
    def execute(self, rdd):
        x, y, z = self._pop_vars("xyz")
        def translate(pts):
            return pts + np.array([x, y, z])
        return rdd.map(translate)

class Scale(OpStage):
    """
    scale points by a constant factor in each dimension
    """

    def __init__(self, x=1, y=1, z=1, **kwargs):
        super().__init__(**kwargs)
        self._save_vars(x, y, z, name="xyz")
    
    def execute(self, rdd):
        x, y, z = self._pop_vars("xyz")
        def scale(pts):
            return pts * np.array([x, y, z])
        return rdd.map(scale)


class Split(OpStage):
    """
    splits data into many rdds with user-defined function.
    args:
        f: function that accepts an np.array as the first argument, returns array of keys corresponding to each point
        keys: list of possible keys f can return
    returns:
        dict: mapping each key to an rdd
    If you want to supply additional arguments to `f`, pass them in  `f_args` 
    and/or `f_kwargs`.
    """

    _check_output = False

    def __init__(self, f, keys, f_args=None, f_kwargs=None, **kwargs):
        super().__init__(**kwargs)
        self.f = f
        self.f_args = () if f_args is None else f_args
        self.f_kwargs = {} if f_kwargs is None else f_kwargs
        self.keys = keys
    
    def execute(self, rdd):
        f = self.f
        args = self.f_args
        kwargs = self.f_kwargs
        keyed_rdd = rdd.map(lambda x: (x, f(x, *args, **kwargs)))

        def np_filter(arr, key_arr, target_key):
            cond = (key_arr == target_key)
            return arr[cond]
        def map_it(key):
            """this little function is required so that the value of 'key' is not cached after the first loop iteration"""
            return keyed_rdd.map(lambda x: np_filter(x[0], x[1], key))

        rdds = {k:map_it(k) for k in self.keys}
        return rdds
