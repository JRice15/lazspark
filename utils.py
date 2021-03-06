import laspy

import os, sys, re
import psutil
import time
import numpy as np
import pandas as pd
from contextlib import contextmanager
import geopandas
import pyproj

try:
    import lazrs
    BACKEND = laspy.compression.LazBackend.LazrsParallel
except:
    BACKEND = laspy.compression.LazBackend.Laszip
COMPRESS = False

@contextmanager
def timethis():
    t1 = time.perf_counter()
    yield
    print("time:", time.perf_counter() - t1)


def np_to_laspy_pts(pts, point_format):
    data = laspy.point.record.PackedPointRecord.zeros(len(pts), point_format)
    data.x = pts[:,0]
    data.y = pts[:,1]
    data.z = pts[:,2]
    return data


def laspy_to_np_pts(pts):
    return np.stack([pts.x, pts.y, pts.z], axis=-1)


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

def pointcount(filename):
    with laspy.open(filename, "r", laz_backend=BACKEND) as reader:
        return reader.header.point_count

def sample_points(filename, n=100):
    with laspy.open(filename, "r", laz_backend=BACKEND) as reader:
        pts = reader.read_points(n)
        return laspy_to_np_pts(pts)



