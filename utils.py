import laspy

import os, sys, re
import psutil
import time
import numpy as np
import pandas as pd



BACKEND = laspy.compression.LazBackend.LazrsParallel
COMPRESS = False

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

