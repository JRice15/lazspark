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


