import numpy as np
import laspy
import os, sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import stages
from utils import *

"""
regular versions
"""

def translate(pts, x, y, z):
    return pts + np.array([x, y, z])

def filterer(pts, min_x=np.NINF, min_y=np.NINF, min_z=np.NINF, max_x=np.inf, max_y=np.inf, max_z=np.inf):
    return pts[
        (pts[:,0] >= min_x) & \
        (pts[:,0] <= max_x) & \
        (pts[:,1] >= min_y) & \
        (pts[:,1] <= max_y) & \
        (pts[:,2] >= min_z) & \
        (pts[:,2] <= max_z)
    ]

def decimate(x, n):
    return x[::n]

def reproject(xyz, from_crs, to_crs):
    transformer = pyproj.Transformer.from_crs(from_crs, to_crs)
    projected = transformer.transform(xyz[:,0], xyz[:,1], xyz[:,2])
    return np.stack(projected, axis=-1)


"""
tests to run
"""

def run_simple(infile, outfile, points_per_chunk, from_crs, to_crs):
    with laspy.open(infile, "r") as reader:
        with laspy.open(outfile, "w", header=reader.header, do_compress=COMPRESS, laz_backend=BACKEND) as writer:
            for pts in reader.chunk_iterator(points_per_chunk):
                pts = laspy_to_np_pts(pts)

                pts = reproject(pts, from_crs, to_crs)
                pts = translate(pts, 2, 0.5, 2)
                pts = filterer(pts, min_z=3)
                pts = decimate(pts, 3)

                pts = np_to_laspy_pts(pts, reader.header.point_format)
                writer.write_points(pts)



def run_parallel(infile, outfile, points_per_chunk, from_crs, to_crs):
    x, header = stages.Reader(infile, points_per_chunk)()

    x = stages.Reproject(from_crs, to_crs)(x)
    x = stages.Translate(2, 0.5, 2)(x)
    x = stages.Filter(min_z=3)(x)
    x = stages.Decimate(3)(x)

    stages.Writer(outfile, header)(x)


def main():
    infile = sys.argv[1]
    from_crs = sys.argv[2]
    to_crs = "epsg:26911"
    outfile = ".temp_out.las"
    points_per_chunk = 10_000_000

    if os.path.exists(outfile):
        os.remove(outfile)

    t0 = time.perf_counter()
    run_parallel(infile, outfile, points_per_chunk, from_crs, to_crs)
    t1 = time.perf_counter()
    pointcount_parallel = pointcount(outfile)
    os.remove(outfile)
    t_parallel = t1 - t0

    t0 = time.perf_counter()
    run_simple(infile, outfile, points_per_chunk, from_crs, to_crs)
    t1 = time.perf_counter()
    pointcount_simple = pointcount(outfile)
    os.remove(outfile)
    t_simple = t1 - t0

    print("simple time:", t_simple, "pointcount:", pointcount_simple)
    print("parallel time:", t_parallel, "pointcount:", pointcount_parallel)
    if t_parallel <= t_simple:
        fmt = "{}x speedup"
    else:
        fmt = "{}x slowdown"
    print(fmt.format(t_parallel / t_simple))
    if pointcount_parallel != pointcount_simple:
        print("Error: Pointcounts differ!")

if __name__ == "__main__":
    main()
