# LazSpark


> LIDAR data has a variety of uses in forestry, urban planning, cartography, and self-driving cars, to name a few. Since LIDAR is commonly used to map large areas, and captures the 3-dimensional structure of those areas, LIDAR files can quickly grow to many gigabytes in size, even when compressed with a LIDAR-specific compression format. Manipulating these files with basic pointwise operations, such as filtering or coordinate reprojection, can thus be quite time consuming. As such, I developed a new package, LazSpark, that enables distributed processing of LIDAR files. It exposes a simple interface that allows distributed reading, processing, and writing in just a few lines of code, and as well as supports custom user-defined functions that operate on standard NumPy arrays. A simple pipeline was benchmarked on a 64 (logical) core machine, which produced a 25x speedup over the sequential processing approach, reducing a task that once took over an hour to just 2.5 minutes.


LazSpark is a distributed LIDAR-processing package for Python.

It was created as a project for CSC 369: Introduction to Distributed Computing, with Dr. Anderson

# Usage

## API
Two interfaces are exposed. One is an API, which can be used by importing `stages`. 
Examples of usage can be seen in the `demo_*.py` files.

Stages implemented are as follows:

StartStages (return rdd and header when called):
* Reader(filename, points_per_chunk)
* FakeReader(points_per_chunk)

OpStages:
* Decimate(n)
* Translate(x, y, z)
* Scale(x, y, z)
* Filter(min_x, min_y, min_z, max_x, max_y, max_z)
* Reproject(in_crs, out_crs)
* Lambda(f): `f` is a user-defined function that operated on an np.array of points

EndStages:
* Writer(filename, header, overwrite=False)
* Collect()
* Take(n)

## CLI
The other is a command-line interface that wraps the API, in `main.py`. It supports
the following arguments:
* --file FILE, -f FILE: input las/laz filename
* --outfile FILE: output las filename (default "out_pipeline.laz")
* --overwrite: set allowing overwrite of outfile to True
* --points_per_chunk N, --ppc N: points per chunk parameter to reader (default 1,000,000)
The pipeline can then be specified with positional arguments, where each is of the form:
```
stagename[=arg1,arg2,...]
```
Arguments to each stage can only be supplied positionally in this interface

For example, the following reads `a.laz`, translates all points by (2, 1, 4), and writes to `b.laz`:
```
$ python3 main.py --file a.laz --outfile b.laz translate=2,1,4
```

This hopefully results in output like so:
```
Stages:
  Reader
  Translate
  Writer('b.laz')
Executing EndStage Writer('b.laz')
time: 6.285911347000001                                                         
smaller.laz pointcount: 1056275
out_pipeline.laz pointcount: 1056275
```
