from pyspark import SparkConf
from pyspark.context import SparkContext
import numpy as np
import pandas as pd
import argparse
import laspy

# init globals
ARGS = argparse.Namespace()
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))



def make_file_rdd(filename):
    # create `max_workers` shards
    shards = list(range(ARGS.max_workers))

    with laspy.open(filename, "r") as reader:
        total_pts = reader.header.point_count
        print(reader.chunk_iterator(1000))



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file","-f", required=True)
    parser.add_argument("--max-workers", type=int)
    parser.parse_args(namespace=ARGS)

    rdd = make_file_rdd(ARGS.file)


if __name__ == "__main__":
    main()
