"""
This file defines a command-line interface to the program
"""

import abc
import argparse
import os
import re
import sys
import time
import inspect

import ilock
import laspy
import numpy as np
import pandas as pd
import psutil

import stages
from utils import pointcount, timethis

def numberify(x):
    try:
        return float(x)
    except:
        return x


def run_spark_pipeline(ARGS):
    print("Stages:")
    reader = stages.Reader(ARGS.file, ARGS.points_per_chunk)
    print(" ", reader)
    x, header = reader()

    STAGE_MAP = {k.lower():v for k,v in vars(stages).items() if inspect.isclass(v) and (issubclass(v, stages.Stage) or isinstance(v, stages.Stage))}
    for pipe in ARGS.pipeline:
        if "=" in pipe:
            pipe, args = pipe.split("=")
            args = [numberify(x) for x in args.split(",")]
        else:
            args = []
        
        stage = STAGE_MAP[pipe.lower()]
        stage = stage(*args)
        print(" ", stage)
        x = stage(x)
    
    writer = stages.Writer(ARGS.outfile, header, overwrite=ARGS.overwrite)
    print(" ", writer)
    writer(x)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file","-f", required=True)
    parser.add_argument("--outfile", default="out_pipeline.laz")
    parser.add_argument("--overwrite",action="store_true")
    parser.add_argument("--points-per-chunk","--ppc", type=int, default=1_000_000)
    parser.add_argument("pipeline",nargs="+")
    ARGS = parser.parse_args()

    if len(ARGS.pipeline) == 0:
        raise ValueError("No pipeline stages provided! Supply their names as positional arguments")

    with timethis():
        run_spark_pipeline(ARGS)

    print(ARGS.file, "pointcount:", pointcount(ARGS.file))
    print(ARGS.outfile, "pointcount:", pointcount(ARGS.outfile))


if __name__ == "__main__":
    main()
