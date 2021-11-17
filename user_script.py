import numpy as np
import stages
from utils import *


r1, header = stages.Reader("smaller.laz", 1_000_000)()

stages.Writer("out_user1.laz", header, overwrite=True)(r1)
results = stages.Collect()(r1)

print(results)

print(pointcount("small.laz"))
print(pointcount("out_user1.laz"))
