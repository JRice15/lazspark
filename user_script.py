import numpy as np
import stages
from utils import *


r1, header = stages.Reader("small.laz", 1_000_000)()

w = stages.Writer("out_user1.laz", header, overwrite=True)(r1)
c = stages.Collect()(r1)

pipeline = stages.Pipeline(c)

for output in pipeline.iter_outputs():
    print(output)


print(pointcount("small.laz"))
print(pointcount("out_user1.laz"))
