import numpy as np
import stages
from utils import *


inpt, header = stages.Reader("smaller.laz", 1_000)()

out1 = stages.Take(1, name="take-1")(inpt)

x = inpt
x = stages.Reproject("epsg:26911", "epsg:4326")(x)
x = stages.Reproject("epsg:4326", "epsg:26911")(x)
out2 = stages.Take(1, name="take-2")(x)

print(out1[0])
print(out2[0])