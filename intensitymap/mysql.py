#处理excel文件
try:
    from tkinter import filedialog as fd
except:
    pass
import pandas as pd
import time
import pdb
import os
import codecs
import sys

sys.path.append('../')
# from sxgmodule.package import *
import sxgmodule.package as sxg

a=sxg.timetra('2023-11-09 16:19:55',0)
print(a)
b=sxg.timetra(float(1699517995.0),1)
print(b)