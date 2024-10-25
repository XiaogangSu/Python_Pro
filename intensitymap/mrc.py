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

class main():
    def __init__(self):
        print('开始处理！')

    def pro(self):   
        # path,file = sxg.openfile()
        path='/mnt/d/定位'
        file = '故障清单-定位图层异常.xlsx'
        saveversionpath=os.path.join(path,file[0:-5])
        cmd_mkdir='mkdir '+saveversionpath
        # col=['故障日期','车辆ID','系统版本','问题根因','故障时间']
        # datadf=pd.read_excel(os.path.join(path,file),usecols=col)
        # datalist=datadf.values.tolist()
        # tasklist=[]
        # for var in datalist:
        #     try:
        #         datevar = var[0].replace('-','')
        #         carid = var[1]
        #         timevar = var[4]
        #         taskvars=sxg.cardate(carid,datevar)
        #         tartask = sxg.getcarmes(carid,timevar,taskvars)
        #         tasklist.append(tartask)
        #     except:
        #         print(var)
        #         pass
        # datadf['taskname']=tasklist
        # datadf.to_csv(os.path.join(path,file+'taskname.csv'),index=False)
        
        datadf2=pd.read_csv(os.path.join(path,file+'taskname.csv'))
        datalist2=datadf2.values.tolist()
        finallist=[]
        for var in datalist2:
            tasknamevar=var[5]
            if os.path.exists(os.path.join(saveversionpath,tasknamevar)):
                pass
            else:
                sxg.downloadverjson(tasknamevar,os.path.join(saveversionpath,tasknamevar))


if __name__=='__main__':
    ex=main()
    ex.pro()