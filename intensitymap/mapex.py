#从txt文件中提取要素几何信息
import os
import sys
import json
import pandas as pd
import pdb
sys.path.append('../')
# from sxgmodule.package import *
import sxgmodule.package as sxg


class main():
    def __init__(self):
        print('开始处理！')
    
    def readtxt(self):
        pass

    def pro1(self):
        (path,file)=os.path.split(sys.argv[1])
        print(path,file)
        files=open(sys.argv[1],'r')
        content=files.read()
        lines=content.split('\n')
        tunnelindex=[]
        start=[]
        allnum=[]
        for i in range(len(lines)):
            if 'TUNNEL' in lines[i]:
                # print(i,lines[i])
                tunnelindex.append(i)
                allnum.append(i)
            if lines[i] == 'roads {':
                # print(i,lines[i])
                start.append(i)
                allnum.append(i)
        tdf=pd.DataFrame(tunnelindex,columns=['tunnel'])
        sdf=pd.DataFrame(start,columns=['start'])
        adf=pd.DataFrame(allnum,columns=['all'])
        tdf.to_csv(os.path.join(path,'tunnel_'+file+'.csv'))
        sdf.to_csv(os.path.join(path,'start_'+file+'.csv'))
        adf.to_csv(os.path.join(path,'all_'+file+'.csv'))
        startend=[]
        for var in tunnelindex:
            a=allnum.index(var)
            print(a,allnum[a-1],allnum[a+1])
            startend.append([allnum[a-1],allnum[a+1]-1])
        stdf=pd.DataFrame(startend,columns=['s','e'])
        stdf.to_csv(os.path.join(path,'startend_'+file+'.csv'))
        fileout=open(os.path.join(path,'out_'+file+'.txt'),'w')
        for var in startend:
            s=var[0]
            e=var[1]
            for i in range(s,e+1):
                fileout.write(lines[i]+'\n')


    def pro2(self):
        (path,file)=os.path.split(sys.argv[1])
        print(path,file)
        files=open(sys.argv[1],'r')
        content=files.read()
        lines=content.split('\n')
        xs=[]
        ys=[]
        for var in lines:
            print(var)
            if 'x:' in var:
                xs.append(float(var.split(':')[1]))
            elif 'y:' in var:
                ys.append(float(var.split(':')[1]))
        xydict={}
        xydict['x']=xs
        xydict['y']=ys
        xydf=pd.DataFrame.from_dict(xydict)
        xydf.to_csv(os.path.join(path,'out_'+file+'.csv'))



if __name__=='__main__':
    ex=main()
    # ex.pro1()
    ex.pro2()