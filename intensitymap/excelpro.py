
# -*- coding: utf-8 -*-
# 处理excel文件
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
        print('开始处理')
        # try:
        #     filepath = fd.askopenfilename(initialdir = "d://",title = '选择工程目录')
        #     # print(filepath)
        #     self.path = os.path.dirname(filepath)
        #     self.file = os.path.basename(filepath)
        # except:
        #     pass
        # self.path,self.file=sxg.openfile()
        
    
    def readexcel(self,path,file): 
        datadf_sheet1=pd.read_excel(os.path.join(path,file),sheet_name='Sheet1')
        datadf_sheet2=pd.read_excel(os.path.join(path,file),sheet_name='Sheet2')
        return(datadf_sheet1,datadf_sheet2)

    def datamatch(self,datadf1,datadf2,path,file):
        idlsit1 = datadf1['CaseID'].tolist()
        idlist2 = datadf2['编号'].tolist()
        out=[]
        for var in idlist2:
            if var in idlsit1:
                out.append(1)
            else:
                out.append(0)
        datadf2['match']=out
        datadf2.to_csv(os.path.join(path,'match_'+file+'.csv'))
    
    def pro(self):
        # path,file=sxg.openfile()
        path,file = os.path.split(sys.argv[1])
        data1,data2=self.readexcel(path,file)
        out=sxg.excelmatch(data1,data2)
        out.to_csv(os.path.join(path,file+'.csv'),index=False)
        print(out.head())
        print('结束处理')


    def timetra(self,path,file,str):  #str=0,时间戳转北京时间，str=1,北京时间转时间戳
        filepath = path+file
        f=open(filepath)
        data=f.readlines()
        out=[]
        if str==1:
            for var in data:
                timestr=var.strip()
                timearr=time.strptime(timestr,'%Y/%m/%d %H:%M:%S')
                timestamp = time.mktime(timearr)
                out.append([timestr,timestamp])
                print(timestr)
        elif str==0:
            for var in data:
                timestr=var.strip()
                timestamp = sxg.timetra(timestr,1)
                out.append([timestr,timestamp])
                print(timestr)
        outdf = pd.DataFrame(out)
        outdf.to_csv('./time_out.csv')

    def pro2(self):
        # path,file = sxg.openfile()
        path,file = os.path.split(sys.argv[1])
        # path='/mnt/d/现实变更/数据集/高质量case'
        # file = '召回-1018.csv'
        # self.path='D:/现实变更/数据集/normal数据集'
        # self.file='1011_误报.txt'
        # print(self.path,self.file)
        #召回指标
        datadf_zh = pd.read_csv(os.path.join(path,file),encoding='gbk')
        outtype_zh = {'几何变更':['可配置-车道线几何变更'],'栅栏变更':['可配置-栅栏变更'],'线型变更':['可配置-线型变更'],'停止线变更':['可配置-停止线新增','可配置-停止线变更'],'行驶方向变更':['可配置-行驶方向变更'],'待转区变更':['可配置-待转区变更'],'安全岛变更':['可配置-安全岛变更'],'人行横道变更':['可配置-人行横道变更'],'拓扑变更':['可配置-道路拓扑变更']}
        zh_num=[]
        numlist = datadf_zh['通过率']
        for var in numlist:
            print(var)
            var1 = var.split('(')
            var2 = var1[1].replace(')','')
            ab=var2.split('/')
            a = int(ab[0])
            b = int(ab[1])
            zh_num.append([a,b])
        numcol = ['a','b']
        numdf = pd.DataFrame(zh_num,columns=numcol)
        datadf_zh1 = pd.concat([datadf_zh,numdf],axis=1)
        print(datadf_zh1)
        zhout=[]
        for var in outtype_zh.keys():
            print(var)
            typevar=outtype_zh[var]
            vardata = datadf_zh1[datadf_zh1['Metric名称'].isin(typevar)]
            suma = vardata['a'].sum()
            sumb = vardata['b'].sum()
            a_b ="{:.2%}".format(suma/sumb) 
            abstr=str(a_b)+'('+str(suma)+'/'+str(sumb)+')'
            zhout.append([var,suma,sumb,abstr])
        print(zhout)
        outcol = ['变更类型','报出次数','总次数','ab']
        zhoutdf=pd.DataFrame(zhout,columns=outcol)
        zhoutdf.to_csv(os.path.join(path,'out_'+file),encoding='gbk', index=False)
    
    # def precision_ana(self,datadf):
    #     tarindex=['几何变更','栅栏变更','线型变更','停止线变更','行驶方向变更','待转区变更','安全岛变更','人行横道变更','拓扑变更']
    #     print(datadf)
    #     dftype=datadf[]
    #     out=[]
    #     for var in tarindex:



    def pro3(self):
        # path,file = sxg.openfile()
        path,file = os.path.split(sys.argv[1])
        # path='/mnt/d/现实变更/数据集/normal数据集'
        # file='1018_误报.txt'
        f = codecs.open(os.path.join(path,file),'r','UTF-8')
        data=f.readlines()
        data1=[]
        for var in data:
            tempvar=var.replace("\r\n", "")
            data1.append(tempvar)
        data2=[]
        for i in range(len(data1)):
            if '百公里误报次数' in data1[i]:
                data2.append(data1[i:i+3])
        columnstr=['报出类型','n1','n2']
        data2df=pd.DataFrame(data2,columns=columnstr)
        # out=self.precision_ana(data2df)
        # pdb.set_trace()
        print(os.path.join(path+file+'.csv'))
        data2df.to_csv(os.path.join(path,file+'.csv'),encoding='gbk', index=False,)
            
    def clear3(self):
        path = '/mnt/d/现实变更/数据集/高质量case'
        file = '全量导出-20231013-1697189485752.xlsx'
        colu = ['CaseID','AdsID','OV链接','可配置-车道线新增(存在为1)','可配置-车道线删除(存在为1)','可配置-非路口车道线几何类变更(存在为1)','可配置-路口车道线几何类变更(存在为1)','可配置-非路口车道线线型变更(存在为1)','可配置-路口车道线线型变更(存在为1)']
        datadf=pd.read_excel(os.path.join(path,file),usecols=colu)
        datadf.to_excel(os.path.join(path,'out_'+file),index=False)
        print(datadf.head())


if __name__=='__main__':
    ex=main()
    ex.pro()
    # ex.timetra('./','time.txt',1)
    # ex.pro2()  #现实变更召回离线指标整理
    # ex.pro3()  #现实变更误报离线指标整理
    # ex.clear3()  #临时程序  
    print('处理结束！')
