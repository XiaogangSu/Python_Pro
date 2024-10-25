# normal数据集处理

import pandas as pd
import os
import sys
import urllib.request
from urllib import parse
import pdb
from urllib.parse import urlparse,parse_qs,parse_qsl
import json
sys.path.append('../')
# from sxgmodule.package import *
import sxgmodule.package as sxg

class main():
    def __init__(self):
        print('开始处理！')

    def readlog(self,path,file):
        fn=open(path+file,'r')
        lines=fn.readlines()
        adsidlist=[]
        hdmaplist=[]
        typelist=[]
        starttimelist=[]
        endtimelist=[]
        Xposelist=[]
        Yposelist=[]
        for i in range(len(lines)):
            if 'ads_id' in lines[i]:
                adsidlist.append(int(lines[i].split(':')[1]))
                
                hdmaplist.append(lines[i+1].split(':')[1].strip('\n'))
                
                typelist.append(lines[i+2].split(':')[1].strip('\n'))
                
                timestr = lines[i+3].split(':')[1].strip('\n')
                timestr1=timestr.replace('[','').replace(']','').split(',')
                starttimelist.append(float(timestr1[0]))
                endtimelist.append(float(timestr1[1]))
                
                posestr=lines[i+4].split(':')[1].strip('\n')
                posestr1 = posestr.replace('[','').replace(']','')
                res=posestr1.split(',')
                Xposelist.append(float(res[0]))
                Yposelist.append(float(res[1]))
        # print(len(adsidlist),len(hdmaplist),len(typelist),len(starttimelist),len(endtimelist),len(Xposelist),len(Yposelist))
        # print(Xposelist,Yposelist)
        out=[]
        for i in range(len(adsidlist)):
            out.append([adsidlist[i],hdmaplist[i],typelist[i],starttimelist[i],endtimelist[i],Xposelist[i],Yposelist[i]])
        print(out[0:3])
        index = ['adsid','hdmap','type','stime','etime','X','Y']
        outdf = pd.DataFrame(out,columns=index)
        outdf.to_csv(path+'out_'+file+'.csv',index=False)


    def pro(self):
        print('结束处理！')
        self.readlog('/mnt/d/Data/现实变更/数据集/normal数据集/chongqing_recheck/','change_result.txt')

    def badcasepro(self):
        # path = '/mnt/d/Data/现实变更/数据集/高质量case/入库/'
        # file = '仿真入库-1215.xlsx'
        path,file = os.path.split(sys.argv[1])
        datadf=pd.read_excel(os.path.join(path,file))
        index = datadf.columns.tolist()
        outputlist=['通过','未通过']
        datadf_ex = datadf[datadf['运行结果'].isin(outputlist)].copy()
        metriclistindex=index[8:]
        out=['场景id','地图区域','问题id','OV链接']
        all = out+metriclistindex
        metriclist = datadf_ex[all].values.tolist()
        badout=[]
        for i in range(len(metriclist)):
            var=metriclist[i]
            if '未通过' in var:
                badout.append(var)
        outdf=pd.DataFrame(badout,columns=all)
        # print(outdf.head())
        anaout=[]
        for var in metriclistindex:
            print(var)
            colu=out.append(var)
            vardf=datadf_ex[~datadf_ex[var].isna()]
            vardf.to_csv(os.path.join(path+'/badcase_out/',var+'.csv'),columns=out,index=False,encoding='gbk')
            badnum=len(datadf_ex[datadf_ex[var]=='未通过'])
            anaout.append([var,len(vardf),badnum])
        analist=['变更类型','总数','未通过数']
        anaoutdf=pd.DataFrame(anaout,columns=analist)
        anaoutdf.to_csv(os.path.join(path,'out.csv'),index=False,encoding='gbk')
        outdf.to_csv(os.path.join(path,file+'_badcase.csv'),index=False,encoding='gbk')
    
    def badcasepro2(self):
        path,file = os.path.split(sys.argv[1])
        colunmsstr=['场景id','问题id','运行结果','可配置-人行横道变更','可配置-栅栏变更','可配置-路口车道线几何类变更','可配置-路口车道线线型变更','可配置-车道线几何变更','可配置-线型变更','可配置-路牙变更','可配置-非路口车道线几何类高危变更','可配置-车道线新增','可配置-车道线删除','可配置-非路口车道线几何类变更','可配置-非路口车道线线型变更','可配置-安全岛变更','可配置-停止线变更','可配置-停止线新增','可配置-行驶方向变更','可配置-待转区变更']
        datadf=pd.read_excel(os.path.join(path,file),usecols=colunmsstr)
        index = datadf.columns.tolist()
        outputlist=['通过','未通过']
        datadf_ex = datadf[datadf['运行结果'].isin(outputlist)].copy()
        caseid_list=datadf_ex['问题id'].tolist()
        out=[]
        index=['icafeid','x','y','问题时间点','issuefinder','icafeid2','地图-变更类型','报出内容','变更类型','是否ODD内','是否报出','iso','类型','是否通过']
        for var in caseid_list:
            print(var)
            datavar = datadf_ex[datadf_ex['问题id']==var]
            mrdrout=sxg.readmrdr(var)+sxg.readroad(var)
            datavarnonull = datavar.dropna(axis=1)
            column_var=datavarnonull.columns
            if len(column_var)==0:
                mrdrout=mrdrout+['','']
            else:
                type='||'.join(column_var[3:])
                # print(datavarnonull.values.tolist())
                yn='||'.join(datavarnonull.values.tolist()[0][3:])
                mrdrout=mrdrout+[type,yn]
            out.append(mrdrout)
        outdf=pd.DataFrame(out,columns=index)
        outdf.to_excel(os.path.join(path,'out_'+file))

    def pro3(self):
        path,file = os.path.split(sys.argv[1])
        datadf=pd.read_excel(os.path.join(path,file))
        icafeidlist=datadf['icafeid'].tolist()
        out=[]
        for var in icafeidlist:
            print(var)
            varout=sxg.readmrdr(var)
            out.append(varout)
            # print(out)
        columns_str=['icafeid','x','y','问题时间点','issuefinder']
        outdf=pd.DataFrame(out,columns=columns_str)
        outdf.to_excel(os.path.join(path,'out_'+file))

if __name__=='__main__':
    ex = main()
    # ex.pro()
    # ex.badcasepro2()
    # ex.pro3()