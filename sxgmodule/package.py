#_*_coding:utf-8_*_
# 导入random模块，我们要制作的包要用
import subprocess
import time
import os
import sys
import pandas as pd
import datetime
import urllib.request
from urllib import parse
import pdb
from urllib.parse import urlparse,parse_qs,parse_qsl
import json

try:
    from tkinter import filedialog as fd

except:
    pass


# 定义自定义包模块的简单功能
def openfile():
    try:
        filepath = fd.askopenfilename(initialdir = "d://",title = '选择工程目录')
        dpath = os.path.dirname(filepath)
        file = os.path.basename(filepath)
    except:
        print('打开win窗口失败！')
        pass
    return(dpath,file)

def excelmatch(datadf1,datadf2):
    idlist1 = datadf1['id0'].tolist()
    idlist2 = datadf2['id'].tolist()
    tarindex = datadf1.values.tolist()
    out=[]
    for var in idlist2:
        try:
            out.append(tarindex[idlist1.index(var)])
        except:
            print('无匹配项')
            tempvar=[]
            for i in range(datadf1.columns.size):
                tempvar.append(0)
            out.append(tempvar)
    datadf2[datadf1.columns]=out
    return(datadf2)
        # datadf2.to_csv(os.path.join(savepath,savefile),encoding='gbk', index=False)
    
    
def cardate(carid,date):  #根据carid,date获取当天的tasklist
    cmd=('/mnt/d/code/adb_client/bin/adb_data ls /auto_car/'+carid+'/'+date+'/')
    # p1=subprocess.Popen(cmd.split())
    p=subprocess.Popen(cmd.split(),stdout=subprocess.PIPE)
    data_tuple = p.communicate()
    data_str = data_tuple[0].decode('utf-8')
    datalist=data_str.split('\n')
    del datalist[-1]
    tasklist=[]
    for var in datalist:
        taskvar=var.split('/')[-1]
        tasklist.append(taskvar)
    p.wait()
    # print(tasklist)
    return(tasklist)


# 根据carid与问题时间获取iso信息
def getcarmes(carid,timevar,carlist):
    datestr = timevar.split(' ')[0].replace('-','')
    timestr = timevar.split(' ')[1]
    # carlist = cardate(carid,datestr)
    # date_time = time.strptime(timevar , "%Y-%m-%d %H:%M:%S")
    date_str = timevar.replace('-','').replace(':','').replace(' ','')
    finaltime=[date_str]
    for var in carlist:
        timevarstr=var.split('_')[1]
        # timevar= time.strptime(timevarstr, "%Y%m%d%H%M%S")
        finaltime.append(timevarstr)
    timesort=sorted(finaltime)
    tartimeindex=timesort.index(date_str)
    if tartimeindex==0:
        targettask=carid+'_'+timesort[0]
    else:
        targettask=carid+'_'+timesort[tartimeindex-1]
    return(targettask)

def downloadverjson(taskname,savepath):
    taskname=taskname.replace(' ','')
    print('taskname:',taskname)
    carid=taskname.split('_')[0]
    datevar=taskname.split('_')[1][0:8]
    cmd_mkdir='mkdir -p '+savepath+'/'+taskname
    # print(cmd_mkdir.split())
    mkdir=subprocess.Popen(cmd_mkdir.split())
    cmd_v=('/mnt/d/code/adb_client/bin/adb_data get /auto_car/'+carid+'/'+datevar+'/'+taskname+'/carversion/cmptnode/realtime_version.json '+savepath)
    print(cmd_v)
    vjson=subprocess.Popen(cmd_v.split())
    vjson.wait()
    print('下载地图版本文件成功！')

def timetra(timein,str):  #str=1,时间戳转北京时间，str=0,北京时间转时间戳
    out=''
    if str==0:
        timein=timein[0:19]
        timestr=timein.strip()
        if timein[4]=='-':
            timearr=time.strptime(timestr,'%Y-%m-%d %H:%M:%S')
        else:
            timearr=time.strptime(timestr,'%Y/%m/%d %H:%M:%S')
        out = time.mktime(timearr)
    elif str==1:
        timein= int(timein)
        out = datetime.datetime.fromtimestamp(timein)
    return(out)

def readmrdr(icafeid):
        urlstr='http://mrdr.icafeapi.baidu-int.com/api/spaces/MRDR/cards?u=suxiaogang&pw=VVVa05jBYu9aDLJIKYtDCMY3759p6jk6rZi&iql=searchstr'
        out=[]
        qustr='编号 = '+str(icafeid)
        result=parse.quote(qustr)
        urlvar = urlstr.replace('searchstr',result)
        print(urlvar)
        try:
            req = urllib.request.urlopen(urlvar)
            datastr=req.read().decode()
            datadict=json.loads(datastr)
            maindata=datadict['cards'][0]['properties']
            temp=[icafeid]
            for i in range(len(maindata)):
                if maindata[i]['propertyName']=='问题时间点':
                    temp.append(maindata[i]['value'])
                elif maindata[i]['propertyName']=='问题定位工具issuefinder':
                    temp.append(maindata[i]['value'])
                elif maindata[i]['propertyName']=='高精地图坐标':
                    cor=maindata[i]['value']
                    cordict=json.loads(cor)
                    temp.append(cordict['x'])
                    temp.append(cordict['y'])
            out=temp 
        except:
            print(str(icafeid)+'获取失败！')
            out=[icafeid,'','','','']
        # columnsname=['icafeid','x','y','问题时间点','issuefinder']
        # outdf=pd.DataFrame(out,columns=columnsname)
        return(out)


def readmrdr2(icafeid):
        urlstr='http://mrdr.icafeapi.baidu-int.com/api/spaces/MRDR/cards?u=suxiaogang&pw=VVVa05jBYu9aDLJIKYtDCMY3759p6jk6rZi&iql=searchstr'
        qustr='编号 = '+str(icafeid)
        result=parse.quote(qustr)
        urlvar = urlstr.replace('searchstr',result)
        temp={}
        try:
            req = urllib.request.urlopen(urlvar)
            datastr=req.read().decode()
            datadict=json.loads(datastr)
            maindata=datadict['cards'][0]['properties']
            for i in range(len(maindata)):
                if maindata[i]['propertyName']=='问题定位工具issuefinder':
                    temp['问题定位工具issuefinder']=maindata[i]['displayValue'].replace('amp;','')
                elif maindata[i]['propertyName']=='高精地图坐标':
                    temp['高精地图坐标']=maindata[i]['displayValue']
        except:
            print(str(icafeid)+'获取失败！')
            temp['地图-导致问题模块及要素类别']=''
            temp['高精地图坐标']=''
        # columnsname=['icafeid','x','y','问题时间点','issuefinder']
        # outdf=pd.DataFrame(out,columns=columnsname)
        # out=temp['问题定位工具issuefinder']
        # print(out)
        return(temp)

def readroad(icafeid):
        urlstr='http://icafeapi.baidu-int.com/api/spaces/road-change/cards?u=suxiaogang&pw=VVVa05jBYu9aDLJIKYtDCMY3759p6jk6rZi&iql=searchstr'
        temp={}
        temp['icafeid']=icafeid
        qustr='icafeid = '+str(icafeid)
        result=parse.quote(qustr)
        urlvar = urlstr.replace('searchstr',result)
        print(urlvar)
        try:
            req = urllib.request.urlopen(urlvar)
            datastr=req.read().decode()
            datadict=json.loads(datastr)
            maindata=datadict['cards'][0]['properties']
            for i in range(len(maindata)):
                if maindata[i]['propertyName']=='问题定位工具issuefinder':
                    temp['问题定位工具issuefinder']=maindata[i]['displayValue']
                elif maindata[i]['propertyName']=='地图-导致问题模块及要素类别':
                    temp['地图-导致问题模块及要素类别']=maindata[i]['displayValue']
                elif maindata[i]['propertyName']=='变更or报出类型':
                    temp['变更or报出类型']=maindata[i]['displayValue']
                elif maindata[i]['propertyName']=='是否算法ODD内':
                    temp['是否算法ODD内']=maindata[i]['displayValue']
                elif maindata[i]['propertyName']=='云代驾是否报出':
                    temp['云代驾是否报出']=maindata[i]['displayValue']
            print('out:',temp)
        except:
            print(str(icafeid)+'获取失败')
            temp['问题定位工具issuefinder']=''
            temp['地图-导致问题模块及要素类别']=''
            temp['变更or报出类型']=''
            temp['是否算法ODD内']=''
            temp['云代驾是否报出']=''
        # columnsname=['icafeid','x','y','问题时间点','issuefinder']
        # outdf=pd.DataFrame(out,columns=columnsname)
        return(temp)


def routecon(winpath): #windows路径转为wsl路径
    print(winpath)
    # str='d:\现实变更\数据集\主线case\out_主线case0108-0114.xlsx'
    wslpath=winpath.replace("\\","/").replace('d:','/mnt/d/')
    print(winpath,wslpath)
    return(wslpath)
