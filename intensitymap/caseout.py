# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os
import pdb
import datetime
import time
import sys
import json
from collections import Counter
import math
sys.path.append('../')
# from sxgmodule.package import *
import sxgmodule.package as sxg

class main():
    def __init__(self):
        print('开始处理！')
        # self.path='/mnt/d/现实变更/数据集/主线case/主线case导出/'
        # (self.weekcasepath,self.weekcasefile)=('/mnt/d/现实变更/数据集/主线case/','主线case1120-1126.xlsx')
    
    def readcase(self,path):
        filelist = os.listdir(path)
        outlist=[]
        usecolslist=['编号','标题','是否重复反馈','问题时间点','问题定位工具issuefinder']
        outdf = pd.DataFrame(data=None,columns=usecolslist)
        for var in filelist:
            print(var)
            vardf = pd.read_excel(os.path.join(path,var),usecols=usecolslist,engine='openpyxl')
            # outdf.append(vardf)
            outlist.append(vardf)
        outdf=pd.concat(outlist)
        print(len(outdf))
        return(outdf)
    
    def readcase2(self,path):
        print(path)
        filelist = os.listdir(path)
        outlist=[]
        outdf = pd.DataFrame(data=None,columns=['icafeid'])
        for var in filelist:
            print(var)
            vardf = pd.read_excel(os.path.join(path,var),usecols=['icafeid'],engine='openpyxl')
            # outdf.append(vardf)
            outlist.append(vardf)
        outdf=pd.concat(outlist)
        return(outdf)
    
    # weekcase 筛选
    def weekcase(self,path,file,hisdf):
        print(path,file)
        usecolslist=['编号','标题','是否重复反馈','问题时间点','地图-导致问题模块及要素类别','仿真是否发生碰撞-带反应','仿真是否发生碰撞-带反应（人工评测）','仿真违规分类（人工评测）','问题定位工具issuefinder','高精地图坐标','碰撞责任判定','阶段','无人化程度']
        weekcasedf=pd.read_excel(os.path.join(path,file),usecols=usecolslist)
        hiscaseids = hisdf['编号'].tolist()
        print(len(hiscaseids))
        weekcasedf_pro1 = weekcasedf[~weekcasedf['编号'].isin(hiscaseids)].copy()
        print(weekcasedf_pro1.shape)
        corlist = weekcasedf_pro1['高精地图坐标'].tolist()
        cor=[]
        for var in corlist:
            try:
                if var[-1]!='}':
                    var=var+'}'
                varjson=json.loads(var)
                cor.append([varjson['x'],varjson['y']])
            except:
                cor.append([0,0])
        corindex=['X','Y']
        cordf=pd.DataFrame(cor,columns=corindex)
        # print(weekcasedf_pro1)
        # pdb.set_trace()
        #保存为txt
        # os.system('rm -rf /mnt/d/code/python_pro2/intensitymap/data/issueurl_list.txt')
        weekcasedf_pro1.to_csv('/mnt/d/code/python_pro2/intensitymap/issueurl_list.txt',columns=['问题定位工具issuefinder'],header=False,index=False)
        os.system('python3 /mnt/d/code/python_pro2/intensitymap/issuepro.py')
        issueoutdf = pd.read_csv('/mnt/d/code/python_pro2/intensitymap/out.csv')
        weekcasedf_pro1['indexstr']=range(0,len(weekcasedf_pro1))
        weekcasedf_pro1.set_index(['indexstr'],inplace=True)
        
        finaloutdf=pd.concat([weekcasedf_pro1,issueoutdf,cordf],axis=1)
        finaloutdf.to_excel(os.path.join(path,'out_'+file),index=False)
    
    def recallpro(self,hisdf,weekdf,savepath,weekfile):
        hiscaseids = hisdf['编号'].tolist()
        weekdf_ex = weekdf[~weekdf['编号'].isin(hiscaseids)].copy()
        weekdf_ex.to_csv('/mnt/d/code/python_pro2/intensitymap/issueurl_list.txt',columns=['问题定位工具issuefinder'],header=False,index=False)
        os.system('rm -rf /mnt/d/code/python_pro2/intensitymap/out.csv')
        os.system('python3 /mnt/d/code/python_pro2/intensitymap/issuepro.py')
        issueoutdf = pd.read_csv('/mnt/d/code/python_pro2/intensitymap/out.csv')
        weekdf_ex['indexstr']=range(0,len(weekdf_ex))
        weekdf_ex.set_index(['indexstr'],inplace=True)
        finaloutdf=pd.concat([weekdf_ex,issueoutdf],axis=1)
        arealistex=['WuHanJingKaiQuKaiChengLuWang','YiZhuangDaLuWang']
        caridlistex=['ARCF','WM']
        HW=[]
        carlistraw= finaloutdf['taskid'].tolist()
        for var in carlistraw:
            print(var)
            try:
                if 'ARCF' in var or 'WM' in var: 
                    HW.append('HW5')
                else:
                    HW.append('HW3')
            except:
                pass
        finaloutdf['HW']=HW
        # finaloutdf = finaloutdf[(finaloutdf['area'].isin(arealistex))&(finaloutdf['HW'].isin(['HW5']))]
        finaloutdf = finaloutdf[finaloutdf['area'].isin(arealistex)]
        print(len(finaloutdf))
        finaloutdf.to_excel(savepath+'ex_'+weekfile,index=False)

    def readmrc(self,hispath,hisfile,weekpath,weekfile):
        usecolslist=['icafeurl']
        hiscasedf=pd.read_excel(hispath+hisfile,sheet_name='Sheet1',usecols=usecolslist)
        hiscasedf_risk=pd.read_excel(hispath+hisfile,sheet_name='Sheet2',usecols=usecolslist)
        hislist = hiscasedf['icafeurl'].values.tolist()
        hislist_pro=[]
        for var in hislist:
            if len(str(var))<15:
                hislist_pro.append(str(var))
            else:
                varlist = var.split('/')
                for varstr in varlist:
                    if 'MRDR' in varstr:
                        icafeid=varstr.split('-')[1]
                        hislist_pro.append(str(icafeid))
        hislist_risk = hiscasedf_risk['icafeurl'].values.tolist()
        hislist_risk_pro=[]
        for var in hislist_risk:
            if len(str(var))<15:
                hislist_risk_pro.append(str(var))
            else:
                varlist = var.split('/')
                for varstr in varlist:
                    if 'MRDR' in varstr:
                        icafeid=varstr.split('-')[1]
                        hislist_risk_pro.append(str(icafeid))
        # print(hislist_risk_pro,hislist_pro)
        weekcasedf=pd.read_excel(weekpath+weekfile)
        weekcaselist= weekcasedf.values.tolist()
        columns_name = weekcasedf.columns.tolist()
        weekcaseex=[]
        print(weekcaselist[:5])
        for var in weekcaselist:
            name = var[1]
            namelist=name.split('_')
            mrcname=namelist[1]
            icafeid=var[0].replace('MRDR-','')
            var[1]=mrcname
            var[0]=icafeid
            weekcaseex.append(var)
        weekcasedfpro=pd.DataFrame(weekcaseex,columns=columns_name)
        # exmrc=['车端变更检测停止线变更预警','路口车端变更检测几何变更预警','非路口车端变更检测几何变更预警','路口车端变更检测线型变更预警','非路口车端变更检测线型变更预警']
        exmrc=['车端变更检测停止线变更预警','车端变更检测栅栏变更预警']
        riskexmrc=['非高危路沿变更']
        if 'risk' in weekfile:
            weekcasedfpro = weekcasedfpro[~weekcasedfpro['标题'].isin(riskexmrc)]
        else:
            weekcasedfpro = weekcasedfpro[weekcasedfpro['标题'].isin(exmrc)]
        # print(weekcasedfpro.head())
        return(weekcasedfpro,hislist_pro,hislist_risk_pro)
        # print(hislist)

    def mrc_ex_clear(self,weekcasedf,hisicafeid,weekpath,weekfile,icafefile):
        weekpro1 = weekcasedf[~weekcasedf['icafeID'].isin(hisicafeid)]
        week_ex = weekpro1.sample(n=30)
        icafedf = pd.read_excel(weekpath+icafefile)
        mrdridlist = icafedf['编号'].tolist()
        weekexids = week_ex['icafeID'].tolist()
        weekissue=[]
        print(weekexids)
        for var in weekexids:
            strindex = 'MRDR-'+str(var)
            indexnum = mrdridlist.index(strindex)
            weekissue.append(icafedf['问题定位工具issuefinder'].tolist()[indexnum])
        # print(weekissue)
        # pdb.set_trace()
        week_ex['issueurl'] = weekissue
        week_ex.to_excel(weekpath+'ex_'+weekfile,index=False)
    
    def mrc_ex(self,weekcasedf,hisicafeid,weekpath,weekfile,icafefile):
        weekpro1 = weekcasedf[~weekcasedf['编号'].isin(hisicafeid)]
        week_ex = weekpro1.sample(n=20)
        # issuefinderlist = week_ex['问题定位工具issuefinder'].tolist()
        week_ex.to_csv('/mnt/d/code/python_pro2/intensitymap/issueurl_list.txt',columns=['问题定位工具issuefinder'],header=False,index=False)
        os.system('rm -rf /mnt/d/code/python_pro2/intensitymap/out.csv')
        os.system('python3 /mnt/d/code/python_pro2/intensitymap/issuepro.py')
        issueoutdf = pd.read_csv('/mnt/d/code/python_pro2/intensitymap/out.csv')
        week_ex['indexstr']=range(0,len(week_ex))
        week_ex.set_index(['indexstr'],inplace=True)
        finalweekoutdf=pd.concat([week_ex,issueoutdf],axis=1)

        finalweekoutdf.to_excel(weekpath+'ex_'+weekfile,index=False)
    
    def mrc_risk_ex_clear(self,weekcasedf,hisicafeid_risk,weekpath,weekfile,icafefile):
        # print(hisicafeid_risk)
        # print(weekcasedf['icafeID'].tolist())
        week_ex = weekcasedf[~weekcasedf['icafeID'].isin(hisicafeid_risk)]
        # week_ex = weekpro1.sample(n=30)
        print(week_ex['故障日期'])
        # pdb.set_trace()
        icafedf = pd.read_excel(weekpath+icafefile)
        mrdridlist = icafedf['编号'].tolist()
        weekexids = week_ex['icafeID'].tolist()
        weekissue=[]
        for var in weekexids:
            strindex = 'MRDR-'+str(var)
            try:
                indexnum = mrdridlist.index(strindex)
                weekissue.append(icafedf['问题定位工具issuefinder'].tolist()[indexnum])
            except:
                weekissue.append('nullstr')
                pass
        # print(weekissue)
        # pdb.set_trace()
        week_ex['issueurl'] = weekissue
        week_ex.to_excel(weekpath+'risk_'+weekfile,index=False)

    def mrc_risk_ex(self,weekcasedf,hisicafeid_risk,weekpath,weekfile,icafefile):
        week_ex = weekcasedf[~weekcasedf['编号'].isin(hisicafeid_risk)].copy()
        week_ex.to_csv('/mnt/d/code/python_pro2/intensitymap/issueurl_list.txt',columns=['问题定位工具issuefinder'],header=False,index=False)
        os.system('rm -rf /mnt/d/code/python_pro2/intensitymap/out.csv')
        os.system('python3 /mnt/d/code/python_pro2/intensitymap/issuepro.py')
        issueoutdf = pd.read_csv('/mnt/d/code/python_pro2/intensitymap/out.csv')
        week_ex['indexstr']=range(0,len(week_ex))
        week_ex.set_index(['indexstr'],inplace=True)
        finalweekoutdf=pd.concat([week_ex,issueoutdf],axis=1)
        finalweekoutdf.to_excel(weekpath+'risk_'+weekfile,index=False)

    def icafemrc(self,icafedf,mrcdf,allpath,allfile):
        timerange=10
        icafelist=icafedf[['exactts','carid','编号']].values.tolist()
        mrclist=mrcdf[['故障时间','车辆ID']].values.tolist()
        # print(mrclist)
        icafeout=[]
        # mrcdf.set_index('故障时间',inplace=True)
        out=[]
        for var in icafelist:
            timevar = var[0]
            # datetimevar = timevar.to_pydatetime()
            # print(timevar)
            starttime = timevar-timerange
            endtime = timevar+timerange
            carid = var[1]
            print('编号：%s|时间段：%f---%f|carid:%s' %(var[2],starttime,endtime,carid))
            mrcdf_ex = mrcdf[(mrcdf['车辆ID']==carid)&(mrcdf['timestamp']<=endtime)&(mrcdf['timestamp']>=starttime)]
            mrcexlist = mrcdf_ex.values.tolist()
            if len(mrcexlist) == 0:
                var.append('null')
            else:
                idliststr = '-'.join([str(i) for i in mrcdf_ex['id'].tolist()])
                typelsitstr = '-'.join(mrcdf_ex['问题根因'].tolist())
                var.append(idliststr+', '+typelsitstr)
            out.append(var)
        columnname=['exactts','carid','编号','匹配結果']
        outdf = pd.DataFrame(out,columns=columnname)
        outdf.to_csv(os.path.join(allpath,allfile+'.csv'),encoding='gbk',index=False)

    def case_ht(self,df1,df2):
        idlist = df1['编号'].tolist()
        out=[]
        columns_name = df1.columns.tolist()
        for var in idlist:
            # print(var)
            try:
                df2_ex = df2[df2['case_id']==var]
                # df2_ex2 = df2_ex[df2_ex['report']=TRUE]
                num = df2_ex.shape[0]
                x = df2_ex['x'].unique()[0]
                y = df2_ex['y'].unique()[0]
                contentlist = df2_ex['content'].unique()
                repoortvar = df2_ex['report'].unique()[0]
                # print(repoortvar,type(repoortvar))
                # pdb.set_trace()
                if repoortvar:
                    contentvar='||'.join(contentlist)
                else:
                    contentvar='未报出'
                tempout=[x,y,contentvar]
            except:
                print(var)
                tempout=[0,0,'null']
            out.append(tempout)
        outdf=pd.DataFrame(out,columns=['x','y','content'])

        finalout = pd.concat([df1,outdf],axis=1)  
        # print(finalout)
        # pdb.set_trace()  
        return(finalout)
    
    def zy_num(self,df,outtype):
        keys=outtype.keys()
        out=[]
        for var in keys:
            element = outtype[var]['name']
            typelist = outtype[var]['type']
            dfex = df[(df['intelligence_element_type']==element) & (df['intelligence_change_type'].isin(typelist))]
            tempvar=[var,dfex.shape[0]]
            out.append(tempvar)
        index = ['变更类型','报出次数']
        outdf = pd.DataFrame(out,columns=index)
        return(outdf)

    def infoana(self,df):
        #输出总的报出次数
        outtype = {'车道线几何变更':{'name':'Road','type':['ALTER_DEL','ALTER_GEO_MOD']},'车道新增':{'name':'Road','type':['ALTER_ADD']},'车道删除':{'name':'Road','type':['ALTER_DEL']},'停止线变更':{'name':'StopLine','type':['ALTER_ADD','ALTER_GEO_MOD']},'栅栏变更':{'name':'Fence','type':['ALTER_ADD']},'车道线线型变更':{'name':'Road','type':['ALTER_MOD']},'路沿变更':{'name':'Curb','type':['ALTER_MOD','ALTER_DEL']},'待转区变更':{'name':'LeftTurnRoad','type':['ALTER_DEL']}}
        outnum_all = self.zy_num(df, outtype)
        valid = df[df['idnum'].notnull()]
        novalid = df[~df['idnum'].notnull()]
        print(valid.shape,novalid.shape)
        outnum_valid = self.zy_num(valid, outtype)
        print(outnum_all,outnum_valid)
        out = pd.concat([outnum_all,outnum_valid],axis=1)
        return(out)
    
    def recall_ana(self,df):
        df_in_odd=df[df['是否算法ODD内']=='是']
        carget=df_in_odd[df_in_odd['云代驾是否报出']=='是']
        changetypelist=df['变更类型'].tolist()
        chty_out=[]
        for var in changetypelist:
            print(var)
            varlist=var.split(',')
            chty_out.append(varlist[0])
        df['chty']=chty_out
        chtype=['车道线几何变更','车道新增','车道删除','停止线变更','停止线新增','停止线删除','安全岛&导流区变更','待转区变更','线型变更','栅栏新增','栅栏变更','栅栏删除','路牙变更','行驶方向变更','人行横道变更','限速','禁掉指示牌','减速带','车道类型变更','道路拓扑','路沿变更','道路拓扑-方向']
        out=[['汇总',df.shape[0],df_in_odd.shape[0],carget.shape[0]]]
        for var in chtype:
            print(var)
            varall=df[df['chty']==var]
            varall_inodd=varall[varall['是否算法ODD内']=='是']
            varall_get=varall_inodd[varall_inodd['云代驾是否报出']=='是']
            out.append([var,varall.shape[0],varall_inodd.shape[0],varall_get.shape[0]])
        print(out)
        columnname=['变更类型','总数','ODD内','云代驾报出']
        outdf=pd.DataFrame(out,columns=columnname)
        return(outdf)
    
    def mrcuuid(self,mrcdfex,tdf):
        timedel=5
        out=[]
        print(mrcdfex)
        slist=mrcdfex.values.tolist()
        tdf['intelligence_uuid']=tdf['intelligence_uuid'].astype('str')
        for var in slist:
            timevar=var[0]
            print(type(var[2]))
            # carid=var[2].split('_')[0]
            if type(var[2])==str:
                carid=var[2].split('_')[0]
            else:
                carid='nan000'
            timestampvar=sxg.timetra(timevar,0)  #转换为时间戳
            tariddf=tdf[(tdf['timestamps']<=timestampvar+timedel)&(tdf['timestamps']>=timestampvar-timedel)&(tdf['intelligence_car_id']==carid)]
            if len(tariddf)==0:
                uuidstr='-'
                print('无匹配项')
            else:
                uuidlist=tariddf['intelligence_uuid'].tolist()
                uuidstr='_'.join(uuidlist)
            out.append(uuidstr)
        mrcdfex['uuids']=out
        return(mrcdfex)
    
    def mrcuuid_1(self,mrcdfex,tdf):  #情报id通用匹配
        timedel=5
        out=[]
        print(mrcdfex)
        slist=mrcdfex.values.tolist()
        tdf['intelligence_uuid']=tdf['intelligence_uuid'].astype('str')
        mrcdfex_col=mrcdfex.columns.tolist()
        # print(mrcdfex_col)
        timeindex=mrcdfex_col.index('故障时间')
        carindex=mrcdfex_col.index('车辆ID')
        print(mrcdfex_col)
        for var in slist:
            timevar=var[timeindex]
            # if type(var[])==str:
            #     carid=var[2].split('_')[0]
            # else:
            #     carid='nan000'
            carid=var[carindex]
            timestampvar=sxg.timetra(timevar,0)  #转换为时间戳
            tariddf=tdf[(tdf['timestamps']<=timestampvar+timedel)&(tdf['timestamps']>=timestampvar-timedel)&(tdf['intelligence_car_id']==carid)]
            if len(tariddf)==0:
                uuidstr='-'
                print('无匹配项')
            else:
                uuidlist=tariddf['intelligence_uuid'].tolist()
                uuidstr='_'.join(uuidlist)
            out.append(uuidstr)
        mrcdfex['uuids']=out
        return(mrcdfex)
    
    def n_ana(self,df_mrc,df_n):
        # print(df_mrc.describe(),df_n.describe())
        mrc_col=df_mrc.columns.tolist()
        df_n['开始时间']=df_n['开始时间'].astype('str')
        df_n['结束时间']=df_n['结束时间'].astype('str')
        df_mrc['故障时间']=df_mrc['故障时间'].astype('str')
        df_n['该分钟内该预警的case数']=df_n['该分钟内该预警的case数'].astype('str')
        mrctimelist=df_mrc['故障时间'].tolist()
        mrctimeout=[]
        for var in mrctimelist:
            vartime=sxg.timetra(var,0)
            mrctimeout.append(vartime)
        df_mrc['times']=mrctimeout
        nlist=df_n.values.tolist()
        out=[]
        ncol=df_n.columns.tolist()
        c_n={'定位-车道线变更':['非路口车端高危变更检测几何变更预警','非路口车端高危变更检测属性变更预警','车端变更检测车道拓扑变更预警','车端变更检测新增待转区预警'],'感知-车道行驶方向变更':['车端变更检测车道行驶方向变更预警'],'定位-硬隔离变更':['车端变更检测车道新增预警','车端变更检测安全岛/导流区变更预警','现实变更-路牙碰撞风险'],'感知-停止线或行人横道更改':['车端变更检测停止线变更预警','车端变更检测停止线新增预警','车端高危变更检测停止线变更预警','车端高危变更检测停止线新增预警']}
        for var in nlist:
            # print(var)
            varstr='*'.join(var[0:4])+'*'+str(var[-1])
            ctype=var[ncol.index('预警名称')]
            starttimevar=sxg.timetra(var[ncol.index('开始时间')],0)
            endtimevar=sxg.timetra(var[ncol.index('结束时间')],0)
            mrcex=df_mrc[(df_mrc['times']<=endtimevar)&(df_mrc['times']>=starttimevar)&(df_mrc['问题根因'].isin(c_n[ctype]))]
            outvar=[]
            if len(mrcex)==0:
                out.append(['']*(len(mrc_col)+1)+[varstr])
            else:
                mrcex['tarstr']=[varstr]*len(mrcex)
                mrceslist=mrcex.values.tolist()
                out=out+mrceslist
        print(out[0],len(out[0]))
        outcol=mrc_col+['报出时间戳','云代驾报出']
        print(outcol,len(outcol))
        outdf=pd.DataFrame(out,columns=outcol)
        return(outdf)

    def n_ana_1(self,df_mrc,df_n):
        df_mrc['id']=df_mrc['id'].astype('str')
        dfn_list=df_n.values.tolist()
        n_col=df_n.columns.tolist()
        print(dfn_list[0:10])
        c_n={'感知-车道线变更':['非路口车端高危变更检测几何变更预警','非路口车端高危变更检测属性变更预警','车端变更检测车道拓扑变更预警','车端变更检测新增待转区预警'],'感知-车道行驶方向变更':['车端变更检测车道行驶方向变更预警'],'定位-硬隔离变更':['车端变更检测车道新增预警','车端变更检测安全岛/导流区变更预警','现实变更-路牙碰撞风险'],'感知-停止线或行人横道更改':['车端变更检测停止线变更预警','车端变更检测停止线新增预警','车端高危变更检测停止线变更预警','车端高危变更检测停止线新增预警']}
        deltime=5
        outid=[]
        for var in dfn_list:
            carid = var[n_col.index('车辆ID')]
            ctype = var[n_col.index('预警名称')]
            vartimes = var[n_col.index('out_故障时间')]
            stime=vartimes-deltime
            etime=vartimes+deltime
            exmrc=df_mrc[(df_mrc['out_故障时间']<=etime)&(df_mrc['out_故障时间']>=stime)&(df_mrc['问题根因'].isin(c_n[ctype]))&(df_mrc['车辆ID']==carid)]
            if len(exmrc)==0:
                print('无匹配项')
                outid.append('nan')
            else:
                idlist='*'.join(exmrc['id'].tolist())
                outid.append(idlist)
        df_n['mrcid']=outid
        return(df_n)
    
    def zhy_ana(self,datadf,element,chtype,iso,taskdatadf):
        print(element,chtype,iso)
        isolist=[]
        if len(iso) > 5:
            for i in range(1,50):
                isolist.append(iso+str(i))
            # print(isolist)
            dataex=datadf[(datadf['intelligence_element_type']==element) & (datadf['intelligence_change_type']==chtype)&(datadf['intelligence_iso_version'].isin(isolist))]
            taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element) & (taskdatadf['intelligence_change_type']==chtype)&(taskdatadf['intelligence_iso_version'].isin(isolist))]
        else:
            dataex=datadf[(datadf['intelligence_element_type']==element) & (datadf['intelligence_change_type']==chtype)]
            taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element) & (taskdatadf['intelligence_change_type']==chtype)]
        outdict={}
        outdict['变更类型']=[element+'-'+chtype]
        outdict['情报总数']=[len(dataex)]
        # if element=='RoadTurn':
        #     machine_risk= dataex
        # else:
        #     machine_risk = dataex[dataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE','ROAD_TURN_UPDATE'])]
        machine_risk = dataex[dataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE','ROAD_TURN_UPDATE'])]
        outdict['机审高危情报数']=[len(machine_risk)]
        filter_3 = machine_risk[machine_risk['intelligence_filter'].isin([3])]
        outdict['误报库策略过滤']=[len(filter_3)]
        filter_valid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==1)&(machine_risk['intelligence_valid_state'].isin(['VALID_HDMAP_NOT_UPDATED_YET','VALID_HDMAP_UPDATED_ALREADY']))]
        outdict['误报库有效']=[len(filter_valid)]
        filter_novalid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==1)&(machine_risk['intelligence_valid_state'].isin(['NO_VAILD']))]
        outdict['误报库无效']=[len(filter_novalid)]
        filter_man = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==2)]
        outdict['待人审']=[len(filter_man)]
        man_valid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==2)&(machine_risk['intelligence_valid_state'].isin(['VALID_HDMAP_NOT_UPDATED_YET','VALID_HDMAP_UPDATED_ALREADY']))]
        outdict['人审有效']=[len(man_valid)]
        man_novalid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==2)&(machine_risk['intelligence_valid_state'].isin(['NO_VAILD']))]
        outdict['人审无效']=[len(man_novalid)]
        # if element=='RoadTurn':
        #     taskdf = taskdataex
        # else:
        #     taskdf = taskdataex[taskdataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE'])]
        taskdf = taskdataex[taskdataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE','ROAD_TURN_UPDATE'])]
        outdict['任务数']=[len(taskdf['taskid'].unique())]
        outdf=pd.DataFrame(outdict)
        # outdf['确认情报'] = outdf['误报库有效'] + outdf['误报库无效'] + outdf['人审有效']+ outdf['人审无效']
        # outdf['确认有效情报']= outdf['误报库有效']+outdf['人审有效']
        # outdf['有效率']=outdf['确认有效情报']/outdf['确认情报']
        # outdf['有效率']=outdf['有效率'].apply(lambda x: '{:.2%}'.format(x))
        # outdf['out'] = outdf['有效率'].astype('str')+'('+outdf['确认有效情报'].astype('str')+'/'+outdf['确认情报'].astype('str')+')'
        return(outdf)
    
    def zhy_ana2(self,datadf,element,iso,cartype):
        print(element,iso)
        isolist=[]
        if len(cartype)>2:
            if len(iso) > 5:
                for i in range(1,50):
                    isolist.append(iso+str(i))
                # print(isolist)
                dataex=datadf[(datadf['intelligence_element_type']==element) &(datadf['intelligence_iso_version'].isin(isolist))&(datadf['intelligence_car_id'].str.contains(cartype))]
                # taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element) &(taskdatadf['intelligence_iso_version'].isin(isolist))&(taskdatadf['intelligence_car_id'].str.contains(cartype))]
            else:
                dataex=datadf[(datadf['intelligence_element_type']==element)&(datadf['intelligence_car_id'].str.contains(cartype))]
                # taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element)&(taskdatadf['intelligence_car_id'].str.contains(cartype))]
        else:
            if len(iso) > 5:
                for i in range(1,50):
                    isolist.append(iso+str(i))
                # print(isolist)
                dataex=datadf[(datadf['intelligence_element_type']==element) &(datadf['intelligence_iso_version'].isin(isolist))]
                # taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element) &(taskdatadf['intelligence_iso_version'].isin(isolist))]
            else:
                dataex=datadf[(datadf['intelligence_element_type']==element)]
                # taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element)]
        outdict={}
        outdict['变更类型']=[element]
        outdict['情报总数']=[len(dataex)]
        # if element=='RoadTurn':
        #     machine_risk= dataex
        # else:
        #     machine_risk = dataex[dataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE','ROAD_TURN_UPDATE'])]
        machine_risk = dataex[dataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE','ROAD_TURN_UPDATE'])]
        outdict['机审高危情报数']=[len(machine_risk)]
        filter_3 = machine_risk[machine_risk['intelligence_filter'].isin([3])]
        outdict['误报库策略过滤']=[len(filter_3)]
        filter_valid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==1)&(machine_risk['intelligence_valid_state'].isin(['VALID_HDMAP_NOT_UPDATED_YET','VALID_HDMAP_UPDATED_ALREADY']))]
        outdict['误报库有效']=[len(filter_valid)]
        filter_novalid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==1)&(machine_risk['intelligence_valid_state'].isin(['NO_VAILD']))]
        outdict['误报库无效']=[len(filter_novalid)]
        filter_man = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==2)]
        outdict['待人审']=[len(filter_man)]
        man_valid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==2)&(machine_risk['intelligence_valid_state'].isin(['VALID_HDMAP_NOT_UPDATED_YET','VALID_HDMAP_UPDATED_ALREADY']))]
        outdict['人审有效']=[len(man_valid)]
        man_novalid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==2)&(machine_risk['intelligence_valid_state'].isin(['NO_VAILD']))]
        outdict['人审无效']=[len(man_novalid)]
        # if element=='RoadTurn':
        #     taskdf = taskdataex
        # else:
        #     taskdf = taskdataex[taskdataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE'])]
        # taskdf = taskdataex[taskdataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE','ROAD_TURN_UPDATE'])]
        # outdict['任务数']=[len(taskdf['taskid'].unique())]
        outdf=pd.DataFrame(outdict)
        # outdf['确认情报'] = outdf['误报库有效'] + outdf['误报库无效'] + outdf['人审有效']+ outdf['人审无效']
        # outdf['确认有效情报']= outdf['误报库有效']+outdf['人审有效']
        # outdf['有效率']=outdf['确认有效情报']/outdf['确认情报']
        # outdf['有效率']=outdf['有效率'].apply(lambda x: '{:.2%}'.format(x))
        # outdf['out'] = outdf['有效率'].astype('str')+'('+outdf['确认有效情报'].astype('str')+'/'+outdf['确认情报'].astype('str')+')'
        return(outdf)
    
    def zhy_ana3(self,datadf,element,iso,taskdatadf,cartype):  #临时分析用
        # print(element,iso)
        isolist=[]
        if len(cartype)>2:
            if len(iso) > 5:
                for i in range(1,50):
                    isolist.append(iso+str(i))
                # print(isolist)
                dataex=datadf[(datadf['intelligence_element_type']==element) &(datadf['intelligence_iso_version'].isin(isolist))&(datadf['intelligence_car_id'].str.contains(cartype))]
                taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element) &(taskdatadf['intelligence_iso_version'].isin(isolist))&(taskdatadf['intelligence_car_id'].str.contains(cartype))]
            else:
                dataex=datadf[(datadf['intelligence_element_type']==element)&(datadf['intelligence_car_id'].str.contains(cartype))]
                taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element)&(taskdatadf['intelligence_car_id'].str.contains(cartype))]
        else:
            if len(iso) > 5:
                for i in range(1,50):
                    isolist.append(iso+str(i))
                # print(isolist)
                dataex=datadf[(datadf['intelligence_element_type']==element) &(datadf['intelligence_iso_version'].isin(isolist))]
                taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element) &(taskdatadf['intelligence_iso_version'].isin(isolist))]
            else:
                dataex=datadf[(datadf['intelligence_element_type']==element)]
                taskdataex=taskdatadf[(taskdatadf['intelligence_element_type']==element)]
        outdict={}
        outdict['变更类型']=[element]
        outdict['情报总数']=[len(dataex)]
        # if element=='RoadTurn':
        #     machine_risk= dataex
        # else:
        #     machine_risk = dataex[dataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE','ROAD_TURN_UPDATE'])]
        # machine_risk = dataex[dataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE','ROAD_TURN_UPDATE'])]
        machine_risk = dataex[dataex['intelligence_high_risk_type'].isin(['TOPOLOGY_UPDATE'])]
        outdict['机审高危情报数']=[len(machine_risk)]
        filter_3 = machine_risk[machine_risk['intelligence_filter'].isin([3])]
        outdict['误报库策略过滤']=[len(filter_3)]
        filter_valid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==1)&(machine_risk['intelligence_valid_state'].isin(['VALID_HDMAP_NOT_UPDATED_YET','VALID_HDMAP_UPDATED_ALREADY']))]
        outdict['误报库有效']=[len(filter_valid)]
        filter_novalid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==1)&(machine_risk['intelligence_valid_state'].isin(['NO_VAILD']))]
        outdict['误报库无效']=[len(filter_novalid)]
        filter_man = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==2)]
        outdict['待人审']=[len(filter_man)]
        man_valid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==2)&(machine_risk['intelligence_valid_state'].isin(['VALID_HDMAP_NOT_UPDATED_YET','VALID_HDMAP_UPDATED_ALREADY']))]
        outdict['人审有效']=[len(man_valid)]
        man_novalid = machine_risk[(~machine_risk['intelligence_filter'].isin([3]))&(machine_risk['intelligence_free_inspection']==2)&(machine_risk['intelligence_valid_state'].isin(['NO_VAILD']))]
        outdict['人审无效']=[len(man_novalid)]
        taskdf = taskdataex[taskdataex['intelligence_high_risk_type'].isin(['DRIVE_REGION_EXPAND','TOPOLOGY_UPDATE','ROAD_TURN_UPDATE'])]
        outdict['任务数']=[len(taskdf['taskid'].unique())]
        outdf=pd.DataFrame(outdict)
        return(outdf)
    


    def novalid_ana(self,df1,df_car):
        print(df1.head())
        df1['intelligence_source_time']=df1['intelligence_source_time'].astype('str')
        dfexlist=df1[['intelligence_car_id','intelligence_source_time']].values.tolist()
        df1_idlist=[]
        for var in dfexlist:
            print(var)
            time_st=sxg.timetra(var[1],0)
            varid=var[0]+'_'+str(int(time_st-20))+'_'+str(int(time_st+9))
            # print(varid)
            df1_idlist.append(varid)
        print(df1_idlist)
        df_caridlist=df_car['场景名称'].tolist()
        car_idlist=[]
        for var in df_caridlist:
            varlist=var.split('_')
            tempvar=[varlist[1],varlist[3],varlist[4]]
            varid='_'.join(tempvar)
            car_idlist.append(varid)
        print(car_idlist)
        df1['id']=df1_idlist
        df_car['id0']=car_idlist
        dfout=sxg.excelmatch(df_car,df1)
        return(dfout)

    def stopline_gtc(self,df_icafe,df_tc):
        #处理高提纯结果，获取唯一id
        df_tc['开始时间']=df_tc['开始时间'].astype('int64',errors='ignore')
        df_tc['结束时间']=df_tc['开始时间'].astype('int64',errors='ignore')
        df_icafe['问题时间点']=df_icafe['问题时间点'].astype('str',errors='ignore')
        icafelist=df_icafe.values.tolist()
        icafecolumns=df_icafe.columns.tolist()
        # print(icafecolumns)
        outlist=[]
        for var in icafelist:
            carid=var[icafecolumns.index('车辆ID')]
            timeraw=var[icafecolumns.index('问题时间点')]
            corraw=var[icafecolumns.index('高精地图坐标')]
            taskid=var[icafecolumns.index('高精地图坐标')]
            extimeint=sxg.timetra(timeraw,0)
            X=float(corraw.split(',')[0])
            Y=float(corraw.split(',')[1])
            reid=carid+'_'+str(int(extimeint))
            tcsearch = df_tc[(df_tc['车辆ID']==carid)&(df_tc['开始时间']<(extimeint-12))&(df_tc['开始时间']>(extimeint-18))]
            out = tcsearch.shape[0]
            if out==1:
                videostr=tcsearch['TASKID'].values.tolist()[0]+'_'+str(tcsearch['开始时间'].values.tolist()[0])+'_'+str(tcsearch['结束时间'].values.tolist()[0])
            else:
                videostr='暂未回传'
            outlist.append([extimeint,X,Y,out,videostr,reid])
        outdf=pd.DataFrame(outlist,columns=['extime','X','Y','匹配个数','高提纯','id'])
        finaldf=pd.concat([df_icafe,outdf],axis=1)
        return(finaldf)        

    def pro(self):
        path='/mnt/d/现实变更/数据集/主线case/主线case导出'
        print(path)
        hiscase = self.readcase(path)
        # hiscase.to_csv(os.path.join(path,'out_merge_0729.csv'),index=False)
        # pdb.set_trace()
        # path,file = sxg.openfile()
        # print(hiscase)
        path_week='/mnt/d/现实变更/数据集/主线case'
        file = sys.argv[1]
        print('本周文件：',file)
        weekcases = self.weekcase(path_week,file,hiscase)
        os.system('cp ' + os.path.join(path_week,'out_'+file) + ' '+path)
    
    # 主线case筛选
    def pro2(self):
        (hispath,hisfile) = ('/mnt/d/现实变更/数据集/主线case/主线case召回统计/','all-20230807.xlsx')
        (weekpath,weekfile) = ('/mnt/d/现实变更/数据集/主线case/主线case召回统计/','all-20230814.xlsx')
        usecolslist=['编号','标题','问题时间点','问题定位工具issuefinder','是否重复反馈','地图-导致问题模块及要素类别']
        hisdf = pd.read_excel(hispath+hisfile,usecols=usecolslist)
        weekdf = pd.read_excel(weekpath+weekfile,usecols=usecolslist)
        self.recallpro(hisdf,weekdf,weekpath,weekfile)
    
    def pro3(self):
        (hispath,hisfile) = ('/mnt/d/现实变更/数据集/现实变更MRC/报出待分析/','车端报出待分析-all.xlsx')
        (weekpath,weekfile) = ('/mnt/d/现实变更/数据集/现实变更MRC/报出待分析/','常规变更检测报出-icafe-0814.xlsx')
        icafefile = 'excel-export_icafe_0807.xlsx'
        (weekcasedf,hisicafeid,hisicafeid_risk)=self.readmrc(hispath,hisfile,weekpath,weekfile)
        mrccase = self.mrc_ex(weekcasedf,hisicafeid,weekpath,weekfile,icafefile)   #分两步执行，第一步提取非高危报警
        # mrccase_rist = self.mrc_risk_ex(weekcasedf,hisicafeid_risk,weekpath,weekfile,icafefile)   #分两步执行，第二步提取高危报警，weekfile要修改
    
    def pro4(self):
        path = '/mnt/d/现实变更/数据集/主线case/主线case导出'
        out = []
        for var in os.listdir(path):
            if 'out' in var:
                print(var)
                vardata = pd.read_excel(path+'/'+var)
                out.append(vardata)
        outdf = pd.concat(out,axis=0)
        outdf.to_excel(path+'-merge.xlsx',index=False)
    
    def pro5(self):
        allpath,allfile = ('/mnt/d/现实变更/数据集/主线case','out_主线case0918-0924.xlsx')
        icafedf = pd.read_excel(os.path.join(allpath,allfile),sheet_name='Sheet1')
        mrcdf = pd.read_excel(os.path.join(allpath,allfile),sheet_name='Sheet2')
        timelist=icafedf['问题时间点'].tolist()
        print('时间周期：%s--->%s' %(min(timelist),max(timelist)))
        taskidlist = icafedf['taskid']
        isolist = icafedf['isoversition'].unique()
        caridlist=[]
        arcfidlist=[]
        for var in taskidlist:
            varid = var.split('_')[0]
            print(var)
            caridlist.append(varid)
            if 'ARCF' in varid:
                arcfidlist.append(varid)
        cariduni=list(set(arcfidlist))
        icafedf['carid']=caridlist
        print('arcfidlist:',cariduni)
        mrctime = mrcdf['故障时间'].values.tolist()
        mrctimestamp=[]
        for var in mrctime:
            timearr=time.strptime(var,'%Y-%m-%d %H:%M:%S')
            timestamp = time.mktime(timearr)
            mrctimestamp.append(timestamp)
        mrcdf['timestamp']=mrctimestamp
        mrcdf['id']=range(len(mrcdf))
        self.icafemrc(icafedf,mrcdf,allpath,allfile)

    def pro6(self):
        # path,file = sxg.openfile()
        path,file = os.path.split(sys.argv[1])
        # path = '/mnt/d/现实变更/数据集/主线case'
        # file = 'out_主线case1009-1015.xlsx'
        df1 = pd.read_excel(os.path.join(path,file),sheet_name='Sheet1')
        df2 = pd.read_excel(os.path.join(path,file),sheet_name='Sheet2')
        print(df1.head(),df2.head())
        out = self.case_ht(df1,df2)
        out.to_csv(os.path.join(path,file+'.csv'),index=False,encoding='gbk')

    def pro7(self):
        # path,file = sxg.openfile()
        # path = '/mnt/d/现实变更/众源情报'
        # file = '202310251508-all.csv'
        path,file = os.path.split(sys.argv[1])
        print(os.path.join(path,file))
        if os.path.splitext(file)[-1]=='.csv':
            datadf = pd.read_csv(os.path.join(path,file),encoding='GBK')
        else:
            datadf = pd.read_excel(os.path.join(path,file))
        #提取坐标
        corstr = datadf['intelligence_change_location'].tolist()
        outcor=[]
        for var in corstr:
            vard = json.loads(var)
            outcor.append([vard['x'],vard['y']])
        corstr=['x','y']
        cordf = pd.DataFrame(outcor,columns=corstr)
        datadf1 = pd.concat([datadf,cordf],axis=1)
        # datadf1.to_csv(os.path.join(path,'cor_'+file),index=False,encoding='gbk')
        # out = self.infoana(datadf1)
        if os.path.splitext(file)[-1]=='.csv':
            datadf1.to_csv(os.path.join(path,'cor_'+file),index=False,encoding='gbk')
        else:
            datadf1.to_excel(os.path.join(path,'cor_'+file),index=False)
    
    def pro7_1(self):
        path,file = os.path.split(sys.argv[1])
        print(os.path.join(path,file))
        if os.path.splitext(file)[-1]=='.csv':
            datadf = pd.read_csv(os.path.join(path,file))
        else:
            datadf = pd.read_excel(os.path.join(path,file))
        #提取坐标
        corstr = datadf['intelligence_change_location'].tolist()
        outcor=[]
        for var in corstr:
            vard = json.loads(var)
            outcor.append([vard['x'],vard['y']])
        corstr=['x','y']
        cordf = pd.DataFrame(outcor,columns=corstr)
        datadf1 = pd.concat([datadf,cordf],axis=1)
        # datadf1.to_csv(os.path.join(path,'cor_'+file),index=False,encoding='gbk')
        out = self.infoana(datadf1)
        if os.path.splitext(file)[-1]=='.csv':
            out.to_csv(os.path.join(path,'out_'+file),index=False,encoding='gbk')
        else:
            out.to_excel(os.path.join(path,'out_'+file),index=False)
   
    def pro8(self):
        path,file = sxg.openfile()
        # path = '/mnt/d/现实变更/众源情报'
        # file = 'cor.csv'
        datadf = pd.read_excel(os.path.join(path,file))
        corlist = datadf['高精地图坐标']
        corout=[]
        print(corlist)
        for var in corlist:
            var0=var.replace('"x":','').replace('"y":','').replace('"z":','').replace('"latitude":','').replace('"longitude":','').replace('{','').replace('}','')
            print(var0)
            vars=var0.split(',')
            x=0
            y=0
            for corvar in vars:
                if float(corvar)>3000000:
                    y=float(corvar)
                elif float(corvar)<3000000 and float(corvar)>200000:
                    x=float(corvar)
            corout.append([x,y])
        cordf=pd.DataFrame(corout,columns=['X','Y'])
        # outdf=pd.concat([datadf,cordf],axis=1)
        cordf.to_csv(os.path.join(path,'out_'+file+'.csv'),encoding='gbk',index=False)

    def pro9(self):
        path,file = os.path.split(sys.argv[1])
        # path = '/mnt/d/现实变更/众源情报/有效变更'
        # file = 'excel-export-icafe-1108.xlsx'
        df1 = pd.read_excel(os.path.join(path,file),sheet_name='Sheet1',usecols=['编号','问题时间点','地图-导致问题模块及要素类别','高精地图坐标','问题定位工具issuefinder'],engine='openpyxl')
        df2 = pd.read_excel(os.path.join(path,file),sheet_name='Sheet2',usecols=['intelligence_uuid','intelligence_source_time','intelligence_element_type','intelligence_change_location','intelligence_iso_version','intelligence_change_type'],engine='openpyxl')
        cor1=df1['高精地图坐标'].tolist()
        cor2=df2['intelligence_change_location'].tolist()
        cor1_out=[]
        cor2_out=[]
        for var in cor1:
            try:
                vard = json.loads(var)
                cor1_out.append([vard['x'],vard['y']])
            except:
                cor1_out.append([0,0])
        for var in cor2:
            try:
                vard = json.loads(var)
                cor2_out.append([vard['x'],vard['y']])
            except:
                cor1_out.append([0,0])

        corstr=['x','y']
        cor1df = pd.DataFrame(cor1_out,columns=corstr)
        cor2df = pd.DataFrame(cor2_out,columns=corstr)
        df1out=pd.concat([df1,cor1df],axis=1)
        df2out=pd.concat([df2,cor2df],axis=1)
        df2out.rename(columns={'intelligence_uuid': '编号','intelligence_source_time':'问题时间点','intelligence_element_type':'地图-导致问题模块及要素类别','intelligence_change_location':'高精地图坐标'}, inplace=True)

        dfout=pd.concat([df1out,df2out],ignore_index = True)
        dfout.to_csv(os.path.join(path,'out_'+file+'.csv'),index=False,encoding='gbk')
    
    def pro10(self):
        path,file = os.path.split(sys.argv[1])
        # path = '/mnt/d/现实变更/数据集/主线case'
        # file = '停止线变更.xlsx'
        df = pd.read_excel(os.path.join(path,file))
        corlist = df['高精地图坐标'].tolist()
        cor=[]
        for var in corlist:
            if type(var) is str:
                if var[-1]!='}':
                    var=var+'}'
                try:
                    varjson=json.loads(var)
                    cor.append([varjson['x'],varjson['y']])
                except:
                    cor.append([0,0])
            else:
                cor.append([0,0])
            # pdb.set_trace()
        corindex=['X','Y']
        cordf=pd.DataFrame(cor,columns=corindex)
        finaloutdf=pd.concat([df,cordf],axis=1)
        print(finaloutdf.head())
        finaloutdf.to_excel(os.path.join(path,'cor_'+file),index=False)
    
    def pro11(self):
        path,file = os.path.split(sys.argv[1])
        # path = '/mnt/d/现实变更/数据集/主线case/召回统计'
        # file = 'excel-export-1129.xlsx'
        df = pd.read_excel(os.path.join(path,file))
        out=self.recall_ana(df)
        out.to_excel(os.path.join(path,'out_'+file),index=False)
        
    def pro12(self):
        path,file = os.path.split(sys.argv[1])
        df = pd.read_csv(os.path.join(path,file))
        allnum=df['id'].value_counts()
        alldf=allnum.to_frame()
        print(alldf)
        alldf.to_csv(os.path.join(path,'out_'+file+'.csv'))

    def pro13(self):
        path,file = os.path.split(sys.argv[1])
        df = pd.read_excel(os.path.join(path,file))
        icafeids=df['编号'].tolist()
        out=[]
        for var in icafeids:
            idnum=var.replace('MRDR-','')
            outtemp=sxg.readroad(idnum)
            # print(type(outtemp),outtemp)
            outtempdf = pd.DataFrame(outtemp,index=[0])
            out.append(outtempdf)
        # indexname=['id','地图-导致问题模块及要素类别','报出内容','变更类型','是否算法ODD内','云代驾是否报出','iso']
        # outdf=pd.DataFrame(out,columns=indexname)
        outdf=pd.concat(out,axis=0)
        outdf.to_excel(os.path.join(path,'out_'+file))
 
    def pro14(self):
        path,file = os.path.split(sys.argv[1])
        # mrcdatadf=pd.read_excel(os.path.join(path,file),usecols=readcols,names=outname)
        mrcdatadf_raw=pd.read_excel(os.path.join(path,file))
        mrcdatadf_raw=mrcdatadf_raw[mrcdatadf_raw['icafeID']!='-']
        mrcdatadf_raw['icafeID']=mrcdatadf_raw['icafeID'].astype('int64',errors='ignore')
        hisdf=self.readcase2(path+'/已分析')
        hiscaseids = hisdf['icafeid'].tolist()
        print('hiscaseids:',len(hiscaseids),'mrcdatadf_raw:',len(mrcdatadf_raw))
        mrcdatadf = mrcdatadf_raw[~mrcdatadf_raw['icafeID'].isin(hiscaseids)].copy()
        mrcdatadf.index=list(range(len(mrcdatadf)))
        icafeids=mrcdatadf['icafeID'].tolist()
        print('mrcdatadf:',len(mrcdatadf))

        out=[]
        for var in icafeids:
            outtemp=sxg.readmrdr2(var)
            outtempdf = pd.DataFrame(outtemp,index=[0])
            out.append(outtempdf)
        outdf=pd.concat(out,axis=0)
        outdf.index=list(range(len(outdf)))
        print(outdf,len(outdf))

        readcols=['故障时间','icafeID','taskID','问题根因','x','y']
        outname=['报出时间点','icafeid','taskid','问题根因','X','Y']
        mrcdatadfex=mrcdatadf[readcols]
        #调用issuepro.py
        outdf.to_csv('/mnt/d/code/python_pro2/intensitymap/issueurl_list.txt',columns=['问题定位工具issuefinder'],header=False,index=False)
        os.system('python3 /mnt/d/code/python_pro2/intensitymap/issuepro.py')
        issueoutdf = pd.read_csv('/mnt/d/code/python_pro2/intensitymap/out.csv')
        # print(len(outdf),)
        issueoutdf.index=list(range(len(outdf)))
        # pdb.set_trace()

        #df合并
        outdf_final=pd.concat([mrcdatadfex,outdf,issueoutdf],axis=1)
        print(issueoutdf.columns)
        outdf_final.columns=outname+['问题定位工具issuefinder']+issueoutdf.columns.tolist()
        outdf_final.to_excel(os.path.join(path,'out_'+file),index=False)
        os.system('cp ' + os.path.join(path,'out_'+file) + ' '+path+'/已分析')
    
    def pro15(self):
        path,file = os.path.split(sys.argv[1])
        mrcdatadf=pd.read_excel(os.path.join(path,file),sheet_name='Sheet1')  #mrc导出数据
        zydatadf=pd.read_excel(os.path.join(path,file),sheet_name='Sheet2')   #众源数据库导出数据
        mrcdatadf['报出时间点'] = mrcdatadf['报出时间点'].astype('str')
        mrcdfex=mrcdatadf[['报出时间点','icafeid','taskid','exactts']]
        zydatadf['intelligence_source_time']= zydatadf['intelligence_source_time'].astype('str')
        zytimelist=zydatadf['intelligence_source_time'].tolist()
        zytimestr=[]
        for var in zytimelist:
            vartime = sxg.timetra(var,0)
            zytimestr.append(vartime)
        zydatadf['timestamps']=zytimestr
        # print(zydatadf['timestamps'])
        mrcoutdf=self.mrcuuid(mrcdfex,zydatadf)
        mrcoutdf.to_excel(os.path.join(path,'out_'+file))
   
    def pro16(self):  #carid,报出时间点，匹配情报id
        path,file = os.path.split(sys.argv[1])
        mrcdatadf=pd.read_excel(os.path.join(path,file),sheet_name='Sheet1')  #mrc导出数据,或云代驾导出数据
        zydatadf=pd.read_excel(os.path.join(path,file),sheet_name='Sheet2')   #众源数据库导出数据
        mrcdatadf['故障时间'] = mrcdatadf['故障时间'].astype('str')
        # mrcdfex=mrcdatadf[['报出时间点','icafeid','taskid','exactts']]
        zydatadf['intelligence_source_time']= zydatadf['intelligence_source_time'].astype('str')
        zytimelist=zydatadf['intelligence_source_time'].tolist()
        zytimestr=[]
        for var in zytimelist:
            vartime = sxg.timetra(var,0)
            zytimestr.append(vartime)
        zydatadf['timestamps']=zytimestr
        # print(zydatadf['timestamps'])
        mrcoutdf=self.mrcuuid_1(mrcdatadf,zydatadf)
        mrcoutdf.to_excel(os.path.join(path,'out_'+file))

    def pro17(self): #根据icafeid获取
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        if os.path.splitext(file)[-1]=='.csv':
            df = pd.read_csv(os.path.join(path,file))
        else:
            df = pd.read_excel(os.path.join(path,file))
        # df = pd.read_excel(os.path.join(path,file))
        mrdrlist=df['icafeid'].tolist()
        out=[]
        for var in mrdrlist:
            print(var)
            outtemp=sxg.readmrdr2(var)
            outtempdf = pd.DataFrame(outtemp,index=[0])
            out.append(outtempdf)
        outdf=pd.concat(out,axis=0)
        outdf.index=list(range(len(outdf)))
        # outdf.to_csv('/mnt/d/code/python_pro2/intensitymap/issueurl_list.txt',columns=['问题定位工具issuefinder'],header=False,index=False)
        # os.system('python3 /mnt/d/code/python_pro2/intensitymap/issuepro.py')
        # issueoutdf = pd.read_csv('/mnt/d/code/python_pro2/intensitymap/out.csv')
        # outdf_final=pd.concat([df,outdf,issueoutdf],axis=1)
        outdf_final=pd.concat([df,outdf],axis=1)
        outdf_final.to_excel(os.path.join(path,'out_'+file),index=False)

    def pro18(self):  #云代驾预警分析
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        df_mrc = pd.read_excel(os.path.join(path,file),sheet_name='Sheet1')
        df_n = pd.read_excel(os.path.join(path,file),sheet_name='Sheet2')
        outdf=self.n_ana(df_mrc,df_n)
        outdf.to_excel(os.path.join(path,'out_'+file),index=False)

    def pro18_1(self):  #云代驾预警分析
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        df_mrc = pd.read_excel(os.path.join(path,file),sheet_name='Sheet1')
        df_n = pd.read_excel(os.path.join(path,file),sheet_name='Sheet2')
        outdf=self.n_ana_1(df_mrc,df_n)
        outdf.to_excel(os.path.join(path,'out_'+file),index=False)

    def pro19(self):
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        if os.path.splitext(file)[-1]=='.csv':
            datadf = pd.read_csv(os.path.join(path,file))
        else:
            datadf = pd.read_excel(os.path.join(path,file))
        datadf['故障时间']=datadf['故障时间'].astype('str')
        timelist=datadf['故障时间'].tolist()
        outtime=[]
        for var in timelist:
            # print(var,type(var))
            if ':' in var:
                varout=sxg.timetra(var,0)
            else:
                varout=sxg.timetra(int(float(var)),1)
            outtime.append(varout)
        datadf['out_故障时间']=outtime
        datadf.to_excel(os.path.join(path,'out_'+file),index=False)
    
    def pro20(self):
        path=sys.argv[1]
        outdflist=[]
        for var in os.listdir(path):
            print(path+'/'+var)
            if os.path.splitext(path+'/'+var)[-1]=='.csv':
                datadf = pd.read_csv(os.path.join(path,var))
            else:
                datadf = pd.read_excel(os.path.join(path,var))
            outdflist.append(datadf)
        finaldf=pd.concat(outdflist)
        finaldf.to_excel(os.path.join(path+'.xlsx'),index=False)
    
    def pro21(self):  #MRC问题根因分布统计
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        if os.path.splitext(file)[-1]=='.csv':
            datadf = pd.read_csv(os.path.join(path,file))
        else:
            datadf = pd.read_excel(os.path.join(path,file))
        typelist=datadf['问题根因'].tolist()
        print(typelist)
        tarindex=['车端变更检测几何变更预警','车端变更检测车道新增预警','车端变更检测车道删除预警','车端变更检测停止线变更预警','车端变更检测栅栏变更预警','车端变更检测车道行驶方向变更预警','车端变更检测线型变更预警','车端变更检测新增待转区预警','车端变更检测安全岛/导流区变更预警','车端变更检测人行横道变更预警','非高危路沿变更','车端变更检测车道拓扑变更预警','车端变更检测ROI变更预警','车端变更检测车道拓扑方向变更预警','车端高危变更检测停止线变更预警','车端高危变更检测停止线新增预警','非路口车端高危变更检测几何变更预警']  
        tarindexdf=pd.DataFrame(tarindex,index=list(range(len(tarindex))))
        print(tarindexdf)
        outindex=[]
        for var in typelist:
            tarvarindex=[]
            for tarvar in tarindex:
                if tarvar in var:
                    outindex.append(tarindex.index(tarvar))
                    tarvarindex.append(tarvar)
            if len(tarvarindex)==0:
                outindex.append(999)
        datadf['indexnum']=outindex
        datadf['indexcol']=outindex
        datadf.set_index('indexcol',inplace=True)
        print(datadf)
        out=datadf.groupby(['indexnum'],sort=True)['订单内根因数'].sum()
        outdf=out.to_frame()
        outmerge=pd.concat([tarindexdf,outdf],axis=1)
        print(outmerge)
        if os.path.splitext(file)[-1]=='.csv':
            outmerge.to_csv(os.path.join(path,'out_'+file))
        else:
            outmerge.to_excel(os.path.join(path,'out_'+file))
    
    def pro22(self):  #MRC问题根因分布统计
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        taskfile=sys.argv[2]
        if os.path.splitext(file)[-1]=='.csv':
            datadf = pd.read_csv(os.path.join(path,file))
            taskdatadf = pd.read_csv(os.path.join(path,taskfile))
        else:
            datadf = pd.read_excel(os.path.join(path,file))
            taskdatadf = pd.read_excel(os.path.join(path,taskfile))
        #获取版本信息
        isoverlist=datadf['intelligence_iso_version'].tolist()
        iso_num=[]
        for var in isoverlist:
            try:
                tempvar=var.split('.')
                if tempvar[0]=='6' and tempvar[1]=='2':
                    iso_num.append(int(tempvar[2]))
            except:
                pass
        isodict=dict(Counter(iso_num))
        print(isodict)
        isodf=pd.DataFrame([isodict])
        isodf2=pd.DataFrame(isodf.values.T,index=isodf.columns,columns=['num'])
        isodf2_sort=isodf2.sort_values(by='num',ascending=False)
        isolist_sort=list(isodf2_sort.index)
        isolist_sort.insert(0,'')
        for i in range(0,4):
            iso='6.2.'+str(isolist_sort[i])+'.'
            tarstr=['Fence-ALTER_DEL','Fence-ALTER_ADD','Road-ALTER_ADD','Road-ALTER_GEO_MOD','RoadTurn-ALTER_MOD']
            dflist=[]
            for var in tarstr:
                element=var.split('-')[0]
                chtype=var.split('-')[1]
                vardf = self.zhy_ana(datadf,element,chtype,iso,taskdatadf)
                dflist.append(vardf)
            finalout=pd.concat(dflist)

            finalout['确认情报'] = finalout['误报库有效'] + finalout['误报库无效'] + finalout['人审有效']+ finalout['人审无效']
            finalout['确认有效情报']= finalout['误报库有效']+finalout['人审有效']
            finalout.loc['Row_sum'] = finalout.apply(lambda x: x.sum())

            finalout['有效率']=finalout['确认有效情报']/finalout['确认情报']
            finalout['有效率']=finalout['有效率'].apply(lambda x: '{:.2%}'.format(x))
            finalout['out'] = finalout['有效率'].astype('str')+'('+finalout['确认有效情报'].astype('str')+'/'+finalout['确认情报'].astype('str')+')'
            print(finalout)

            if os.path.splitext(file)[-1]=='.csv':
                finalout.to_csv(os.path.join(path,iso+'_'+file))
            else:
                finalout.to_excel(os.path.join(path,iso+'_'+file))
    
    def pro23(self):
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        datadf_1 = pd.read_excel(os.path.join(path,file),sheet_name="Sheet1")
        datadf_car = pd.read_excel(os.path.join(path,file),sheet_name="Sheet2")
        outdf=self.novalid_ana(datadf_1,datadf_car)
        outdf.to_excel(os.path.join(path,'out_'+file),index=False)
    
    def pro24(self): #根据icafeid获取
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        if os.path.splitext(file)[-1]=='.csv':
            df = pd.read_csv(os.path.join(path,file))
        else:
            df = pd.read_excel(os.path.join(path,file))
        # df = pd.read_excel(os.path.join(path,file))
        mrdrlist=df['icafeid'].tolist()
        out=[]
        for var in mrdrlist:
            print(var)
            outtemp=sxg.readroad(var)
            outtempdf = pd.DataFrame(outtemp,index=[0])
            out.append(outtempdf)
        outdf=pd.concat(out,axis=0)
        outdf.index=list(range(len(outdf)))
        # outdf.to_csv('/mnt/d/code/python_pro2/intensitymap/issueurl_list.txt',columns=['问题定位工具issuefinder'],header=False,index=False)
        # os.system('python3 /mnt/d/code/python_pro2/intensitymap/issuepro.py')
        # issueoutdf = pd.read_csv('/mnt/d/code/python_pro2/intensitymap/out.csv')
        # outdf_final=pd.concat([df,outdf,issueoutdf],axis=1)
        outdf_final=pd.concat([df,outdf],axis=1)
        outdf_final.to_excel(os.path.join(path,'out_'+file),index=False)
    
    def pro25(self):  #准确率统计——新版
        if sys.argv[3]=='JME':
            cartype='RT6'
        elif sys.argv[3]=='ARCF':
            cartype='RT5'
        else:
            cartype='.'
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        taskfile=sys.argv[2]
        if os.path.splitext(file)[-1]=='.csv':
            datadf = pd.read_csv(os.path.join(path,file))
            taskdatadf = pd.read_csv(os.path.join(path,taskfile))
        else:
            datadf = pd.read_excel(os.path.join(path,file))
            taskdatadf = pd.read_excel(os.path.join(path,taskfile))
        #获取版本信息
        isoverlist=datadf['intelligence_iso_version'].tolist()
        iso_num=[]
        for var in isoverlist:
            try:
                tempvar=var.split('.')
                if tempvar[0]=='6' and tempvar[1]=='2':
                    iso_num.append(int(tempvar[2]))
            except:
                pass
        isodict=dict(Counter(iso_num))
        print(isodict)
        isodf=pd.DataFrame([isodict])
        isodf2=pd.DataFrame(isodf.values.T,index=isodf.columns,columns=['num'])
        isodf2_sort=isodf2.sort_values(by='num',ascending=False)
        isolist_sort=list(isodf2_sort.index)
        isolist_sort.insert(0,'')
        for i in range(len(isolist_sort)):
            iso='6.2.'+str(isolist_sort[i])+'.'
            tarstr=['Fence','Road','RoadTurn']
            dflist=[]
            for var in tarstr:
                vardf = self.zhy_ana2(datadf,var,iso,taskdatadf,sys.argv[3])
                dflist.append(vardf)
            finalout=pd.concat(dflist)

            finalout['确认情报'] = finalout['误报库有效'] + finalout['误报库无效'] + finalout['人审有效']+ finalout['人审无效']
            finalout['确认有效情报']= finalout['误报库有效']+finalout['人审有效']
            finalout.loc['Row_sum'] = finalout.apply(lambda x: x.sum())

            finalout['有效率']=finalout['确认有效情报']/finalout['确认情报']
            finalout['有效率']=finalout['有效率'].apply(lambda x: '{:.2%}'.format(x))
            finalout['out'] = finalout['有效率'].astype('str')+'('+finalout['确认有效情报'].astype('str')+'/'+finalout['确认情报'].astype('str')+')'
            print(finalout)

            if os.path.splitext(file)[-1]=='.csv':
                finalout.to_csv(os.path.join(path,sys.argv[3]+'_'+iso+'_'+file))
            else:
                finalout.to_excel(os.path.join(path,sys.argv[3]+'_'+iso+'_'+file))
    
    def pro26(self):  #准确率统计——临时版
        if sys.argv[3]=='JME':
            cartype='RT6'
        elif sys.argv[3]=='ARCF':
            cartype='RT5'
        else:
            cartype='.'
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        taskfile='全量导出-有task.csv'
        pathraw = sxg.routecon(sys.argv[1])
        dflist=[]
        for filevar in os.listdir(pathraw):
            path=pathraw+'/'+filevar
            file = '全量导出-无task.csv'
            datadf = pd.read_csv(os.path.join(path,file))
            taskdatadf = pd.read_csv(os.path.join(path,taskfile))
            isolist_sort=['']
            vardf = self.zhy_ana3(datadf,'Fence','',taskdatadf,sys.argv[3])
            dflist.append(vardf)
        finalout=pd.concat(dflist)
        
        finalout.to_csv(os.path.join(path,sys.argv[1]+'_'+'TOPOLOGY_UPDATE'+'_'+file))
        
    def pro27(self):  #out.csv生成json文件
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        if os.path.splitext(file)[-1]=='.csv':
            df = pd.read_csv(os.path.join(path,file))
        else:
            df = pd.read_excel(os.path.join(path,file))
        tarlist=df[['taskid','exactts']].values.tolist()
        outdict={'source_type':2}
        tasklist=[]
        for var in tarlist:
            vartask=var[0]
            extime=var[1]
            if isinstance(vartask, str):
                vardict={}
                vardict['task_id']=vartask
                vardict['start_time']=extime-30
                vardict['end_time']=extime+15
                vardict['exact_time']=extime
                tasklist.append(vardict)
            else:
                pass
        outdict['task_list']=tasklist
        # out_str = json.dumps(outdict, indent=4)
        with open(os.path.join(path,file[:-4]+'_data.json'), 'w') as f:
            json.dump(outdict, f, indent=4)
        print(outdict)
    
    def pro28(self):  #阻塞case整理
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        if os.path.splitext(file)[-1]=='.csv':
            df = pd.read_csv(os.path.join(path,file))
        else:
            df = pd.read_excel(os.path.join(path,file))
        tarlist=df[['polygonID','刘俊洋补充case']].values.tolist()
        outlist=[]
        for var in tarlist:
            tempvarout=[]
            casestr=var[1]
            caselist=casestr.split(' ')
            for casevar in caselist:
                urlstrlist=casevar.split('/')
                caseid=''
                for urlvar in urlstrlist:
                    if 'MRDR-' in urlvar:
                        caseid=urlvar
                outlist.append([var[0],casevar,caseid])
        
        columnsname=['polygonID','caseurl','caseid']
        outdf=pd.DataFrame(outlist,columns=columnsname)
        outdf.to_excel(os.path.join(path,'out_'+file),index=False)
    

    def pro29(self):  #准确率统计——去掉任务统计
        if sys.argv[2]=='JME':
            cartype='RT6'
        elif sys.argv[2]=='ARCF':
            cartype='RT5'
        else:
            cartype='.'
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        # taskfile=sys.argv[2]
        if os.path.splitext(file)[-1]=='.csv':
            datadf = pd.read_csv(os.path.join(path,file))
            # taskdatadf = pd.read_csv(os.path.join(path,taskfile))
        else:
            datadf = pd.read_excel(os.path.join(path,file))
            # taskdatadf = pd.read_excel(os.path.join(path,taskfile))
        #获取版本信息
        isoverlist=datadf['intelligence_iso_version'].tolist()
        iso_num=[]
        for var in isoverlist:
            try:
                tempvar=var.split('.')
                if tempvar[0]=='6' and tempvar[1]=='2':
                    iso_num.append(int(tempvar[2]))
            except:
                pass
        isodict=dict(Counter(iso_num))
        print(isodict)
        isodf=pd.DataFrame([isodict])
        isodf2=pd.DataFrame(isodf.values.T,index=isodf.columns,columns=['num'])
        isodf2_sort=isodf2.sort_values(by='num',ascending=False)
        isolist_sort=list(isodf2_sort.index)
        isolist_sort.insert(0,'')
        for i in range(len(isolist_sort)):
            iso='6.2.'+str(isolist_sort[i])+'.'
            tarstr=['Fence','Road','RoadTurn']
            dflist=[]
            for var in tarstr:
                vardf = self.zhy_ana2(datadf,var,iso,sys.argv[2])
                dflist.append(vardf)
            finalout=pd.concat(dflist)

            finalout['确认情报'] = finalout['误报库有效'] + finalout['误报库无效'] + finalout['人审有效']+ finalout['人审无效']
            finalout['确认有效情报']= finalout['误报库有效']+finalout['人审有效']
            finalout.loc['Row_sum'] = finalout.apply(lambda x: x.sum())

            finalout['有效率']=finalout['确认有效情报']/finalout['确认情报']
            finalout['有效率']=finalout['有效率'].apply(lambda x: '{:.2%}'.format(x))
            finalout['out'] = finalout['有效率'].astype('str')+'('+finalout['确认有效情报'].astype('str')+'/'+finalout['确认情报'].astype('str')+')'
            print(finalout)

            if os.path.splitext(file)[-1]=='.csv':
                finalout.to_csv(os.path.join(path,sys.argv[2]+'_'+iso+'_'+file))
            else:
                finalout.to_excel(os.path.join(path,sys.argv[2]+'_'+iso+'_'+file))
    

    def pro30(self):  #停止线运营case处理
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        df_icafe = pd.read_excel(os.path.join(path,file),sheet_name='Sheet1')
        df_tc = pd.read_excel(os.path.join(path,file),sheet_name='Sheet2')
        # print(df_icafe.describe(), df_tc.describe())
        out=self.stopline_gtc(df_icafe,df_tc)
        out.to_excel(os.path.join(path,'out_'+file),index=False)
        


    def proex(self):
        print('''
    # ex.pro()    #主线case筛选
    # ex.pro2()   #主线召回case筛选
    # ex.pro3()   #车端报出待分析
    # ex.pro4()   #case融合
    # ex.pro5()   #从mrc中获取报出信息
    # ex.pro6()   #基于瀚天统计结果输出变更类型
    # ex.pro7()   #众源情报统计-提取坐标
    # ex.pro7_1() #众源情报统计
    # ex.pro8()   #处理李鑫导出的文件
    # ex.pro9()   #合并icafe，情报有效情报
    # ex.pro10()  #从主线数据中提取坐标
    # ex.pro11()  #基于数据集统计召回结果
    # ex.pro12()  #挑选出现频率最高的数字并排序  
    # ex.pro13()  #压线case分析结果导出
    # ex.pro14()  #整理高危报出情报
    # ex.pro15()  #通过taskid、问题时间点，查找对应的情报id
    # ex.pro16()  #复制pro15,通用匹配，根据时间戳，carid，匹配获取对应的情报id
    # ex.pro17()  #通过icafeid获取坐标和issuefinder（主线icafe空间）
    # ex.pro18()  #云代驾预警分析
    # ex.pro19()  #时间戳、北京时间转换
    # ex.pro20()  #excel,csv文件合并
    # ex.pro21()  #MRC报出根因统计
    # ex.pro18_1()#云代驾预警分析,预警报出时间
    # ex.pro22()  #众源情报准确率统计
    # ex.pro23()  #无效分析，基于车端可分析结果输出最终的分析case
    # ex.pro24()  #通过icafeid获取坐标和issuefinder（现实变更空间）
    # ex.pro25()  #准确率统计-新版
    # ex.pro27()  #out.csv生成json文件
    # ex.pro28()  #阻塞case整理
    # ex.pro29()  #准确率统计-无task
    # ex.pro30()  #停止线运营case处理''')
        pronum=input('选择函数序号:')
        match pronum:
            case '1':
                self.pro()
                print('22')
            case '6':
                self.pro6()
            case '7':
                self.pro7()
            case '7_1':
                self.pro7_1() 
            case '9':
                self.pro9()
            case '10':
                self.pro10()
            case '16':
                self.pro16()
            case '17':
                self.pro17()
            case '19':
                self.pro19()
            case '18_1':
                self.pro18_1()
            case '20':
                self.pro20()
            case '22':
                self.pro22()
            case '23':
                self.pro23()
            case '24':
                self.pro24()
            case '25':
                self.pro25()
            case '26':
                self.pro26()
            case '27':
                self.pro27()
            case '28':
                self.pro28()
            case '29':
                self.pro29()
            case '30':
                self.pro30()
            case _:
                print('输入有问题！')


if __name__=='__main__':
    ex=main()
    ex.proex()
    print('结束处理！')
