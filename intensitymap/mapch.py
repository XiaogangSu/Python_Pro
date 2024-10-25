#现实变更指标统计
import pandas as pd
import os
import json
import urllib.request
import pdb
from urllib.parse import urlparse,parse_qs,parse_qsl
import time

class main():
    def __init__(self) -> None:
        print('开始处理。。。')
        self.path='/mnt/d/Data/现实变更/数据集/现实变更MRC/'
        self.file='全量变更报出-2023.07.18-MRC平台-6B.xlsx'
        self.conf={'highrisk':[]}

    def readexcel(self,pathfile):
        data_df = pd.read_excel(pathfile)
        return(data_df)
    
    def getdata(self,url):
        # print(url)
        req = urllib.request.urlopen(url)
        datastr=req.read().decode()
        # print(datastr)
        try:
            null=''
            datadict=eval(datastr)['data']
            taskid=datadict['task_id']
            starttime=datadict['start_time']
            endtime=datadict['end_time']
            isoversition=datadict['iso_version']
            exactts=datadict['exact_ts']
            icafeurl=datadict['icafe_url'].replace('\\','')
            mapver=datadict['map_version']
            area = datadict['map_name']
            if len(mapver)<13:
                mapver='hdmap-'+area+'_'+mapver
        # pdb.set_trace()
        except Exception as e:
            print(e)
            taskid=''
            starttime=''
            endtime=''
            isoversition=''
            exactts=''
            icafeurl=''
            mapver=''
            area=''
        return([taskid,starttime,endtime,isoversition,exactts,icafeurl,mapver,area])
      
    def data_ana(self,datadf):
        verlist=datadf['系统版本'].unique()
        verlist2=[]
        verraw=datadf['系统版本'].str.split('.')
        # datadf['系统版本2'] = datadf['系统版本'].str.split('.').str.get(1)+datadf['系统版本'].str.split('.').str.get(2)
        datadf['系统版本2'] = verraw.str.get(1).str.cat(verraw.str.get(2),'.')
        print(verlist)
        # print(datadf['系统版本2'])
        for var in verlist:
            varlist=var.split('.')
            verlist2.append(varlist[1]+'.'+varlist[2])
        verlist2_df=pd.Series(data=verlist2)
        verlist3=verlist2_df.unique()
        print(verlist3)
        partroltype = datadf['问题根因'].unique()
        finallist=[]
        for partvar in partroltype:
            partlist=[]
            for ver in verlist3:
                # print(ver,partvar)
                partlist.append(datadf[(datadf['系统版本2']==ver) & (datadf['问题根因']== partvar)].shape[0])
            finallist.append(partlist)
        # print(finallist)
        finaldf=pd.DataFrame(finallist,index= partroltype,columns= verlist3)
        finaldf.to_excel(self.path+'out_'+self.file)
        
        return(finaldf)
    
    def data_ana2(self,pathfile):
        datadf = pd.read_excel(pathfile,index_col=0)
        indexstr=datadf.index.tolist()
        print('indexstr:',indexstr)
        jihelist=[]
        xianxinglist=[]
        for var in indexstr:
            if '几何变更预警' in var:
                jihelist.append(var)
            elif '线型变更预警' in var:
                xianxinglist.append(var)
        droplist=jihelist+xianxinglist
        datadf_jh = datadf.loc[jihelist].sum()
        datadf_jh_list = datadf_jh.tolist()
        datadf.loc['车道线几何变更']=datadf_jh_list
        datadf_x = datadf.loc[xianxinglist].sum()
        datadf_x_list = datadf_x.tolist()
        datadf.loc['车道线线型变更']=datadf_x_list
        datadf.drop(index=droplist,inplace = True)
        # print(datadf)
        datadf.to_excel(self.path+'不区分路口out_'+self.file,sheet_name='不区分路口')
    
    def readcase(self,path,file):
        datadf = pd.read_excel(path+file,usecols=['编号','问题定位工具issuefinder'])
        datalist = datadf.values.tolist()
        urlstr='http://idgdata.baidu-int.com/bee/yixiu/api/caseinfo?id=issue_url_id&from=yf&token=adt_test&ak=adt_test'
        urlstr2='https://idgdata.baidu-int.com/bee/yixiu/api/casereportinfo?id=issue_url_id&namespace=auto_car&from=yf&token=adt_test&ak=adt_test'
        urlstr3='http://idgdata.baidu-int.com/bee/yixiu/api/caseinfo?carId=caridstr&exactTs=timestamp&from=yf&token=adt_test&ak=adt_test'
        finalout=[]
        for var in datalist:
            # print(var)
            caseid = var[0].split('-')[1]
            query = urlparse(var[1]).query
            urldict = parse_qs(query)
            if 'namespace' in var[1]:
                issue_id=urldict['id'][0]
                url=urlstr2.replace('issue_url_id',issue_id)
            elif 'carId' in var[1]:
                carid = urldict['carId'][0]
                exactTs = urldict['exactTs'][0]
                url=urlstr3.replace('caridstr',carid).replace('timestamp',exactTs)
            else:
                issue_id=urldict['id'][0]
                url=urlstr.replace('issue_url_id',issue_id)

            varlist=self.getdata(url)
            varlist.insert(0,caseid)
            finalout.append(varlist)
        # print(finalout[0:2])
        colname=['caseid','taskid','starttime','endtime','isoversition','exactts','icafeurl','mapversion','area']
        finaldf=pd.DataFrame(finalout,columns=colname)
        return(finaldf)
    
    def readmrc(self,path,file):
        datadf = pd.read_excel(path+file)
        return(datadf)
    
    def gettime(self,tartime,timelist):
        indexlist=[]
        for i in range(0,len(timelist)):
            s_t = time.strptime(timelist[i], "%Y-%m-%d %H:%M:%S") 
            mkt = float(time.mktime(s_t))
            if abs(float(tartime)-mkt)<=30:
                indexlist.append(i)
        return(indexlist)


    def timematch(self,casedf,mrcdf):
        caselist = casedf.values.tolist()
        case_colm = [column for column in casedf]
        taskid_index = case_colm.index('taskid')
        time_index = case_colm.index('exactts')
        finalout=[]
        for var in caselist:
            carid = var[taskid_index].split('_')[0]
            print('taskid:',var[taskid_index])
            case_time = var[time_index]
            carmrc_df = mrcdf[mrcdf['车辆ID']==carid]
            if len(carmrc_df)==0:
                # print(len(carmrc_df))
                var.append('无MRC报警')
            else:
                indexlist = self.gettime(case_time,carmrc_df['故障时间'].tolist())
                # print(indexlist)
                if len(indexlist)==0:
                    var.append('无MRC报警')
                else:
                    mrccaseidlist = mrcdf['icafeID'].tolist()
                    mrccaseid=[]
                    for indexvar in indexlist:
                        mrccaseid.append(mrccaseidlist[indexvar])
                    caseidstr = '-'.join(mrccaseid)
                    var.append(caseidstr)
            finalout.append(var)
        columnstr=case_colm.append('MRC')
        finaldf = pd.DataFrame(finalout,columns=columnstr)
        finaldf.to_excel('/mnt/d/Data/现实变更/数据集/主线case/6月主线case-out.xlsx')
        print(finalout[0:3])

    def pro(self):
        pathfile = self.path+self.file
        datadf = self.readexcel(pathfile)
        self.data_ana(datadf)
        self.data_ana2(self.path+'out_'+self.file)
        print('处理结束！')
    
    def Q2ana(self):
        path = '/mnt/d/Data/现实变更/Q车道线变更场景.csv'
        datadf=pd.read_csv(path,encoding = 'gbk')
        scene=datadf['变更场景'].value_counts().index.tolist()
        scene_num=datadf['变更场景'].value_counts().tolist()
        finallist=[]
        for var in scene:
            varlist=[]
            # varlist.append(var)
            vardf = datadf[datadf['变更场景']==var]
            varscene=vardf['变更类型'].value_counts().index.tolist()
            varscene_num=vardf['变更类型'].value_counts().tolist()
            for i in range(0,len(varscene)):
                #  finaldict[var][varscene[i]]=varscene_num[i]
                 finallist.append([var,varscene[i],varscene_num[i]])  
        print(finallist)
        index=['变更场景','变更类型','case数']
        finaldf=pd.DataFrame(finallist,columns=index)
        finaldf.to_csv('./map.csv',encoding='gbk')
        # json_str = json.dumps(finaldict,ensure_ascii=False)
        # with open('./data.json', 'w',encoding='utf-8') as f:
        #     f.write(json_str)

        # f.close()
        
    
    def zhaohui(self):
        (path1,file1)=('/mnt/d/Data/现实变更/数据集/主线case/','6月主线case.xlsx')
        (path2,file2)=('/mnt/d/Data/现实变更/数据集/现实变更MRC/','全量变更报出-2023.07.11-MRC平台.xlsx')
        datacase = self.readcase(path1,file1)
        datamrc = self.readmrc(path2,file2)
        print(datacase.head())
        out=self.timematch(datacase,datamrc)

if __name__=='__main__':
    ex=main()
    ex.pro() #统计MRC报出，分报出类型
    ex.Q2ana() #统计MRC报出，几何、线型的路口与非路口合并
    #ex.zhaohui()  #主线case召回统计
