# 根据issuefinder url list 获取对应的taskid，起止时间
import urllib.request
import pandas as pd
import pdb
from urllib.parse import urlparse,parse_qs,parse_qsl
import sys
import os
sys.path.append('../')
# from sxgmodule.package import *
import sxgmodule.package as sxg

class main():
    def __init__(self):
        print('开始处理！')
        self.urllist='./issueurl_list.txt'
        
    
    # 读取urllist并，反馈issuefinder id list
    def readtxt(self,pathfile):
        f=open(pathfile)
        data=f.readlines()
        idlist=[]
        for i in range(len(data)):
            var=data[i]
        # for var in data:
            try:
                query = urlparse(var).query
                urldict = parse_qs(query)
                # print(urldict)
                if 'namespace' in var:
                    idlist.append([urldict['id'][0], 'namespace'])
                elif 'carId' in var:
                    # extime=sxg.timetra(urldict['exactTs'][0],1)
                    idlist.append([urldict['carId'][0], urldict['exactTs'][0]])
                    # idlist.append([urldict['carId'][0], extime])
                else:
                    idlist.append([urldict['id'][0], 'no_namespace'])
            except:
                idlist.append(['999999','999999'])
                pass
        # print(idlist)
        f.close()
        return(idlist)


    def getdata(self,url):
        try:
            req = urllib.request.urlopen(url)
            datastr=req.read().decode()
            null=''
            datadict=eval(datastr)['data']
            taskid=datadict['task_id']
            starttime=datadict['start_time']
            endtime=datadict['end_time']
            isoversition=datadict['iso_version']
            exactts=datadict['exact_ts']
            # print(type(exactts),exactts)
            extime_bj=sxg.timetra(int(float(exactts)),1)
            icafeurl=datadict['icafe_url'].replace('\\','')
            mapver=datadict['map_version']
            area = datadict['map_name']
            lat = datadict['lat']
            lon = datadict['lng']
            onlineconfigname=datadict['online_config_name']
            if len(mapver)<13:
                mapver='hdmap-'+area+'_'+mapver
        except Exception as e:
            print(e)
            taskid=''
            starttime=''
            endtime=''
            isoversition=''
            exactts=''
            extime_bj=''
            icafeurl=''
            mapver=''
            area=''
            lat = ''
            lon = ''
            onlineconfigname=''
        return([taskid,starttime,endtime,isoversition,exactts,icafeurl,mapver,area,lat,lon,onlineconfigname,extime_bj])
       
    def pro(self):
        #检查out.csv是否可操作
        datadf_clear=pd.DataFrame()
        try:
            datadf_clear.to_csv('./out.csv',index=False)
        except Exception as e:
            print(e)
            print('out.csv 文件入法操作，请先关闭相关应用')
            sys.exit()
        urlstr='http://idgdata.baidu-int.com/bee/yixiu/api/caseinfo?id=issue_url_id&from=yf&token=adt_test&ak=adt_test'
        urlstr2='https://idgdata.baidu-int.com/bee/yixiu/api/casereportinfo?id=issue_url_id&namespace=auto_car&from=yf&token=adt_test&ak=adt_test'
        urlstr3='http://idgdata.baidu-int.com/bee/yixiu/api/caseinfo?carId=caridstr&exactTs=timestamp&from=yf&token=adt_test&ak=adt_test'
        idlist=self.readtxt(self.urllist)
        # pdb.set_trace()
        colname=['taskid','starttime','endtime','isoversion','exactts','icafeurl','mapversion','area','lat','lon','onlineconfigname','extime_bj']
        alldata=[]
        for var in idlist:
            varid=var[0]
            print(varid)
            varid=varid.rstrip()
            if var[1] == 'namespace':
                url=urlstr2.replace('issue_url_id',varid)
            elif var[1] == 'no_namespace':
                url=urlstr.replace('issue_url_id',varid)
            else:
                url=urlstr3.replace('caridstr',var[0]).replace('timestamp',var[1])
            varlist=self.getdata(url)
            alldata.append(varlist)
        # print(alldata)
        datadf=pd.DataFrame(alldata,columns=colname)
        datadf.to_csv('./out.csv',index=False)
        print('处理结束！')

    
if __name__ == '__main__':
    ex=main()
    ex.pro()
