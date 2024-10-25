# -*- coding:UTF-8 -*-
import urllib.request
from urllib import parse
import pandas as pd
import pdb
from urllib.parse import urlparse,parse_qs,parse_qsl
import sys
import json
import os
sys.path.append('../')
# from sxgmodule.package import *
import sxgmodule.package as sxg

class main():
    def __init__(self):
        print('开始处理...')    
    
    def readicafe(self,mrdrlist):
        urlstr='http://mrdr.icafeapi.baidu-int.com/api/spaces/MRDR/cards?u=suxiaogang&pw=VVVa05jBYu9aDLJIKYtDCMY3759p6jk6rZi&iql=searchstr'
        out=[]
        for var in mrdrlist:
            qustr='编号 = '+str(var)
            print(qustr)
            result=parse.quote(qustr)
            urlvar = urlstr.replace('searchstr',result)
            try:
                req = urllib.request.urlopen(urlvar)
                datastr=req.read().decode()
                datadict=json.loads(datastr)
                maindata=datadict['cards'][0]['properties']
                temp=[var]
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
                out.append(temp)   
            except:
                print(str(var)+'获取失败！')
                out.append([var,'','','',''])
            # try:
            #     req = urllib.request.urlopen(urlvar)
            #     datastr=req.read().decode()
            #     datadict=eval(datastr)['cards'][0]
            #     print(datadict.keys())
            #     pdb.set_trace()
            # except:
            #     
        columnsname=['icafeid','x','y','问题时间点','issuefinder']
        outdf=pd.DataFrame(out,columns=columnsname)
        return(outdf)
            

    def pro1(self):  #根据icafeid批量获取信息
        print('处理完成！')
        # path,file = sxg.openfile()
        # path = '/mnt/d/现实变更/数据集/主线case/压线'
        # file = 'jy_1124.xlsx'
        (path,file)=os.path.split(sxg.routecon(sys.argv[1]))
        df = pd.read_excel(os.path.join(path,file))
        mrdrlist=df['icafeid'].tolist()
        outdf=self.readicafe(mrdrlist)
        outdf.to_excel(os.path.join(path,'out_'+file),index=False)

    def pro2(self):
        pass


if __name__=='__main__':
    ex = main()
    ex.pro1()
