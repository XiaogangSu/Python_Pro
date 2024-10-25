#该脚本用于合并cybertron 解析的inspva脚本，解析record
import os
import pdb
import pandas as pd

class main():
    def __init__(self):
        print('开始处理！')
        self.path=''
    
    def readinspva(self,file):
        n=100  #抽取系数
        f=open(file,'r')
        lines=f.readlines()
        # print(lines[0:5])
        outlist=[]
        for i in range(1,len(lines),n):
            var=lines[i]
            var_split=var.split(' ')
            time=float(var_split[0])
            lon=float(var_split[1])
            lat=float(var_split[2])
            outlist.append([time,lon,lat])
        return(outlist)
    
    def inspro(self, path):
        outpath = path+'_out'
        os.system('mkdir '+outpath)
        for var in os.listdir(path):
            savepath = outpath+'/'+var
            os.system('mkdir '+savepath)
            filepath=path+'/'+var
            os.system('/mnt/d/code/cybertron/output/cybertron/bin/localization_record_parse '+filepath+' '+savepath+'/')

    def pro(self,path):
        finalout=[]
        for var in os.listdir(path):
            file=path+'/'+var+'/'+'novatel_inspva_2.txt'
            data=self.readinspva(file)
            finalout.extend(data)
        index=['time','lon','lat']
        data_df=pd.DataFrame(finalout,columns=index)
        print(path+'.csv')
        data_df.to_csv(path+'.csv',index=False)
        print('处理结束！')


if __name__=='__main__':
    ex=main()
    datapath = '/mnt/d/clear/record/9051'
    # ex.inspro(datapath)
    inspath = datapath+'_out'
    ex.pro(inspath)
