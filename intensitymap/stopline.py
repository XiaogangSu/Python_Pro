#该脚本用于合并cybertron 解析的inspva脚本，解析record
import os
import pdb
import pandas as pd
import time
import datetime

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
            time_var=float(var_split[0])
            time1 = time.localtime(time_var)
            bjtime = time.strftime("%Y/%m/%d %H:%M:%S", time1)
            # print(time_var,time1,bjtime)
            # pdb.set_trace()
            lon=float(var_split[1])
            lat=float(var_split[2])
            outlist.append([time_var,lon,lat,bjtime])
        return(outlist)
    
    def inspro(self, path):
        """
        将指定路径下的所有record文件进行本地化记录解析，并将结果保存在新的文件夹中。

        """
        outpath = path+'_out'
        os.system('mkdir '+outpath)
        for var in os.listdir(path):
            savepath = outpath+'/'+var
            os.system('mkdir '+savepath)
            filepath=path+'/'+var
            os.system('/mnt/d/code/cybertron/output/cybertron/bin/localization_record_parse '+filepath+' '+savepath+'/')

    def readlog_bak(self,path,file):
        f=open(path+file,'r')
        lines=f.readlines()
        cor = []
        i=1
        for var in lines:
            if 'x:' in var or 'y:' in var:
                corvar = float(var.split(':')[1])
                if abs(corvar)>1000:
                    print(var,i)
                    cor.append(corvar)
            i=i+1
        wktcor=[]
        for i in range(0,len(cor),6):
            wkt_var = 'LineString('
            wkt_temp = ''
            for j in range(i,i+6,2):
                # print(str(cor[j])+' '+str(cor[j+1]))
                wkt_temp = wkt_temp+str(cor[j])+' '+str(cor[j+1])+','
                print(wkt_temp)
            wkt_temp=wkt_temp.rstrip(',')
            wktcor.append([i,'LineString('+wkt_temp+')'])
        print(wktcor)
        indexstr=['num','wktgeom']
        cordf = pd.DataFrame(wktcor,columns=indexstr)
        cordf.to_csv(path+'out_'+file+'.csv',index=False)


        print(cor,len(cor))

    def readlog_clear(self,path,file):
        f=open(path+file,'r')
        lines=f.readlines()
        tar = []
        timestamplist=[]
        corlist=[]
        poslist=[]
        i=0
        for var in lines:
            if 'timestamp_sec' in var:
                timestamplist.append(float(var.split(':')[1]))
            elif ('x:' in var and float(var.split(':')[1])>1000) or ('y:' in var and float(var.split(':')[1])>1000):
                corlist.append(float(var.split(':')[1]))
            elif 'vehicle_pose' in var and float(var.split(':')[1])>1000:
                poslist.append(float(var.split(':')[1]))
            else:
                pass
            i=i+1
        out=[]
        print(len(timestamplist))
        print(len(corlist))
        pdb.set_trace()
        print(len(corlist))
        for i in range(len(timestamplist)):
            vartemp=[]
            vartemp.append(timestamplist[i])
            geomtype='Polygon(('
            geom=''
            for j in range(i*8,i*8+7,2):
                geom = geom+str(corlist[j])+' '+str(corlist[j+1])+','
            geom=geomtype+geom.rstrip(',')+','+geom.split(',')[0]+'))'
            print(geom)
            vartemp.append(geom)
            out.append(vartemp)
        indexstr=['time','wktgeom']
        cordf = pd.DataFrame(out,columns=indexstr)
        cordf.to_csv(path+'out_'+file+'.csv',index=False)
            
    def readlog(self,path,file):
        f=open(path+file,'r')
        lines=f.readlines()
        tar = []
        timestamplist=[]
        corlist=[]
        poslist=[]
        hdmapids=[]
        i=0
        for var in lines:
            if 'timestamp_sec' in var:
                timestamplist.append([i,float(var.split(':')[1])])
            elif ('x:' in var and float(var.split(':')[1])>1000) or ('y:' in var and float(var.split(':')[1])>1000):
                corlist.append([i,var.split(':')[1].strip('\n').strip(' ')])
            elif 'vehicle_pose' in var and float(var.split(':')[1])>1000:
                poslist.append([i,var.split(':')[1].strip('\n').strip(' ')])
            elif 'hdmap_id' in var:
                hdmapids.append([i,var.split(':')[1].strip('\n')])
            else:
                pass
            i=i+1
        corlistdf = pd.DataFrame(corlist,columns=['num','cor'])
        hdmapiddf = pd.DataFrame(hdmapids,columns=['num','hdmapid'])
        posdf = pd.DataFrame(poslist,columns=['num','cor'])
        out=[]
        for i in range(len(timestamplist)):
            if i == len(timestamplist)-1:
                beginindex = timestamplist[i][0]
                endindex = timestamplist[i][0]+150
            else:
                beginindex = timestamplist[i][0]
                endindex = timestamplist[i+1][0]
            print(beginindex,endindex)
            carpos = posdf[(posdf['num']>beginindex) & (posdf['num']<endindex)].values.tolist()
            carpospro= [carpos[0][1],carpos[1][1]]
            hdmapid = hdmapiddf[(hdmapiddf['num']>beginindex) & (hdmapiddf['num']<endindex)].values.tolist()
            hdmapidtar=[]
            for idvar in hdmapid:
                # print(idvar)
                if len(idvar[1])>5:
                    hdmapidtar.append(idvar)
            if len(hdmapidtar)==0:
                pass
            else:
                hdmapidfinal = hdmapidtar[0][1]
                corlistex = corlistdf[(corlistdf['num']>hdmapidtar[0][0]) & (corlistdf['num']<hdmapidtar[0][0]+25)].values.tolist()
                corall=[[corlistex[0][1],corlistex[1][1]],[corlistex[2][1],corlistex[3][1]],[corlistex[4][1],corlistex[5][1]]]
                print(corall)
                geofinal='Polygon(('+' '.join(carpospro)+','+' '.join(corall[0])+','+' '.join(corall[1])+','+' '.join(corall[2])+','+' '.join(carpospro)+'))'
                out.append([timestamplist[i][0],timestamplist[i][1],hdmapidfinal,geofinal])
            
        indexstr=['num','time','hdmapid','wktgeom']
        cordf = pd.DataFrame(out,columns=indexstr)
        cordf.to_csv(path+'out2_'+file+'.csv',index=False)
            

    def pro(self,path):
        finalout=[]
        index=['time','lon','lat','bjtime']
        os.system('mkdir '+path+'_txt')
        for var in os.listdir(path):
            print(var)
            file=path+'/'+var+'/'+'novatel_inspva_2.txt'
            data=self.readinspva(file)
            finalout.extend(data)
            data_df=pd.DataFrame(finalout,columns=index)
            data_df.to_csv(path+'_txt/'+var+'.txt',index=False)
        print('处理结束！')



if __name__=='__main__':
    ex=main()
    datapath = '/mnt/d/Data/现实变更/数据集/地图融合/record'
    # ex.inspro(datapath)
    inspath = datapath+'_out'
    # ex.pro(inspath)
    ex.readlog('/mnt/d/Data/现实变更/数据集/地图融合/0828_out/ARCF167_20230607140000/log/','hdmap-server.log.20230828-161417.1544446')
