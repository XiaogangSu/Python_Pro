# 处理统计定位地图生命周期
import pandas as pd
import numpy as np
import pdb
import datetime

class main():
    def __init__(self):
        print('开始处理。。。')
        self.path='./data/'
        self.cardata='car_20230519.xlsx'   #carid、用途 存储表
        self.mapdata='map_20230519.xlsx'   #地图版本&时间，王轩平台导出
        self.map2car='map2car_20230519.xlsx'  #提测版本，提测时间，外部平台导出

    #读取车辆信息,反馈dict(id,用途)，dict(园区，路测车个数，路跑车个数，运营车个数)
    def readcar(self, filename):
        path=self.path
        pathfile=path+filename
        data_df=pd.read_excel(pathfile,index_col=0)
        print(data_df[0:5])
        data_dict=data_df.to_dict('index')
        # print(data_dict['WM198']['车辆用途'])
        L4_data_df=data_df[data_df['车辆用途'].isin(['萝卜快跑/主驾运营/主驾运营/主驾运营','萝卜快跑/副驾运营/副驾运营/副驾运营','萝卜快跑/无人运营/无人运营/无人运营','研发/探路者/QA路测/QA路测','研发/探路者/路跑/路跑']) & data_df['定位地图版号'].notnull()]
        mapid=L4_data_df['定位地图版号'].tolist()
        # print(mapid)
        area=[]
        for var in mapid:
            var_area1=var.replace('intensitymap_msg_alt_map-','').replace('-','_').replace(',','_')
            # print(var_area1.split('map_'))
            var_area=var_area1.split('_')[0]
            area.append(var_area)
        
        L4_data_df['area']=area
        area_uni=L4_data_df['area'].unique()
        print(area_uni)
        print(L4_data_df[0:5])
        area_dict={}
        for var in area_uni:
            roadtest=len(L4_data_df[L4_data_df['车辆用途'].isin(['研发/探路者/QA路测/QA路测']) & L4_data_df['area'].isin([var])])
            operation=len(L4_data_df[L4_data_df['车辆用途'].isin(['研发/探路者/路跑/路跑']) & L4_data_df['area'].isin([var])])
            publish=len(L4_data_df[L4_data_df['车辆用途'].isin(['萝卜快跑/主驾运营/主驾运营/主驾运营','萝卜快跑/副驾运营/副驾运营/副驾运营','萝卜快跑/无人运营/无人运营/无人运营']) & L4_data_df['area'].isin([var])])
            area_dict[var]=[roadtest,operation,publish]
        print(area_dict)
        data_dict=L4_data_df.to_dict('index')
        return(data_dict,area_dict)
   
    #读取车辆id及用途
    def readmap(self, filename):
        path=self.path
        pathfile=path+filename
        data_df=pd.read_excel(pathfile,skiprows=0,header=1)
        data_arr=np.array(data_df)
        data_list=data_arr.tolist()
        print(data_list[0:10])
        return(data_list)

    #读取地图版本及上车时间    
    def mergetab(self, cardict, maplist):
        outlist=[]
        # print(len(maplist))
        for var in maplist:
            try:
                mapid=var[0]
                area=mapid.split('_')[1]
                time=var[2]
                carid=var[3]
                car_lev = cardict[carid]['车辆用途'].split('/')[-1]  #获取车辆状态，直接去最后一个字段
                outlist.append([mapid,area,time,carid,car_lev])
            except:
                pass
        index=['定位地图版本','园区','上车时间','carid','车辆用途']
        out_df=pd.DataFrame(outlist,columns=index)
        out_df.to_excel(self.path+'out_'+self.mapdata,index=False)
        return(out_df)
    
    #根据地图版本查找合适的时间,返回上路跑时间
    def gettime_lupao(self,mapdf,mapid):
        # print(mapdf.describe())
        mapid_df_lupao=mapdf[(mapdf['定位地图版本']==mapid) & (mapdf['车辆用途']=='路跑')]
        print(mapid_df_lupao)
        lupao_timelist_raw=mapid_df_lupao['上车时间'].tolist()
        lupao_timelist = []
        for var in lupao_timelist_raw:
            vartime=datetime.datetime.strptime(var,'%Y-%m-%d %H:%M:%S')
            lupao_timelist.append(vartime)
        # print(lupao_timelist)
        lupao_timelist_sort=sorted(lupao_timelist)  #时间排序，从低到高
        # print(type(lupao_timelist_sort))
        # print(lupao_timelist_sort)
        timedict={}
        if len(lupao_timelist_sort)==0:
            timedict['首次上路跑时间']='未上车'
            timedict['最晚上路跑时间']='未上车'
            timedict['路跑车部署量']=len(lupao_timelist_sort)
        else:
            timedict['首次上路跑时间']=lupao_timelist_sort[0]
            timedict['最晚上路跑时间']=lupao_timelist_sort[-1]
            timedict['路跑车部署量']=len(lupao_timelist_sort)
        return(timedict)
    
    #根据地图版本查找合适的时间,返回上运营的时间
    def gettime_yunying(self,mapdf,mapid):
        # print(mapdf.describe())
        mapid_df_yunying=mapdf[(mapdf['定位地图版本']==mapid) & ((mapdf['车辆用途']=='副驾运营') | (mapdf['车辆用途']=='无人运营') | (mapdf['车辆用途']=='主驾运营'))]
        yunying_timelist_raw=mapid_df_yunying['上车时间'].tolist()
        yunying_timelist = []
        for var in yunying_timelist_raw:
            vartime=datetime.datetime.strptime(var,'%Y-%m-%d %H:%M:%S')
            yunying_timelist.append(vartime)
        # print(lupao_timelist)
        yunying_timelist_sort=sorted(yunying_timelist)  #时间排序，从低到高
        # print(type(lupao_timelist_sort))
        # print(lupao_timelist_sort)
        timedict={}
        if len(yunying_timelist_sort)==0:
            timedict['首次上运营时间']='未上车'
            timedict['最晚上运营时间']='未上车'
            timedict['运营车部署量']=len(yunying_timelist_sort)
        else:
            timedict['首次上运营时间']=yunying_timelist_sort[0]
            timedict['最晚上运营时间']=yunying_timelist_sort[-1]
            timedict['运营车部署量']=len(yunying_timelist_sort)
        return(timedict)

    #读取发版地图并赋值上车时间
    def readmap2car(self, filename, mergedf,areadict):
        path=self.path
        pathfile=path+filename
        data_df=pd.read_excel(pathfile)
        data_arr=np.array(data_df)
        data_list=data_arr.tolist()
        # print(data_list[0:10])
        finalout=[]
        for var in data_list:
            mapid=var[0].strip()
            createtime=var[1]
            # print('mapid=',mapid)
            areaid1=mapid.replace('Intensitymap_','').replace('-','_').replace(',','_')
            areaid=areaid1.split('_')[0]
            # print(areaid1)
            tocartime_lupao_first=self.gettime_lupao(mergedf,mapid)['首次上路跑时间']
            tocartime_lupao_end=self.gettime_lupao(mergedf,mapid)['最晚上路跑时间']
            tocartime_lupao_num=self.gettime_lupao(mergedf,mapid)['路跑车部署量']
            lupao_num=areadict[areaid][1]

            tocartime_yunying_first=self.gettime_yunying(mergedf,mapid)['首次上运营时间']
            tocartime_yunying_end=self.gettime_yunying(mergedf,mapid)['最晚上运营时间']
            tocartime_yunying_num=self.gettime_yunying(mergedf,mapid)['运营车部署量']
            yunying_num=areadict[areaid][2]
            
            finalout.append([mapid,createtime,tocartime_lupao_first,tocartime_lupao_end,tocartime_lupao_num,lupao_num,tocartime_yunying_first,tocartime_yunying_end,tocartime_yunying_num,yunying_num])
            # finalout.append([mapid,createtime,tocartime_lupao_first,tocartime_lupao_end,tocartime_lupao_num])
 
        indexname=['定位地图版本','创建时间','首次上路跑时间','路跑最后部署时间','路跑车部署量','路跑车总数','首次上运营时间','运营最后部署时间','运营车部署量','运营车总数']
        # indexname=['定位地图版本','创建时间','首次上路跑时间','路跑最后部署时间','路跑车部署量']
        final_df=pd.DataFrame(finalout,columns=indexname)
        final_df.to_excel(self.path+'out_'+self.map2car,index=False)
        # return(data_list)

    def pro(self):
        car_dict,area_dict = self.readcar(self.cardata)

        map_list = self.readmap(self.mapdata)

        merge_df = self.mergetab(car_dict,map_list)
        self.readmap2car(self.map2car,merge_df,area_dict)

        print('处理完成！')


if __name__ == '__main__':
    ex = main()
    ex.pro()
