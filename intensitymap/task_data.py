# task数据查询
# !/usr/bin/env python3
import argparse
import subprocess
import json
import pandas as pd
import logging
import os
import pdb
import time

class main():
    def __init__(self):
        print('开始处理。。。')
    
    def downloadverjson(self,tn):
        tn=tn.replace(' ','')
        print('tn:',tn)
        carid=tn.split('_')[0]
        datevar=tn.split('_')[1][0:8]
        cmd_mkdir='mkdir ./'+tn
        # print(cmd_mkdir.split())
        mkdir=subprocess.Popen(cmd_mkdir.split())
        cmd_v=('/mnt/d/code/adb_client/bin/adb_data get /auto_car/'+carid+'/'+datevar+'/'+tn+'/carversion/cmptnode/realtime_version.json ./'+tn+'/')
        vjson=subprocess.Popen(cmd_v.split())
        vjson.wait()
        print('下载地图版本文件成功！')
    
    def readjson(self, pathname):  #json数据读取解析,返回字典数据类型
        with open(pathname, 'r') as load_f:
            load_dict = json.load(load_f)
        return(load_dict)

    def showtask(self,tn):
        #读取定位地图版本信息
        try:
            map_v=self.readjson('./'+tn+'/realtime_version.json')
            msg=map_v['intensitymap_msg_lossy'][1]
            vis=map_v['vismap'][1]
            hdmap=map_v['hdtilemap'][1]
        except:
            map_v='null'
            msg='null'
            vis='null'
            hdmap='null'
        cmd=('/mnt/d/code/adb_client/bin/task_client query -t '+ tn)
        p=subprocess.Popen(cmd.split(),stdout=subprocess.PIPE)
        data_tuple = p.communicate()
        data_str = data_tuple[0].decode('utf-8')
        data_dict = json.loads(data_str)
        p.wait()
        index=['taskid','开始时间','结束时间','园区名称','行驶里程','闭环里程','hdmap','msg','vis','iso','任务类型']
        datam=data_dict['data'][0]['meta']
        datalist=[tn,datam['start_time'],datam['end_time'],datam['capture_place'],datam['miles'],datam['ad_miles'],hdmap,msg,vis,datam['cmpt_version'],datam['task_purpose']]
        datalist2=[[tn,datam['start_time'],datam['end_time'],datam['capture_place'],datam['miles'],datam['ad_miles'],hdmap,msg,vis,datam['cmpt_version'],datam['task_purpose']]]
        datadf=pd.DataFrame(datalist2,columns=index)
        # cmd_mkdir='mkdir ./'+tn
        # print(cmd_mkdir.split())
        # mkdir=subprocess.Popen(cmd_mkdir.split())
        # datadf.to_csv('./'+tn+'/'+tn+'.csv',encoding='gbk',index=False)
        # print(datadf.to_dict())
        return(datadf.to_dict(),datalist2)

    def cardate(self,carid,date):
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
        print(tasklist)
        return(tasklist)
    
    def savetasks(self,tasklist,tn):
        finaldata=[]
        for var in tasklist:
            # self.downloadverjson(var)
            taskdata=self.showtask(var)[1][0]
            finaldata.append(taskdata)
        print(finaldata[0])
        index=['taskid','开始时间','结束时间','园区名称','行驶里程','闭环里程','hdmap','msg','vis','iso','任务类型']
        datadf=pd.DataFrame(finaldata,columns=index)
        print(datadf.head())
        # pdb.set_trace()

        datadf.to_csv('./data/'+tn+'.csv',encoding='gbk',index=False)
    
    #根据carid与问题时间获取iso信息
    def getcarmes(self,carid,timevar):
        datestr = timevar.split(' ')[0].replace('-','')
        timestr = timevar.split(' ')[1]
        carlist = self.cardate(carid,datestr)
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

    def mapchangemessage(self,path,file):
        pathfile=path+'/'+file
        datadf=pd.read_excel(pathfile)
        datadf2=datadf[['情报ID','情报标题','要素类型','区域','情报来源','创建时间','高精地图版本号','车辆ID','情报有效性','是否重复反馈','变更位置']]
        datadf3=datadf2[(datadf2['要素类型']=='车道线')&(datadf2['区域']=='YiZhuangDaLuWang')]
        datalist=datadf3.values.tolist()
        column_index = datadf3.columns.tolist()
        # print(type(column_index))
        time_index = column_index.index('创建时间')
        carid_index = column_index.index('车辆ID')
        finallist = []
        for var in datalist:
            time=var[time_index]
            carid=var[carid_index]
            # try:
            carid = self.getcarmes(carid,time)
            var.append(carid)
            (datadict,datalistc) = self.showtask(carid)
            isover=datadict['iso'][0]
            print(carid,isover)
            pdb.set_trace()
            # print(isover)
            var.append(isover)
            finallist.append(var)
            # except:
            #     print('error var')
                # pass
        column_index=datadf3.columns.tolist()
        finaldf=pd.DataFrame(finallist,columns=column_index)
        finaldf.to_csv('./testout.csv',encoding='gbk',index=False)


    # def download(self, filename, outdir, content="lmod", force_download=False):
    def download(self,filename,content):
        filename='./tasklist.csv'
        task_ids = []
        starts = []
        ends = []
        with open(filename) as f:
            for line in f:
                # print(line)
                splits = line.split(',')
                taskid, s, e = splits[:3]
                task_ids.append(taskid)
                starts.append(s)
                ends.append(e)
        del task_ids[0]
        del starts[0]
        del ends[0]

        for i, task_id in enumerate(task_ids):
            # try:
            #     # print(task_id)
            #     query_cmd = "/mnt/d/code/adb_client/bin/task_client query -t " + task_id
            #     query = subprocess.Popen(query_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #     query.wait(10)
            #     task_meta = json.loads(query.stdout.read())
            #     meta = task_meta["data"][0]["meta"]
            #     start_time = int(meta["start_time"])
            #     end_time = int(meta["end_time"])
            #     is_hw60 = int(meta["cmpt_version"].split(".")[0]) > 5
            # except KeyboardInterrupt as e:
            #     raise e
            # except Exception as e:
            #     logging.info("query task meta fails, continue")
            #     raise e
            # logging.info(
            #     f"start_time: {start_time} end_time: {end_time} is_hw60: {is_hw60}")
            # seq_id = 0
            car_id = task_id.split("_")[0]

            # task_time = task_id.split("_")[1]
            beg_time = starts[i]
            stop_time = ends[i]

            if content == "inspva":
                topics = ("/sensor/novatel/inspva")
            else:
                raise ValueError(f"Content not exists {content}")
            print(f'./{car_id}_{str(beg_time)}_{str(stop_time)}.record')
            dump_cmd = (
                "/mnt/d/code/adb_client/bin/task_client dump -t "
                + task_id
                + " --namespace auto_car -s "
                + str(beg_time)
                + " -e "
                + str(stop_time)
                + " --no-param --topics "
                + topics
                + " --output "
                + f'./{car_id}_{str(beg_time)}_{str(stop_time)}.record'
            )
            logging.info(dump_cmd)
            print(dump_cmd)
            dump_proc = subprocess.Popen(dump_cmd.split())
            dump_proc.wait()

    def savetaskmes(self,taskfile):
        taskdata=[]
        with open(taskfile) as f:
            for line in f:
                self.downloadverjson(line)
                datalist = self.showtask(line)
                taskdata.append(datalist)
        index=['taskid','开始时间','结束时间','园区名称','行驶里程','闭环里程','hdmap','msg','vis','iso','任务类型']
        datadf=pd.DataFrame(taskdata,columns=index)
        datadf.to_csv(taskfile+'.out.csv',encoding='gbk',index=False)

    def pro(self):
        parser = argparse.ArgumentParser(description='【使用说明】')
        parser.add_argument('-tn',help='输入task名称')  
        parser.add_argument('-c',help='输入carid_date') 
        parser.add_argument('-cs',help='输入carid_date')  
        parser.add_argument('-td',help='tasklist文件地址')
        parser.add_argument("-tt", help="topic类型", default="inspva")   
        parser.add_argument('-ma', help="输入众源情报导出文件")
        parser.add_argument('-tl', help="tasklist文件地址")
        args = parser.parse_args()
        taskname = args.tn
        cardate = args.c
        cardates = args.cs
        # inspvad = args.td
        mesfile = args.ma
        if taskname:
            self.downloadverjson(taskname)
            self.showtask(taskname)
        elif cardate:
            carid = cardate.split('_')[0]
            date = cardate.split('_')[1]
            self.cardate(carid,date)
        elif cardates:
            carid = cardates.split('_')[0]
            date = cardates.split('_')[1]
            tasklist = self.cardate(carid,date)
            self.savetasks(tasklist,cardates)
        elif args.tl:
            self.savetaskmes(args.tl)
        elif args.td:
            # print(inspvad)
            self.download(args.td,args.tt)
        elif mesfile:
            dir_name = os.path.dirname(mesfile)
            file_name = os.path.basename(mesfile)
            # print(dir_name,file_name)
            self.mapchangemessage(dir_name,file_name)

    def pro2(self):
        cardatelist=['ARCF1068_20230728','ARCF1068_20230731','ARCF1054_20230802','ARCF1068_20230804','ARCF1068_20230807','ARCF219_20230809','ARCF1068_20230811','ARCF1068_20230814','ARCF294_20230816','ARCF1068_20230818','ARCF1260_20230821','ARCF149_20230823','ARCF1146_20230825','ARCF1146_20230828','ARCF219_20230830','ARCF1146_20230901','ARCF1146_20230904','ARCF067_20230906','ARCF1299_20230908','ARCF1146_20230911','ARCF123_20230913']
        finallist = []
        for var in cardatelist:
            carid = var.split('_')[0]
            date = var.split('_')[1]
            tasklist = self.cardate(carid,date)
            finallist = finallist+tasklist
        self.savetasks(finallist,'0914')

if __name__=='__main__':
    ex=main()
    ex.pro()
    # ex.pro2()
    
