# 获取taskid是否使用新版相对高方案
import pandas as pd
import subprocess
import pdb
import os
import sys

def main():
    print('开始处理。。。')
    path,file=os.path.split(sys.argv[1])
    data = pd.read_excel(os.path.join(path,file))
    taskidlist=data['taskid'].tolist()
    out=[]
    for var in taskidlist:
        try:
            carid=var.split('_')[0]
            datestr=var.split('_')[1][0:8]
            print(carid,datestr)
            cmd=('/mnt/d/code/adb_client/bin/adb_data ls /auto_car/'+carid+'/'+datestr)
            p=subprocess.Popen(cmd.split(),stdout=subprocess.PIPE)
            data_tuple = p.communicate()
            data_str = data_tuple[0].decode('utf-8')
            tasklist=data_str.split('\n')
            print('tasklist:',tasklist)
            cmd2=('/mnt/d/code/adb_client/bin/adb_data ls '+tasklist[-2]+'/otherlog/cmptnode/xlog/log')
            print(cmd2)
            p2=subprocess.Popen(cmd2.split(),stdout=subprocess.PIPE)
            data_tuple2 = p2.communicate()
            data_str2 = data_tuple2[0].decode('utf-8')
            # print(data_str2)
            allloglist=data_str2.split('\n')
            num=0
            for var1 in allloglist:
                if 'lane_localization' in var1:
                    # print(var)
                    num=num+1
            print(num)
            out.append([var,num])
        except:
            print('获取错误')
            out.append([var,''])
        print(out)
    data['taskid','new']=out
    data.to_excel(os.path.join(path,'out_'+file))

main()