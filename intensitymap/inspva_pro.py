import os
def inspro(path):
    print('开始处理！')
    for var in os.listdir(path):
        fullpath=path+'/'+var[11:44]
        os.system('mkdir '+fullpath)
        os.system('/mnt/d/code/cybertron/output/cybertron/bin/localization_record_parse '+path+'/'+var+' '+fullpath+'/')



if __name__=='__main__':
    path='/mnt/d/Data/现实变更/现实变更巡检/第二次巡检/track'
    inspro(path)
