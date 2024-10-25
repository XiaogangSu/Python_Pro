import pandas as pd

def pro():
    file='/mnt/d/Data/现实变更/数据集/主线case/Q2车道线现实变更-0626.xlsx'
    dataQ2df=pd.read_excel(file,sheet_name='Q2')
    datakudf=pd.read_excel(file,sheet_name='已入库')
    datashdf=pd.read_excel(file,sheet_name='已审核')
    Qlist=dataQ2df['caseid'].tolist()
    kulist=datakudf['已入库'].tolist()
    shlist=datashdf['已审核'].tolist()
    print(Qlist[0:5])
    print(kulist[0:5])
    print(shlist[0:5])
    finallist=[]
    for var in Qlist:
        varlist=[var]
        if var in kulist:
            varlist.append(1)
        else:
            varlist.append(0)

        if var in shlist:
            varlist.append(1)
        else:
            varlist.append(0)
        finallist.append(varlist)
    print(finallist[:500])
    finaldf=pd.DataFrame(finallist)
    finaldf.to_csv(file+'.out.csv')

pro()