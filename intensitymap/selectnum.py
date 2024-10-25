#匹配筛选
import pandas as pd

def run():
    list1df=pd.read_excel('./data/s.xlsx',sheet_name='Sheet1',header=None)
    list2df=pd.read_excel('./data/s.xlsx',sheet_name='Sheet2',header=None)
    list1 = list1df.values.tolist()
    list2 = list2df.values.tolist()
    out1=[]
    out2=[]
    for var in list1:
        # var_re=var.replace(' \n','')
        out1.append(var[0])

    for var in list2:
        # var_re=var.replace(' \n','')
        out2.append(var[0])
    
    print(out1[0:5],out2[0:5])
    out=[]
    for var in out1:
        if var in out2:
            out.append([var,1])  #存在
        else:
            out.append([var,0])  #不存在
    print(out[-5:])
    index=['s1','是否存在']
    out_df=pd.DataFrame(out,columns=index)
    out_df.to_csv('./data/sout.csv',encoding='gbk',index=False)


if __name__=='__main__':
    run()