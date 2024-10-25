# -*- coding: utf-8 -*-
import json
import pandas as pd
def readjson():
    file='./data/polygon_object.json'
    with open(file, "r") as f:
        content = json.load(f)
    data1 = content['replyinfo']['data']['3_864_13176']['polygon_object']
    outlist=[]
    for var in data1:
        if var['type']=='GUIDE_AREA':
            dataxy=var['polygon']['point']
            i=0
            for xyvar in dataxy:
                xvar = xyvar['x']
                yvar = xyvar['y']
                outlist.append([i,xvar,yvar])
                i=i+1
    outdf=pd.DataFrame(outlist,columns=['序号','x','y'])  
    outdf.to_csv('./jsonout.csv')
    
readjson()