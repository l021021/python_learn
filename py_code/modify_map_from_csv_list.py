#!/usr/bin/env python
# coding: utf-8
#!! 按照config.csv的对照表,改写地图里面的设定名称,对照表里面是按照顺序的
#!! 同时生成新的UUID,以免重复
#!! 写入新文件 Newmap

""" sample csv file
NO, Name
1, FANGTANG
2, desk1
3, desk2
4, desk3
5, desk4
6, desk5
7, desk6

"""


###
import json
import pandas as pd
import uuid

with open('c:/log/map.json') as f:
    x=json.load(f)
config=pd.read_csv('c:/log/config.csv')


list=config['Name'].tolist()

start=0
for fea in x['features']:
    if start < len(list):
        fea['properties']['name']=list[start]
    start+=1
    fea['properties']['name']=''
    
    fea['id']    =str(uuid.uuid4()).upper()

with open("c:/log/newmap.json", "w") as f:
    json.dump(x,f)
    print("加载入文件newmap.json 完成...")

