#!/usr/bin/env python
# coding: utf-8


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

