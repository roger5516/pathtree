#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
#useage: python3 main.py 2016-04-28
import datetime
import json
import os, time

def get_date():
    delta = datetime.timedelta(days=1)
    today = datetime.datetime.now()
    yestoday = (today - delta).strftime('%Y/%m/%d 00:00')
    yesto = (today - delta).strftime('%Y-%m-%d')
    delta = datetime.timedelta(days=2)
    dby =  (today - delta).strftime('%Y-%m-%d') #the day before yestoday
    delta = datetime.timedelta(days=8)
    sdayago =  (today - delta).strftime('%Y-%m-%d') #7 days before yestoday
    tod = today.strftime('%Y-%m-%d')
    today = today.strftime('%Y/%m/%d  00:00')

    # print(sdayago)

    return today,yestoday,tod,yesto,dby,sdayago

def getCommodityPool():

    # print('getCommodityPool',channel,activeid,activeskuidFile,yestoday,today)
    filepath='/data/roger/a.jsonarray'
    # filepath = '/home/roger/b.jsonarray'

    d = open('/data/roger/test', 'w')
    # d = open('/home/roger/test', 'w')

    with open(filepath ,'r') as ff:
        # print(len(ff.readlines()))
        lines = ff.readlines()
        for line in lines:
            data = json.loads(line)
            # print(len(data))
            # print(line)
            a = []
            for i in data:
                a.append(i["appName"])
            a = list(set(a))
            for i in a :
                d.writelines(i+'\n')

    d.close()


    # if data['message'] =='成功':
    #
    #     channel_skuid =''
    #     if data['data']['skuids'] != []:
    #         for i in data['data']['skuids']:
    #             channel_skuid = channel_skuid + channel + '\t' + str(i) + '\n'
    #
    #         with open(activeskuidFile,'a') as f:
    #             f.write(channel_skuid)
    #
    #     commoditypool = ''
    #     for i in data['data']['poolIds']:
    #         #print(i)
    #         commoditypool = commoditypool + str(i) + ','









if __name__=='__main__':

    starttime = datetime.datetime.now()
    print('Start time:',starttime)

    getCommodityPool()


    endtime = datetime.datetime.now()
    print('Start time:',starttime)
    print('End time:',endtime)



