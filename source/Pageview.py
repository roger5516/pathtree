#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

from datetime import datetime

class Pagemessage(object):


#初始化
    def __init__(self,servertime, fronttime, ip, guid, userid, platformid, appid, platform, biztype, logtype, referurl, curpageurl, pagelevelid, viewid, viewparam, clickid, clickparam, os, display, downchann, appversion, devicetype, nettype, coordinate, hserecomkey, hseextend, hseepread, searchengine, keyword, chansource, search, hours, ten_min, levelid, country, province, city, district):
        self.servertime = servertime
        self.fronttime = fronttime
        self.ip = ip
        self.guid = guid
        self.userid = userid
        self.platform = platform
        self.biztype = biztype
        self.logtype = logtype
        self.referurl = referurl
        self.curpageurl = curpageurl
        self.pagelevelid = pagelevelid
        self.viewid = viewid
        self.viewparam = viewparam
        self.clickid = clickid
        self.clickparam = clickparam
        self.os = os
        self.display = display
        self.downchann = downchann
        self.appversion = appversion
        self.devicetype = devicetype
        self.nettype = nettype
        self.coordinate = coordinate
        self.hserecomkey = hserecomkey
        self.hseextend = hseextend
        self.hseepread = hseepread
        self.searchengine = searchengine
        self.keyword = keyword
        self.chansource = chansource
        self.search = search
        self.hours = hours
        self.ten_min = ten_min
        self.viewtime = 0
        self.referbiz = ''
        self.referpageid = ''
        self.referpagepra = ''
        self.referclickid = ''
        self.referclickpra = ''
        self.treeid = ''
        self.treepvid = -1
        self.treeclickid = -1
        self.levelid = levelid
        self.ordertreeflag = 0
        self.country = country
        self.province = province
        self.city =city
        self.district = district
        self.platformid = platformid
        self.appid = appid
        '''
        if len(self.userid) ==10 and self.userid.isdigit():
            self.isuser = True
        else:
            self.isuser = False
        if self.curpageurl =='':
            self.ish5 =False
        else:
            self.ish5 = True
        '''

#print
    def print_self(self):
        print('%s,%s,%s,%s,%s,%s,%s,%s,%s' %(self.fronttime, self.guid, self.userid, self.platform, self.biztype, self.curpageurl, self.pagelevelid, self.viewid, self.viewparam))


#is true user?
    def isuid(self):
        return self.isuser


#is is page
    def ish5page(self):
        return self.ish5


#select time
    def timestamp2time(self):
        print(datetime.fromtimestamp(int(self.fronttime)/1000))


