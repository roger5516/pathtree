#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
# useage: python3 main.py 2016-04-28
import datetime, sys, os, logging
import hashlib, random
import Pageview
import urllib.request
import json
import linecache
import shutil

log_file = "/data/roger/tmp/search/search.keywords.log"
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s :%(funcName)s %(message)s',
                    datefmt='%a, %Y-%m-%d %H:%M:%S',
                    filename=log_file,
                    filemode='a')

#ip在线解析
def ipprase(ip):
    url='http://ipsearchneibu.haiziwang.com/ipsearch-web/ipDetail/queryIpDetail.do?ip='+ip
    response = urllib.request.urlopen(url)
    data = json.loads(response.read().decode("utf-8"))
    if data['success'] == True:
        country = data['content']['result']['country']
        if country =='香港' or country =='台湾'or country =='澳门':
            country = '中国'
        return country,data['content']['result']['province'],data['content']['result']['city'],data['content']['result']['district']
    else:
        print(ip)
        return '','','',''


# get yestoday's date
def get_date():
    delta = datetime.timedelta(days=1)
    today = datetime.datetime.now()
    yestoday = today - delta
    date = yestoday.strftime('%Y-%m-%d')
    if 2 == len(sys.argv):
        date = sys.argv[1]

    logging.info('处理日期为 ' + date)
    return date

# md5 jiami
def md5(s):
    m = hashlib.md5()
    m.update(s.encode(encoding="utf-8"))
    return m.hexdigest()

# get flow data  which logtype in '10000' '20000' '30000' from hive
def get_pvdata_from_hive(date,trackerfile):
    logging.info("从hive中查数据!")
    os.system('''hive -e"
select servertime, fronttime, ip, guid,  nvl(userid,0) as userid, platformid, appid, platform, biztype, logtype, referurl, curpageurl, pagelevelid, viewid, viewparam, clickid, clickparam, os, display, downchann, appversion, devicetype, nettype, coordinate, hserecomkey, hseextend, hseepread, searchengine, keyword, chansource, search, hours, ten_min, levelid, country, province, city, district from (
	select c.servertime, c.fronttime, c.ip, c.guid,  case when c.userid is null then 0 else c.userid end as userid, c.platform, c.biztype, c.logtype, c.referurl, c.curpageurl, c.pagelevelid, c.viewid, c.viewparam, c.clickid, c.clickparam, c.os, c.display, c.downchann, c.appversion, c.devicetype, c.nettype, c.coordinate, c.hserecomkey, c.hseextend, c.hseepread, c.searchengine, c.keyword, c.chansource, c.search, c.hours, c.ten_min, nvl(d.levelid,5)  as levelid, c.country, c.province, c.city, c.district , c.platformid, c.appid
	from (
		select
		servertime, fronttime, ip, guid, userid, platform, biztype, logtype, referurl, curpageurl, pagelevelid, viewid, viewparam, clickid, clickparam, os, display, downchann, appversion, devicetype, nettype, coordinate, hserecomkey, hseextend, hseepread, searchengine, keyword, chansource, search, hours, ten_min , country, province, city, district, platformid, appid
		from fdm.fdm_tracker_detail_da
		where days='%s'
		and platform not in ('02','03')
		and  logtype ='20000'
		and clickid <>'20004'
		and concat( platformid ,'-',appid  ) <>'1-2'
		and concat( platformid ,'-',biztype  ) <>'1-006'
		and guid ='169b0346-1a74-ba6a-aa5c-488ec8c4af19'
		and fronttime <>''
		and length(userid)>9
		and userid like '1%%'
		union
		select a.servertime, a.fronttime, a.ip, a.guid, b.userid, a.platform, a.biztype, a.logtype, a.referurl, a.curpageurl, a.pagelevelid, a.viewid, a.viewparam, a.clickid, a.clickparam, a.os, a.display, a.downchann, a.appversion, a.devicetype, a.nettype, a.coordinate, a.hserecomKey, a.hseextend, a.hseepread, a.searchengine, a.keyword, a.chansource, a.search, a.hours, a.ten_min, a.country, a.province, a.city, a.district , a.platformid, a.appid
		from (
			select 	servertime, fronttime, ip, guid, platform, biztype, logtype, referurl, curpageurl, pagelevelid, viewid, viewparam, clickid, clickparam, os, display, downchann, appversion, devicetype, nettype, coordinate, hserecomkey,hseextend, hseepread, searchengine, keyword, chansource, search, hours,ten_min , country, province, city, district , platformid, appid
			from fdm.fdm_tracker_detail_da
			where days='%s'
			and platform not in ('02','03')
			and  logtype ='20000'
			and clickid <>'20004'
		    and concat( platformid ,'-',appid  ) <>'1-2'
		    and concat( platformid ,'-',biztype  ) <>'1-006'
			and guid ='169b0346-1a74-ba6a-aa5c-488ec8c4af19'
			and fronttime <>''
			and (userid like '%%==%%' or length(userid) <=9
			)
		) a
		left join
		(
			select guid,max(userid) as userid
			from fdm.fdm_guid_page_pv_ten_min_de
			where days='%s'
			and platform not in ('02','03')
            and concat( platformid ,'-',appid  ) <>'1-2'
	     	and concat( platformid ,'-',biztype  ) <>'1-006'
	     	and guid ='169b0346-1a74-ba6a-aa5c-488ec8c4af19'
			and length(userid)>9
			and userid like '1%%'
			group by guid
		) b
		on a.guid = b.guid
	)c
	left join
	(
		select ad.platformid,ad.pagelevelid ,ad.biztype ,bd.logtype ,ad.viewid ,bd.clickid, bd.levelid from (
			select platformid,pagelevelid ,biztype  ,eventid as viewid
			from bdm.bdm_tracker_protocol where logtype='10000'
		) ad
		join
		(
			select platformid,pagelevelid ,biztype ,logtype ,eventid as clickid,levelid
			from bdm.bdm_tracker_protocol
			where logtype='20000'
		) bd
		on ad.platformid =bd.platformid and  ad.biztype=bd.biztype and ad.pagelevelid=bd.pagelevelid
	)d on d.platformid =c.platformid and  d.pagelevelid = c.pagelevelid and d.biztype =c.biztype and d.logtype=c.logtype and d.clickid = c.clickid
	union all
	select c.servertime, c.fronttime, c.ip, c.guid,  nvl(c.userid, 0) as userid, c.platform, c.biztype, c.logtype, c.referurl, c.curpageurl, c.pagelevelid, c.viewid, c.viewparam, c.clickid, c.clickparam, c.os, c.display, c.downchann, c.appversion, c.devicetype, c.nettype, c.coordinate, c.hserecomkey, c.hseextend, c.hseepread, c.searchengine, c.keyword, c.chansource, c.search, c.hours, c.ten_min, nvl(d.levelid,5)  as levelid, c.country, c.province, c.city, c.district, c.platformid, c.appid
	from (
		select servertime, fronttime, ip, guid, userid, platform, biztype, logtype, referurl, curpageurl, pagelevelid, viewid, viewparam, clickid, clickparam, os, display, downchann, appversion, devicetype, nettype, coordinate, hserecomkey, hseextend, hseepread, searchengine, keyword, chansource, search, hours, ten_min , country, province, city, district , platformid, appid
		from fdm.fdm_tracker_detail_da
		where days='%s'
		and platform not in ('02','03')
		and  logtype ='10000'
		and concat( platformid ,'-',appid  ) <>'1-2'
		and concat( platformid ,'-',biztype  ) <>'1-006'
		and guid ='169b0346-1a74-ba6a-aa5c-488ec8c4af19'
		and fronttime <>''
		and length(userid)>9
		and userid like '1%%'
		union
		select a.servertime, a.fronttime, a.ip, a.guid, b.userid, a.platform, a.biztype, a.logtype, a.referurl, a.curpageurl, a.pagelevelid, a.viewid, a.viewparam, a.clickid, a.clickparam, a.os, a.display, a.downchann, a.appversion, a.devicetype, a.nettype, a.coordinate, a.hserecomKey, a.hseextend, a.hseepread, a.searchengine, a.keyword, a.chansource, a.search, a.hours, a.ten_min, a.country, a.province, a.city, a.district, a.platformid, a.appid
		from (
			select servertime, fronttime, ip, guid, platform, biztype, logtype, referurl, curpageurl, pagelevelid, viewid, viewparam, clickid, clickparam, os, display, downchann, appversion, devicetype, nettype, coordinate, hserecomkey, hseextend, hseepread, searchengine, keyword, chansource,search,hours, ten_min , country, province, city, district , platformid, appid
			from fdm.fdm_tracker_detail_da
			where days='%s'
			and platform not in ('02','03')
			and  logtype ='10000'
			and concat( platformid ,'-',appid  ) <>'1-2'
		    and concat( platformid ,'-',biztype  ) <>'1-006'
			and guid ='169b0346-1a74-ba6a-aa5c-488ec8c4af19'
			and fronttime <>''
			and (userid like '%%==%%' or length(userid) <=9 )
		) a
		left join
		(
			select guid,max(userid) as userid
			from fdm.fdm_guid_page_pv_ten_min_de
			where days='%s'
			and platform not in ('02','03')
			and concat( platformid ,'-',appid  ) <>'1-2'
			and concat( platformid ,'-',biztype  ) <>'1-006'
			and guid ='169b0346-1a74-ba6a-aa5c-488ec8c4af19'
			and length(userid)>9
			and userid like '1%%'
			group by guid
		) b
		on a.guid = b.guid
	)c
	left join (
		select platformid,pagelevelid ,biztype ,logtype, eventid ,levelid
		from bdm.bdm_tracker_protocol
		where logtype='10000'
	)d on d.platformid =c.platformid and d.pagelevelid = c.pagelevelid and d.biztype =c.biztype and d.logtype=c.logtype
	-- and d.eventid = c.viewid
)aaaa order by platformid,appid,guid,userid,fronttime
">%s''' % (date, date, date, date, date, date,trackerfile))
    count = len(open(trackerfile, 'rU').readlines())
    logging.info('hive found  %d  条数据' % (count))

"""
split the pv flow
"""
def split_bigfile_to_smlallfile(trackerfile,splitpath):
    """
    分割成若干个200W行的小文件
    :return:
    """
    if os.path.exists(splitpath):
        shutil.rmtree(splitpath)
    os.mkdir(splitpath)

    basenumber = 1000000
    rownumber = 1000000

    lines = []
    count = len(open(trackerfile, 'rU').readlines())

    while basenumber < count :
        guid = linecache.getline(trackerfile, basenumber).strip('\n').split('\t')[3]
        basenumber = basenumber + 1

        while  (len(linecache.getline(trackerfile, basenumber)) !=0)&(linecache.getline(trackerfile, basenumber).strip('\n').split('\t')[3] == guid)  :
            basenumber = basenumber + 1
            if basenumber == count:
                break
        print(basenumber)
        lines.append(basenumber)
        basenumber = basenumber + rownumber

    if basenumber <  count + rownumber:
        lines.append(count+1)
    else:
        lines.pop()
        lines.append(count+1)

    with open(trackerfile) as f : # 使用with读文件
        filecount = len(lines)
        i=0
        aaa = lines[i]
        content = ''
        filelinenum = 0
        for line in f:
            filelinenum= filelinenum+1
            if filelinenum < aaa:
                content = content + line
                if count == filelinenum:
                    with open(splitpath+'20_'+"%s" %i,'w') as s:
                        s.writelines( content)
            else:
                with open(splitpath+'20_'+"%s" %i,'w') as s:
                    s.writelines( content)

                i = i + 1
                content = ''
                aaa = lines[i]
                content = content + line

# get the data from file
def get_data_from_file(filename):
    logging.info('从文件导入数据')
    data_all = []
    with open(filename, "rt")  as handle:
        for a in [ln.strip('\n').split('\t') for ln in handle]:
            if len(a) <5:
                print(a)
            else:
                data_all.append(a)
    logging.info('导入数据成功')
    data_all.sort(key=lambda x: (x[5], x[6], x[3], x[4], x[1]))
    logging.info('排序 ok')
    return data_all

# spilt data
def spilttxt(data, date, resultpath):
    logging.info('分割数据')
    i = 0
    while len(data) > 10000:
        g = data[10000][3]
        j = 1
        lendata = len(data)
        while (10000 + j)<lendata and data[10000 + j][3] == g:
            j = j + 1

        locals()['data' + str(i)] = data[0:10000 + j]
        del data[0:10000 + j]
        i = i + 1

    locals()['data' + str(i)] = data
    logging.info('共分割 %d 份数据' %(i))
    for ii in range(i + 1):
        modd(ii, locals()['data' + str(ii)], date,resultpath)

    logging.info('这次的路径树计算完成了--'+resultpath)

def putdatatohdfs(date,resultpath):
    #logging.info("开始把路径结果文件导入到hdfs")
    os.system('''
    hadoop fs -put %s /apps/hive/warehouse/fdm/fdm_tracker_pathtree_result_de/%s/
    ''' % (resultpath,date))
    logging.info('导入 %s 完成' %resultpath)
    #logging.info("把路径结果文件关联外部表")


def webpage(data,resultpath):
    print('--webpage')
    refertime = 0
    referbiz = ''
    referpageid = ''
    referpagepra = ''
    referclickid = ''
    referclickpra = ''
    treeid = md5(data[0][3] + str(random.random()))
    treepvid = 0
    treeclickid = -1
    referurl = ''
    i = 0
    pvlist = [] #pv的i列表
    for x in data:
        locals()['cur' + str(i)] = Pageview.Pagemessage(*x)
        curr = locals()['cur'+str(i)]
        # curr = Pageview.Pagemessage(*x)
        # 第一次访问
        if i == 0 and curr.logtype == '10000':
            refertime = curr.fronttime
            #referbiz = curr.biztype
            #referpageid = curr.pagelevelid
            #referpagepra = curr.viewparam
            #referurl = curr.curpageurl
            curr.treeid = treeid
            curr.treepvid =treepvid
            pvlist.append(i)
            i = i + 1
        # 第一条访问数据是不是pv
        elif i == 0 and curr.logtype != '10000':
            #print('aaa')
            pass
        # 后续的pv
        elif i > 0 and curr.logtype == '10000':
            if ((int(curr.fronttime) - int(refertime)) > 1800000) or ( (locals()['cur' + str(i-1)].userid != curr.userid) and (locals()['cur' + str(i-1)].userid !=0)  ):
                """
                 两次访问超过30分钟
                       1,写入文件
                       2,pvlist,i 清空
                       3,重新计数
                """

                with open(resultpath,'a') as f:
                    for mes in  range(i):
                        if locals()['cur' + str(mes)].country =='':
                            locals()['cur' + str(mes)].country, locals()['cur' + str(mes)].province, locals()['cur' + str(mes)].city, locals()['cur' + str(mes)].district = ipprase(locals()['cur' + str(mes)].ip)
                        if locals()['cur' + str(mes)].country =='香港' or locals()['cur' + str(mes)].country =='台湾'or locals()['cur' + str(mes)].country =='澳门':
                            locals()['cur' + str(mes)].country = '中国'
                        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %(locals()['cur' + str(mes)].servertime,
                                                                                                                                                                                                   locals()['cur' + str(mes)].fronttime,
                                                                                                                                                                                                   locals()['cur' + str(mes)].ip,
                                                                                                                                                                                                   locals()['cur' + str(mes)].guid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].userid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].platformid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].appid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].platform,
                                                                                                                                                                                                   locals()['cur' + str(mes)].treeid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].treepvid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].treeclickid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].biztype,
                                                                                                                                                                                                   locals()['cur' + str(mes)].logtype,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referurl,
                                                                                                                                                                                                   locals()['cur' + str(mes)].curpageurl,
                                                                                                                                                                                                   locals()['cur' + str(mes)].pagelevelid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].viewid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].viewparam,
                                                                                                                                                                                                   locals()['cur' + str(mes)].clickid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].clickparam,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referbiz,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referpageid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referpagepra,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referclickid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referclickpra,
                                                                                                                                                                                                   locals()['cur' + str(mes)].viewtime /1000,
                                                                                                                                                                                                   locals()['cur' + str(mes)].levelid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].country,
                                                                                                                                                                                                   locals()['cur' + str(mes)].province,
                                                                                                                                                                                                   locals()['cur' + str(mes)].city,
                                                                                                                                                                                                   locals()['cur' + str(mes)].district,
                                                                                                                                                                                                   locals()['cur' + str(mes)].os,
                                                                                                                                                                                                   locals()['cur' + str(mes)].display,
                                                                                                                                                                                                   locals()['cur' + str(mes)].downchann,
                                                                                                                                                                                                   locals()['cur' + str(mes)].appversion,
                                                                                                                                                                                                   locals()['cur' + str(mes)].devicetype,
                                                                                                                                                                                                   locals()['cur' + str(mes)].nettype,
                                                                                                                                                                                                   locals()['cur' + str(mes)].coordinate,
                                                                                                                                                                                                   locals()['cur' + str(mes)].hserecomkey,
                                                                                                                                                                                                   locals()['cur' + str(mes)].hseextend,
                                                                                                                                                                                                   locals()['cur' + str(mes)].hseepread,
                                                                                                                                                                                                   locals()['cur' + str(mes)].searchengine,
                                                                                                                                                                                                   locals()['cur' + str(mes)].keyword,
                                                                                                                                                                                                   locals()['cur' + str(mes)].chansource,
                                                                                                                                                                                                   locals()['cur' + str(mes)].search,
                                                                                                                                                                                                   locals()['cur' + str(mes)].hours,
                                                                                                                                                                                                   locals()['cur' + str(mes)].ten_min
                                                                                                                                                                                                   ))
                    locals()['cur' + str(mes)] = None
                locals()['cur' + str(i)] = None
                i = 0
                locals()['cur' + str(i)] = Pageview.Pagemessage(*x)
                curr = locals()['cur'+str(i)]
                pvlist = []
                pvlist.append(i)
                referclickid = ''
                referclickpra = ''
                treeid = md5(data[0][3] + str(random.random()))
                treepvid = 0
                treeclickid = -1
                refertime = curr.fronttime
                referbiz = ''
                referpageid = ''
                referpagepra = ''
                referurl = ''
                curr.treeid = treeid
                curr.treepvid =treepvid
                i = i + 1

            else:
                # 后续访问
                """
                #以后补充
                # 1,referurl 对的上 2,如果来源为空,则强挂
                if referurl == curr.referurl or curr.referurl =='':
                """
                if 1 == 1:
                    treepvid = treepvid + 1
                    """
                     按顺序排出访问顺序
                    """
                    #refertime = locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime
                    referbiz = locals()['cur' + str(pvlist[len(pvlist)-1])].biztype
                    referpageid = locals()['cur' + str(pvlist[len(pvlist)-1])].pagelevelid
                    referpagepra = locals()['cur' + str(pvlist[len(pvlist)-1])].viewparam
                    referurl = locals()['cur' + str(pvlist[len(pvlist)-1])].curpageurl
                    curr.referbiz = referbiz
                    curr.referpageid = referpageid
                    curr.referpagepra = referpagepra
                    curr.treeid = treeid
                    curr.treepvid = treepvid
                    refertime = curr.fronttime
                    #上一个页面的访问时长
                    if locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime  == 0:
                        time_tmp = int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime)
                        if time_tmp <1800000:
                            locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = time_tmp
                        else:
                            locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = 0
                    #判断上一条记录是pv还是click
                    if locals()['cur' + str(i-1)].logtype == '10000':
                        referclickid = ''
                        referclickpra = ''
                    elif i>0 and locals()['cur' + str(i-1)].logtype == '20000':
                        referclickid = locals()['cur' + str(i-1)].clickid
                        referclickpra = locals()['cur' + str(i-1)].clickparam
                    elif i>0 and locals()['cur' + str(i-1)].logtype == '30000':
                        if i>1 and  locals()['cur' + str(i-2)].logtype == '10000':
                            referclickid = ''
                            referclickpra = ''
                        elif i>1 and  locals()['cur' + str(i-2)].logtype == '20000':
                            referclickid = locals()['cur' + str(i-2)].clickid
                            referclickpra = locals()['cur' + str(i-2)].clickparam
                        else :
                            referclickid = ''
                            referclickpra = ''
                    else:
                        pass
                    curr.referclickid = referclickid
                    curr.referclickpra = referclickpra
                    treeclickid = 0
                    pvlist.append(i)
                    # treepvid = treepvid + 1
                    i = i + 1
                """
                #以后补充，打开多网页的情况下，多路径
                # referurl 对不上,在之前的记录中找,从后往前找
                else :
                    foundflag = False
                    foundpvi = -1
                    for pvi in pvlist[::-1]:
                        if locals()['cur' + str(pvi)].referurl == curr.referurl:
                            foundflag = True
                            foundpvi = pvi
                            break
                    # 找到了，新的路径
                    if foundflag == True:
                        pass
                    #没找到，强挂
                    else:
                        pass
                """
        # click事件
        elif (i > 0) and (curr.logtype == '20000') and (( (locals()['cur' + str(i-1)].userid == curr.userid) and (locals()['cur' + str(i-1)].userid !=0)  ) or (locals()['cur' + str(i-1)].userid ==0) ):
            if int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime) >=1800000:
                pass
            elif curr.pagelevelid != locals()['cur' + str(pvlist[len(pvlist)-1])].pagelevelid:
                refertime = curr.fronttime
                curr.referbiz = referbiz
                curr.referpageid = referpageid
                curr.referpagepra = referpagepra
                curr.referclickid = referclickid
                curr.referclickpra = referclickpra
                curr.treeid = treeid
                curr.treepvid = treepvid
                curr.treeclickid = treeclickid
                treeclickid = treeclickid + 1
                #还需要加上对20004 事件处理
                if curr.clickid == '20004':
                    time_tmp = int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime)
                    if time_tmp < 1800000:
                        locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = time_tmp
                    else :
                        locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = 0
                treeclickid = treeclickid + 1
                i = i + 1
        # 跳转事件
        # elif (i > 0) and (curr.logtype == '30000') and (( (locals()['cur' + str(i-1)].userid == curr.userid) and (locals()['cur' + str(i-1)].userid !=0)  ) or (locals()['cur' + str(i-1)].userid ==0) ):
        #     if int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime) >=1800000:
        #         pass
        #     elif (locals()['cur' + str(pvlist[len(pvlist)-1])].biztype == curr.biztype and locals()['cur' + str(pvlist[len(pvlist)-1])].pagelevelid == curr.pagelevelid ) or curr.pagelevelid =='150101':
        #         #访问时长
        #         time_tmp = int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime)
        #         if time_tmp < 1800000:
        #             locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = time_tmp
        #         else :
        #             locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = 0
        #         curr.referbiz = referbiz
        #         curr.referpageid = referpageid
        #         curr.referpagepra = referpagepra
        #         curr.referclickid = referclickid
        #         curr.referclickpra = referclickpra
        #         curr.treeid = treeid
        #         curr.treepvid = treepvid
        #         refertime = curr.fronttime
        #         i = i + 1
        #     # 其他情况，抛弃处理
        #     else:
        #         pass
        #         #print('ccc   ',i,'    ',curr.logtype,'    ',curr.guid ,'    ', curr.fronttime)

    with open(resultpath,'a') as f:
        for mes in  range(i):
            if locals()['cur' + str(mes)].country =='':
                locals()['cur' + str(mes)].country, locals()['cur' + str(mes)].province, locals()['cur' + str(mes)].city, locals()['cur' + str(mes)].district = ipprase(locals()['cur' + str(mes)].ip)
            if locals()['cur' + str(mes)].country =='香港' or locals()['cur' + str(mes)].country =='台湾'or locals()['cur' + str(mes)].country =='澳门':
                locals()['cur' + str(mes)].country = '中国'
            f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %(locals()['cur' + str(mes)].servertime,
                                                                                                                                                                                       locals()['cur' + str(mes)].fronttime,
                                                                                                                                                                                       locals()['cur' + str(mes)].ip,
                                                                                                                                                                                       locals()['cur' + str(mes)].guid,
                                                                                                                                                                                       locals()['cur' + str(mes)].userid,
                                                                                                                                                                                       locals()['cur' + str(mes)].platformid,
                                                                                                                                                                                       locals()['cur' + str(mes)].appid,
                                                                                                                                                                                       locals()['cur' + str(mes)].platform,
                                                                                                                                                                                       locals()['cur' + str(mes)].treeid,
                                                                                                                                                                                       locals()['cur' + str(mes)].treepvid,
                                                                                                                                                                                       locals()['cur' + str(mes)].treeclickid,
                                                                                                                                                                                       locals()['cur' + str(mes)].biztype,
                                                                                                                                                                                       locals()['cur' + str(mes)].logtype,
                                                                                                                                                                                       locals()['cur' + str(mes)].referurl,
                                                                                                                                                                                       locals()['cur' + str(mes)].curpageurl,
                                                                                                                                                                                       locals()['cur' + str(mes)].pagelevelid,
                                                                                                                                                                                       locals()['cur' + str(mes)].viewid,
                                                                                                                                                                                       locals()['cur' + str(mes)].viewparam,
                                                                                                                                                                                       locals()['cur' + str(mes)].clickid,
                                                                                                                                                                                       locals()['cur' + str(mes)].clickparam,
                                                                                                                                                                                       locals()['cur' + str(mes)].referbiz,
                                                                                                                                                                                       locals()['cur' + str(mes)].referpageid,
                                                                                                                                                                                       locals()['cur' + str(mes)].referpagepra,
                                                                                                                                                                                       locals()['cur' + str(mes)].referclickid,
                                                                                                                                                                                       locals()['cur' + str(mes)].referclickpra,
                                                                                                                                                                                       locals()['cur' + str(mes)].viewtime /1000,
                                                                                                                                                                                       locals()['cur' + str(mes)].levelid,
                                                                                                                                                                                       locals()['cur' + str(mes)].country,
                                                                                                                                                                                       locals()['cur' + str(mes)].province,
                                                                                                                                                                                       locals()['cur' + str(mes)].city,
                                                                                                                                                                                       locals()['cur' + str(mes)].district,
                                                                                                                                                                                       locals()['cur' + str(mes)].os,
                                                                                                                                                                                       locals()['cur' + str(mes)].display,
                                                                                                                                                                                       locals()['cur' + str(mes)].downchann,
                                                                                                                                                                                       locals()['cur' + str(mes)].appversion,
                                                                                                                                                                                       locals()['cur' + str(mes)].devicetype,
                                                                                                                                                                                       locals()['cur' + str(mes)].nettype,
                                                                                                                                                                                       locals()['cur' + str(mes)].coordinate,
                                                                                                                                                                                       locals()['cur' + str(mes)].hserecomkey,
                                                                                                                                                                                       locals()['cur' + str(mes)].hseextend,
                                                                                                                                                                                       locals()['cur' + str(mes)].hseepread,
                                                                                                                                                                                       locals()['cur' + str(mes)].searchengine,
                                                                                                                                                                                       locals()['cur' + str(mes)].keyword,
                                                                                                                                                                                       locals()['cur' + str(mes)].chansource,
                                                                                                                                                                                       locals()['cur' + str(mes)].search,
                                                                                                                                                                                       locals()['cur' + str(mes)].hours,
                                                                                                                                                                                       locals()['cur' + str(mes)].ten_min
                                                                                                                                                                                                             ))
            locals()['cur' + str(mes)] = None


def apppagee(data,resultpath):
    print('--apppage')
    refertime = 0
    referbiz = ''
    referpageid = ''
    referpagepra = ''
    referclickid = ''
    referclickpra = ''
    treeid = md5(data[0][3] + str(random.random()))
    treepvid = 0
    treeclickid = -1
    referurl = ''
    i = 0
    pvlist = [] #pv的i列表
    for x in data:
        locals()['cur' + str(i)] = Pageview.Pagemessage(*x)
        curr = locals()['cur'+str(i)]
        # curr = Pageview.Pagemessage(*x)
        # 第一次访问
        if i == 0 and curr.logtype == '10000':
            refertime = curr.fronttime
            #referbiz = curr.biztype
            #referpageid = curr.pagelevelid
            #referpagepra = curr.viewparam
            #referurl = curr.curpageurl
            curr.treeid = treeid
            curr.treepvid =treepvid
            pvlist.append(i)
            i = i + 1
        # 第一条访问数据是不是pv
        elif i == 0 and curr.logtype == '20000' and curr.platformid == '1' and curr.biztype =='005' and curr.pagelevelid =='030101' and curr.clickid =='20006':
            curr.treeid = treeid
            curr.treepvid =treepvid
            pvlist.append(i)
            curr.clickid = 1
            i = i + 1
            #print('aaa   ',i,'    ',curr.logtype,'    ',curr.guid ,'    ', curr.fronttime)

        # 后续的pv
        elif i > 0 and curr.logtype == '10000':
            if ((int(curr.fronttime) - int(refertime)) > 1800000) or ( (locals()['cur' + str(i-1)].userid != curr.userid) and (locals()['cur' + str(i-1)].userid !=0)  ):
                """
                 两次访问超过30分钟
                       1,写入文件
                       2,pvlist,i 清空
                       3,重新计数
                """

                with open(resultpath,'a') as f:
                    for mes in  range(i):
                        if locals()['cur' + str(mes)].country =='':
                            locals()['cur' + str(mes)].country, locals()['cur' + str(mes)].province, locals()['cur' + str(mes)].city, locals()['cur' + str(mes)].district = ipprase(locals()['cur' + str(mes)].ip)
                        if locals()['cur' + str(mes)].country =='香港' or locals()['cur' + str(mes)].country =='台湾'or locals()['cur' + str(mes)].country =='澳门':
                            locals()['cur' + str(mes)].country = '中国'
                        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %(locals()['cur' + str(mes)].servertime,
                                                                                                                                                                                                   locals()['cur' + str(mes)].fronttime,
                                                                                                                                                                                                   locals()['cur' + str(mes)].ip,
                                                                                                                                                                                                   locals()['cur' + str(mes)].guid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].userid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].platformid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].appid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].platform,
                                                                                                                                                                                                   locals()['cur' + str(mes)].treeid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].treepvid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].treeclickid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].biztype,
                                                                                                                                                                                                   locals()['cur' + str(mes)].logtype,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referurl,
                                                                                                                                                                                                   locals()['cur' + str(mes)].curpageurl,
                                                                                                                                                                                                   locals()['cur' + str(mes)].pagelevelid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].viewid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].viewparam,
                                                                                                                                                                                                   locals()['cur' + str(mes)].clickid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].clickparam,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referbiz,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referpageid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referpagepra,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referclickid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].referclickpra,
                                                                                                                                                                                                   locals()['cur' + str(mes)].viewtime /1000,
                                                                                                                                                                                                   locals()['cur' + str(mes)].levelid,
                                                                                                                                                                                                   locals()['cur' + str(mes)].country,
                                                                                                                                                                                                   locals()['cur' + str(mes)].province,
                                                                                                                                                                                                   locals()['cur' + str(mes)].city,
                                                                                                                                                                                                   locals()['cur' + str(mes)].district,
                                                                                                                                                                                                   locals()['cur' + str(mes)].os,
                                                                                                                                                                                                   locals()['cur' + str(mes)].display,
                                                                                                                                                                                                   locals()['cur' + str(mes)].downchann,
                                                                                                                                                                                                   locals()['cur' + str(mes)].appversion,
                                                                                                                                                                                                   locals()['cur' + str(mes)].devicetype,
                                                                                                                                                                                                   locals()['cur' + str(mes)].nettype,
                                                                                                                                                                                                   locals()['cur' + str(mes)].coordinate,
                                                                                                                                                                                                   locals()['cur' + str(mes)].hserecomkey,
                                                                                                                                                                                                   locals()['cur' + str(mes)].hseextend,
                                                                                                                                                                                                   locals()['cur' + str(mes)].hseepread,
                                                                                                                                                                                                   locals()['cur' + str(mes)].searchengine,
                                                                                                                                                                                                   locals()['cur' + str(mes)].keyword,
                                                                                                                                                                                                   locals()['cur' + str(mes)].chansource,
                                                                                                                                                                                                   locals()['cur' + str(mes)].search,
                                                                                                                                                                                                   locals()['cur' + str(mes)].hours,
                                                                                                                                                                                                   locals()['cur' + str(mes)].ten_min
                                                                                                                                                                                                                                 ))
                    locals()['cur' + str(mes)] = None
                locals()['cur' + str(i)] = None
                i = 0
                locals()['cur' + str(i)] = Pageview.Pagemessage(*x)
                curr = locals()['cur'+str(i)]
                pvlist = []
                pvlist.append(i)
                referclickid = ''
                referclickpra = ''
                treeid = md5(data[0][3] + str(random.random()))
                treepvid = 0
                treeclickid = -1
                refertime = curr.fronttime
                referbiz = ''
                referpageid = ''
                referpagepra = ''
                referurl = ''
                curr.treeid = treeid
                curr.treepvid =treepvid
                i = i + 1

            else:
                # 后续访问
                """
                #以后补充
                # 1,referurl 对的上 2,如果来源为空,则强挂
                if referurl == curr.referurl or curr.referurl =='':
                """
                if 1 == 1:
                    treepvid = treepvid + 1
                    """
                     按顺序排出访问顺序
                    """
                    #refertime = locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime
                    referbiz = locals()['cur' + str(pvlist[len(pvlist)-1])].biztype
                    referpageid = locals()['cur' + str(pvlist[len(pvlist)-1])].pagelevelid
                    referpagepra = locals()['cur' + str(pvlist[len(pvlist)-1])].viewparam
                    referurl = locals()['cur' + str(pvlist[len(pvlist)-1])].curpageurl
                    curr.referbiz = referbiz
                    curr.referpageid = referpageid
                    curr.referpagepra = referpagepra
                    curr.treeid = treeid
                    curr.treepvid = treepvid
                    refertime = curr.fronttime
                    #上一个页面的访问时长
                    if locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime  == 0:
                        time_tmp = int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime)
                        if time_tmp <1800000:
                            locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = time_tmp
                        else:
                            locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = 0

                    #判断上一条记录是pv还是click
                    if locals()['cur' + str(i-1)].logtype == '10000':
                        referclickid = ''
                        referclickpra = ''
                    elif i>0 and locals()['cur' + str(i-1)].logtype == '20000':
                        referclickid = locals()['cur' + str(i-1)].clickid
                        referclickpra = locals()['cur' + str(i-1)].clickparam
                    elif i>1 and locals()['cur' + str(i-1)].logtype == '30000':
                        if locals()['cur' + str(i-2)].logtype == '10000':
                            referclickid = ''
                            referclickpra = ''
                        elif locals()['cur' + str(i-2)].logtype == '20000':
                            referclickid = locals()['cur' + str(i-2)].clickid
                            referclickpra = locals()['cur' + str(i-2)].clickparam
                        else :
                            referclickid = ''
                            referclickpra = ''
                            #pass
                            #print('bbb   ',i,'    ', locals()['cur' + str(i-2)].logtype,'    ', locals()['cur' + str(i-2)].guid ,'    ',  locals()['cur' + str(i-2)].fronttime)
                    else:
                        pass
                    curr.referclickid = referclickid
                    curr.referclickpra = referclickpra
                    treeclickid = 0
                    pvlist.append(i)
                    i = i + 1
        # 处理push事件
        elif i > 0 and curr.logtype == '20000' and curr.platformid == '1' and curr.biztype == '005' and curr.pagelevelid == '030101' and curr.clickid == '20006':
            treepvid = treepvid + 1
            """
             按顺序排出访问顺序
            """
            # refertime = locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime
            referbiz = locals()['cur' + str(pvlist[len(pvlist) - 1])].biztype
            referpageid = locals()['cur' + str(pvlist[len(pvlist) - 1])].pagelevelid
            referpagepra = locals()['cur' + str(pvlist[len(pvlist) - 1])].viewparam
            referurl = locals()['cur' + str(pvlist[len(pvlist) - 1])].curpageurl
            curr.referbiz = referbiz
            curr.referpageid = referpageid
            curr.referpagepra = referpagepra
            curr.treeid = treeid
            curr.treepvid = treepvid
            refertime = curr.fronttime
            # 上一个页面的访问时长
            if locals()['cur' + str(pvlist[len(pvlist) - 1])].viewtime == 0:
                time_tmp = int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist) - 1])].fronttime)
                if time_tmp < 1800000:
                    locals()['cur' + str(pvlist[len(pvlist) - 1])].viewtime = time_tmp
                else:
                    locals()['cur' + str(pvlist[len(pvlist) - 1])].viewtime = 0

            # 判断上一条记录是pv还是click
            if locals()['cur' + str(i - 1)].logtype == '10000':
                referclickid = ''
                referclickpra = ''
            elif i > 0 and locals()['cur' + str(i - 1)].logtype == '20000':
                referclickid = locals()['cur' + str(i - 1)].clickid
                referclickpra = locals()['cur' + str(i - 1)].clickparam
            # elif i > 1 and locals()['cur' + str(i - 1)].logtype == '30000':
            #     if locals()['cur' + str(i - 2)].logtype == '10000':
            #         referclickid = ''
            #         referclickpra = ''
            #     elif locals()['cur' + str(i - 2)].logtype == '20000':
            #         referclickid = locals()['cur' + str(i - 2)].clickid
            #         referclickpra = locals()['cur' + str(i - 2)].clickparam
            #     else:
            #         referclickid = ''
            #         referclickpra = ''
                    # pass
                    # print('bbb   ',i,'    ', locals()['cur' + str(i-2)].logtype,'    ', locals()['cur' + str(i-2)].guid ,'    ',  locals()['cur' + str(i-2)].fronttime)
            else:
                pass
            curr.referclickid = referclickid
            curr.referclickpra = referclickpra
            treeclickid = 0
            curr.treeclickid = treeclickid
            treeclickid = treeclickid + 1
            pvlist.append(i)
            i = i + 1

        # click事件
        elif (i > 0) and (curr.logtype == '20000') and (( (locals()['cur' + str(i-1)].userid == curr.userid) and (locals()['cur' + str(i-1)].userid !=0)  ) or (locals()['cur' + str(i-1)].userid ==0) ):
            if int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime) >= 1800000:
                pass
            #本页面点击事件
            elif curr.pagelevelid == locals()['cur' + str(pvlist[len(pvlist)-1])].pagelevelid:
                curr.referbiz = referbiz
                curr.referpageid = referpageid
                curr.referpagepra = referpagepra
                curr.referclickid = referclickid
                curr.referclickpra = referclickpra
                curr.treeid = treeid
                curr.treepvid = treepvid
                curr.treeclickid = treeclickid
                refertime = curr.fronttime
                #还需要加上对20004 事件处理
                if curr.clickid == '20004':
                    time_tmp = int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime)
                    if time_tmp < 1800000:
                        locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = time_tmp
                    else:
                        locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = 0
                treeclickid = treeclickid + 1
                i = i + 1




        # 跳转事件
        # elif (i > 0) and (curr.logtype == '30000') and (( (locals()['cur' + str(i-1)].userid == curr.userid) and (locals()['cur' + str(i-1)].userid !=0)  ) or (locals()['cur' + str(i-1)].userid ==0) ):
        #     #如果上一条是跳转事件，则抛弃
        #     if locals()['cur' + str(i-1)].logtype == '30000':
        #        pass
        #     elif int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime) >=1800000:
        #         pass
        #     elif ( locals()['cur' + str(pvlist[len(pvlist)-1])].biztype == curr.biztype and locals()['cur' + str(pvlist[len(pvlist)-1])].pagelevelid == curr.pagelevelid and  locals()['cur' + str(pvlist[len(pvlist)-1])].viewparam == curr.viewparam)  or curr.pagelevelid =='150101' :
        #         #访问时长
        #         time_tmp = int(curr.fronttime) - int(locals()['cur' + str(pvlist[len(pvlist)-1])].fronttime)
        #         if time_tmp <1800000:
        #             locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = time_tmp
        #         else:
        #             locals()['cur' + str(pvlist[len(pvlist)-1])].viewtime = 0
        #         curr.referbiz = referbiz
        #         curr.referpageid = referpageid
        #         curr.referpagepra = referpagepra
        #         curr.referclickid = referclickid
        #         curr.referclickpra = referclickpra
        #         curr.treeid = treeid
        #         curr.treepvid = treepvid
        #         refertime = curr.fronttime
        #         i = i + 1
        #     else:
        #         pass #不符合条件的30000事件，抛弃掉
        #         #print('ccc   ',i,'    ',curr.logtype,'    ',curr.guid ,'    ', curr.fronttime)

    with open(resultpath,'a') as f:
        for mes in  range(i):
            if locals()['cur' + str(mes)].country =='':
                locals()['cur' + str(mes)].country, locals()['cur' + str(mes)].province, locals()['cur' + str(mes)].city, locals()['cur' + str(mes)].district = ipprase(locals()['cur' + str(mes)].ip)
            if locals()['cur' + str(mes)].country =='香港' or locals()['cur' + str(mes)].country =='台湾'or locals()['cur' + str(mes)].country =='澳门':
                locals()['cur' + str(mes)].country = '中国'
            f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %(
                                                                                                                                                                                       locals()['cur' + str(mes)].servertime,
                                                                                                                                                                                       locals()['cur' + str(mes)].fronttime,
                                                                                                                                                                                       locals()['cur' + str(mes)].ip,
                                                                                                                                                                                       locals()['cur' + str(mes)].guid,
                                                                                                                                                                                       locals()['cur' + str(mes)].userid,
                                                                                                                                                                                       locals()['cur' + str(mes)].platformid,
                                                                                                                                                                                       locals()['cur' + str(mes)].appid,
                                                                                                                                                                                       locals()['cur' + str(mes)].platform,
                                                                                                                                                                                       locals()['cur' + str(mes)].treeid,
                                                                                                                                                                                       locals()['cur' + str(mes)].treepvid,
                                                                                                                                                                                       locals()['cur' + str(mes)].treeclickid,
                                                                                                                                                                                       locals()['cur' + str(mes)].biztype,
                                                                                                                                                                                       locals()['cur' + str(mes)].logtype,
                                                                                                                                                                                       locals()['cur' + str(mes)].referurl,
                                                                                                                                                                                       locals()['cur' + str(mes)].curpageurl,
                                                                                                                                                                                       locals()['cur' + str(mes)].pagelevelid,
                                                                                                                                                                                       locals()['cur' + str(mes)].viewid,
                                                                                                                                                                                       locals()['cur' + str(mes)].viewparam,
                                                                                                                                                                                       locals()['cur' + str(mes)].clickid,
                                                                                                                                                                                       locals()['cur' + str(mes)].clickparam,
                                                                                                                                                                                       locals()['cur' + str(mes)].referbiz,
                                                                                                                                                                                       locals()['cur' + str(mes)].referpageid,
                                                                                                                                                                                       locals()['cur' + str(mes)].referpagepra,
                                                                                                                                                                                       locals()['cur' + str(mes)].referclickid,
                                                                                                                                                                                       locals()['cur' + str(mes)].referclickpra,
                                                                                                                                                                                       locals()['cur' + str(mes)].viewtime /1000,
                                                                                                                                                                                       locals()['cur' + str(mes)].levelid,
                                                                                                                                                                                       locals()['cur' + str(mes)].country,
                                                                                                                                                                                       locals()['cur' + str(mes)].province,
                                                                                                                                                                                       locals()['cur' + str(mes)].city,
                                                                                                                                                                                       locals()['cur' + str(mes)].district,
                                                                                                                                                                                       locals()['cur' + str(mes)].os,
                                                                                                                                                                                       locals()['cur' + str(mes)].display,
                                                                                                                                                                                       locals()['cur' + str(mes)].downchann,
                                                                                                                                                                                       locals()['cur' + str(mes)].appversion,
                                                                                                                                                                                       locals()['cur' + str(mes)].devicetype,
                                                                                                                                                                                       locals()['cur' + str(mes)].nettype,
                                                                                                                                                                                       locals()['cur' + str(mes)].coordinate,
                                                                                                                                                                                       locals()['cur' + str(mes)].hserecomkey,
                                                                                                                                                                                       locals()['cur' + str(mes)].hseextend,
                                                                                                                                                                                       locals()['cur' + str(mes)].hseepread,
                                                                                                                                                                                       locals()['cur' + str(mes)].searchengine,
                                                                                                                                                                                       locals()['cur' + str(mes)].keyword,
                                                                                                                                                                                       locals()['cur' + str(mes)].chansource,
                                                                                                                                                                                       locals()['cur' + str(mes)].search,
                                                                                                                                                                                       locals()['cur' + str(mes)].hours,
                                                                                                                                                                                       locals()['cur' + str(mes)].ten_min
                                                                                                                                                                                                             ))
            locals()['cur' + str(mes)] = None

def modd(i, data, date,resultpath):
    # 获取隔天的时间戳
    # yesa=str(datetime.datetime.strptime(date, '%Y-%m-%d').timestamp()*1000-2000)
    # tod=str(datetime.datetime.strptime(date, '%Y-%m-%d').timestamp()*1000+86405000)
    # print(yesa)
    logging.info('开始计算第 %d 份数据' %(i))
    guid = [i[3] for i in data]
    guid = list(set(guid))
    plah = ['01', '04', '05']
    plaa = ['03', '02']
    for g in guid:
        same_guid = None
        same_guid = [a for a in data if a[3] == g]
        if len(same_guid) == 0:
            pass
        elif plaa.count(same_guid[0][7]) == 1:
            apppagee(same_guid,resultpath)
        elif plah.count(same_guid[0][7]) == 1:
            webpage(same_guid,resultpath)
        else:
            pass
    del guid
    del data
    #logging.info('第 %d 份数据 计算好了' %(i))

# main
def main():
    date = get_date()
    #文件存放路径
    trackerfile =r'/data/roger/pathtree/pv'
    splitpath = r'/data/roger/pathtree/tmp/'
    resultpath = r'/data/roger/pathtree/result/'
    if os.path.exists(resultpath):
        shutil.rmtree(resultpath)
    os.mkdir(resultpath)
    # get_pvdata_from_hive(date,trackerfile)
    split_bigfile_to_smlallfile(trackerfile,splitpath)
    for i in os.listdir(splitpath):
        tmpfile = splitpath+i
        data = get_data_from_file(tmpfile)
        spilttxt(data, date, resultpath+i)



    # for i in os.listdir(resultpath):
    #     putdatatohdfs(date, resultpath+i)
    #
    # os.system('''
    # hive -e "set hive.exec.dynamic.partition.mode=nonstrict;
    # set tez.queue.name=tenhive;
    # alter table fdm.fdm_tracker_pathtree_result_de drop partition (days='%s');
    # alter table fdm.fdm_tracker_pathtree_result_de add partition (days='%s') location '/apps/hive/warehouse/fdm/fdm_tracker_pathtree_result_de/%s';
    # "''' % (date,date,date))
    # logging.info('路径结果文件关联到外部表 完成')
    logging.info('本次测试结果完成，数据在 /data/roger/pathtree/result/')

main()
