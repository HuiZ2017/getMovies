#!usr/bin/env python
#-*- coding:utf-8 _*-  
""" 
@author:ZhangHui
@file: getGQmovie.py 
@time: 2017/12/17 
"""
from opDBforPostgre import DBoperation,getInsertSQL
from bs4 import BeautifulSoup as bs
selectSQL = "select link,movie from \"public\".\"movies\" where link =\'{}\' and movie = \'{}\'"
insertSQL = "INSERT INTO \"public\".\"movies\" (link, moviename) VALUES (%s, %s)"
def gethtml(url):
    from getUserAgent import getUserAgent
    from time import sleep
    import requests
    headers = getUserAgent()
    #cookies = {"Cookie":'UM_distinctid=15ebce789f928d-0d05fef10bdc18-3a3e5f04-15f900-15ebce789fa57; PHPSESSID=jfjre1btlhagq8bpbeo3v1nq75; CNZZDATA1256968772=809799400-1481347274-http%253A%252F%252Fgaoqing.la%252F%7C1513485853'}
    html = requests.get(url=url,headers=headers)
    sleep(0.05)
    return html
def tool(soup):
    import re
    data = ''
    n = soup
    while True:
        try:
            n = n.next_sibling
        except AttributeError as e:
            break
        if re.match('.*span.*',str(n)):break
        if '<a' in str(n):
            data ='%s %s' %(data,n.text)
    return data
def getDetail(url):
    #url = https://gaoqing.fm/view/55c720bfcc48
    Detail = {
        'name':'',
        'director':'',
        'actor':'',
        'type':'',
        'country':'',
        'onboardtime':'',
        'time':'',
        'score_in':'',
        'score_db':'',
    }
    html = gethtml(url)
    soup = bs(html.text, 'html.parser')
    Detail['name'] = soup.select('#mainrow div.row div.col-md-12 h2 a')[0].text
    viewfilm = soup.select('div#viewfilm')[0]
    record = viewfilm.find_all('span')
    for i in range(len(record)):
        key = record[i].text
        value = tool(record[i])
        # print(key,value)
        if '导演' in key:Detail['director'] = value.lstrip(' ')
        if '主演' in key:Detail['actor'] = value
        if '类型' in key:Detail['type'] = value
        if '地区' in key:Detail['country'] = value
        if '上映' in key:Detail['onboardtime'] = value
        if '片长' in key:Detail['time'] = record[i].next_sibling.lstrip('：').rstrip('\t')
        if '打分' in key:Detail['score_in'] = record[i+1].text
        if '评分' in key:Detail['score_db'] = record[i+1].text
    return Detail


def getMovieLinks(url,DBobj):
    html = gethtml(url)
    soup = bs(html.text,'html.parser')
    links = soup.select('li div div p a')
    for link in links:
        movieLink = link['href']
        movieTitle = link.text
        ##检索并插入数据库
        # if DBobj.exec_getreturn(sqlstr=selectSQL,data=(movieLink,movieTitle)):
        #     print('Data already exist')
        # else:
        #     DBobj.insert(sqlstr=insertSQL,data=(movieLink,movieTitle))

        Detail = getDetail(movieLink)
        Detail['movie'] = movieTitle
        Detail['link'] = movieLink
        INSERTSQL = getInsertSQL(Detail)
        SELECTSQL = selectSQL.format(Detail['link'],Detail['movie'])
        if DBobj.exec_getreturn2(SELECTSQL):
            print('Data already exist')
        else:
            DBobj.exec_commit(sqlstr=INSERTSQL)
            getDownloadLink(Detail['link'])
            print('%s insert sussess' %Detail['movie'])
def getDownloadLink(link):
    pass

if __name__ == '__main__':
    args = {
        'type': "",
        'country': "",
        'director': "",
        'actor': "",
        'year': "",
        'sort': ""  # 热度,最新,评分,IMDb
    }
    start_page = 4;end_page = 10
    DBobj = DBoperation(database='Movies', user='DBtest', password='hui', host='127.0.0.1', port='5432')
    #DBobj.tableCreate(tableName='movies')
    url = 'https://gaoqing.fm/ajax.php?type=%s&country=%s&director=%s&actor=%s&year=%s&sort=%s&p={}' \
          % (args['type'], args['country'], args['director'], args['actor'], args['year'], args['sort'])
    while start_page<=end_page:
        getMovieLinks(url.format(start_page),DBobj)
        start_page += 1
    # URL = 'https://gaoqing.fm/view/3763566ec44a'
    # getDetail(URL)