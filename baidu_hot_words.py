#import os
#import random
import requests
#import json
from bs4 import BeautifulSoup
import time
#import sys
import traceback
import threading
import redis
import re
from pymongo import MongoClient
import tieba_fetch_bySort

def words_into_redis(pool,_as,name):
    rcli = redis.StrictRedis(connection_pool=pool)
    for a in _as:
        print(a.text.strip().encode().decode())
        time.sleep(3600)
        word=a.text.strip().encode('gb18030')
        print(word.decode('utf-8'))
        
        if word!='' and word!=None:
            rcli.rpush(name,word)

def fetch_hot_words(pool):
    num_=''
    _url='http://hot.news.baidu.com/'
    for i in range(50):
        _index='index{_num}.html'.format(_num=num_)
        url=_url+_index
        res=requests.get(url,timeout=30)
        try:
            html=res.content.decode('utf-8')
        except UnicodeDecodeError:
            html=res.text
        bs=BeautifulSoup(html, 'html.parser')
        _as=bs.select('div#wrapper div#container div#content ul li a[target]')
        words_into_redis(pool,_as,'baid_hot_words')
        num_='_{nm}'.format(nm=str(i+1))

def fetch_tie(ba_url):
    res=requests.get(ba_url,timeout=30)
    try:
        html=res.content.decode('utf-8')
    except UnicodeDecodeError:
        html=res.text
    bs=BeautifulSoup(html, 'html.parser')
    card_nums=bs.select('div.wrap1 div.wrap2 div.header div.head_main div.head_content div.card_title div.card_num')
    if len(card_nums):
        card_num=card_nums[0]
        ba_m_num=int(card_num.select('span.card_menNum')[0].text.replace(',','')) if len(card_num.select('span.card_menNum')) else 0
        ba_p_num=int(card_num.select('span.card_infoNum')[0].text.replace(',','')) if len(card_num.select('span.card_infoNum')) else 0
        return {'ba_m_num':ba_m_num,'ba_p_num':ba_p_num}
    else:
        return None


def fetch_event(word,db,pool):
    pn=1
    url='http://tieba.baidu.com/f/search/res?isnew=1&kw=&qw={word}&rn=10&un=&only_thread=0&sm=2&sd=&ed=&pn={pn}'.format(word=word,pn=str(pn))
    res=requests.get(url,timeout=30)
    try:
        html=res.content.decode('utf-8')
    except UnicodeDecodeError:
        html=res.text
    bs=BeautifulSoup(html, 'html.parser')
    _as=bs.select('div.wrap1 div.wrap2 div.s_main div.s_post_list div.s_post a.p_forum')
    items=[]
    for a in _as:
        ba_name=a.select('font.p_violet')[0].text if len(a.select('font.p_violet')) else None
        if ba_name==None:
            continue
        ba_url='http://tieba.baidu.com/'+a.get('href')
        tieba=fetch_tie(ba_url.strip())
        if tieba==None:
            continue
        tieba['_id']=ba_name
        tieba['ba_url']=ba_url
        items.append(tieba)
    tieba_fetch_bySort.item_into_mongo(items,db,pool)
    print(len(items),'tieba into mongo')
    last=int(bs.select('a.last')[0].get('href').split('pn=')[-1]) if len(bs.select('a.last')) else False
    while last and pn<=last:
        pn+=1
        url='http://tieba.baidu.com/f/search/res?isnew=1&kw=&qw={word}&rn=10&un=&only_thread=0&sm=2&sd=&ed=&pn={pn}'.format(word=word,pn=str(pn))
        res=requests.get(url,timeout=30)
        try:
            html=res.content.decode('utf-8')
        except UnicodeDecodeError:
            html=res.text
        bs=BeautifulSoup(html, 'html.parser')
        _as=bs.select('div.wrap1 div.wrap2 div.s_main div.s_post_list div.s_post a.p_forum')
        items=[]
        for a in _as:
            ba_name=a.select('font.p_violet')[0].text if len(a.select('font.p_violet')) else None
            if ba_name==None:
                continue
            ba_url='http://tieba.baidu.com'+a.get('href')
            tieba=fetch_tie(ba_url)
            if tieba==None:
                continue
            tieba['_id']=ba_name
            tieba['ba_url']=ba_url
            items.append(tieba)
        tieba_fetch_bySort.item_into_mongo(items,db,pool)
        print(len(items),'tieba into mongo')



def byEvent_work(pool,db,word):
    try:
        #t1=threading.Thread(target=fetch_hot_words,args=(pool,))
        #t1.start()
        print(word)
        fetch_event(word=word,db=db,pool=pool)
    except:
      traceback.print_exc()

if __name__=='__main__':
    pool = redis.ConnectionPool(host='101.201.37.28', port=6379,password='r-2zee00173ec68024.redis.rds.aliyuncs.com:Abc123456')
    mon_url='mongodb://root:joke123098@101.201.37.28:3717/admin?maxPoolSize=5'
    mon_url2='mongodb://root:joke123098@101.201.37.28:3718/admin?maxPoolSize=5'
    mcli = MongoClient(mon_url)
    mcli2 = MongoClient(mon_url2)
    db1 = mcli.get_database('baidutieba')
    db2 = mcli2.get_database('baidutieba')
    byEvent_work(pool,db1,db2)
         




