# coding=utf-8

import hashlib
import redis
import urlparse
import time
import re
from pykafka import KafkaClient
from scapy.all import *
import time
import logging
from pymongo import MongoClient

logging.getLogger("pykafka").addHandler(logging.StreamHandler())
logging.getLogger("pykafka").setLevel(logging.DEBUG)

URL_KEY = "urls"
URL_HASH_KEY = "url_hash_key"
SIM_RULE_KEY = "sim_rule_key"
client = redis.Redis(host='127.0.0.1',port=6379,db=8)

del_key_list = ["_"]

sys_config = {
    "database": {
        "db_name": "url_pool",
        "db_host": "mongodb://172.20.214.71:443/"
    }
}

client = MongoClient(sys_config['database']['db_host'])
db_connect = client[sys_config['database']['db_name']]

client = KafkaClient(zookeeper_hosts ="172.20.214.66:2181,172.20.214.69:2181,172.20.214.71:2181")
topic = client.topics['demo.crawled_firehose']

def md5(strs):
    m1 = hashlib.md5()
    m1.update(strs)
    return m1.hexdigest()

def UrlSortAndCleanKey(url_str):
    new_url = url_str
    url1 = urlparse.urlsplit(url_str)
    key_list = {}
    key_list2 = []
    value_list = []
    l = url1.query.split("&")
    for k in l:
        kk = k.split("=")
        if len(kk) > 1:
            key_list[kk[0]] = kk[1]
            value_list.append(kk[1])
    for p in key_list:
        if p not in del_key_list:
            key_list2.append(p)
    key_query_str = ""
    for pp in sorted(key_list.keys()):
        key_query_str += "&" + pp + "=" + key_list[pp]
    if url1.fragment != "":
        key_query_str += "#" + url1.fragment
    query_str = ""
    if key_query_str != "":
        query_str = "?" + key_query_str[1:]
    new_url = url1.scheme + '://' + url1.netloc + url1.path + query_str
    return new_url

def SimRepeatRemove(url_str):
    is_exists = False
    rex_symbol_list = ["$", "(", ")", "*", "+", ".", "[", "]", "?", "\\", "^", "{", "}", "|", "/"]
    result = client.lrange(SIM_RULE_KEY,0,-1)
    for p in result:
        matchObj = re.search(p,url_str, re.M|re.I)
        if matchObj:
            is_exists = True
            break
    if is_exists == False:
        for rex in rex_symbol_list:
            url_str = url_str.replace(rex,'\\' + rex)
            url_str = url_str.replace(r'\\','\\')
        url_str = '^' + url_str + '$'
        rule = re.sub('\d+','(\d+)' , url_str, count=0, flags=0)
        client.lrem(SIM_RULE_KEY,rule,num=0)
        client.lpush(SIM_RULE_KEY,rule)
    return is_exists


def UrlClean(urls,url):
    t = int(time.time())
    url = UrlSortAndCleanKey(url)
    url_hash = md5(url)
    is_exists = client.hexists(URL_HASH_KEY,url_hash)
    if is_exists == False:
        if SimRepeatRemove(url) == False:
            client.hset(URL_HASH_KEY,url_hash,t)
            client.lpush(URL_KEY,url)
            return 1
        else:
            #print url
            return -1
    else:
        print url
        return 0

if __name__=="__main__":  
    consumer = topic.get_balanced_consumer(consumer_group='test_group',auto_commit_enable=True) 
    regex = 'response_url\": \"(.*?)\",'
    regex2 = 'appid\": \"(.*?)\"}'
    domain_list = []
    url = ""
    appid = ""
    for message in consumer:
        print message.value
        for m in re.findall(regex, message.value):
            url = m
            domain_list.append(url)
            break
        for k in re.findall(regex2, message.value):
            appid = k
            break
        urls_list = db_connect.urls.find({"appid": appid}).distinct('url')
        if UrlClean(urls_list,url) != -1:
            #去重后的url库
            try:
                db_connect.urls.save({"url": url, "appid": appid})
                print "mongodb insert success"
            except ValueError, e:
                print "mongodb insert Error"
        print "----------------------------------------------------------------------------------------------"
        print domain_list
        print "**********************************************************************************************"
