'''Spider tools

Method:
    get_proxy:      获取代理服务器地址
    delete_proxy:   通知代理池删除代理服务器
    get_UA:         获取随机 UA
    get_IP:         获取随机国内 IP
'''

import random
import requests
import logging
import json


with open('ip_list.json', 'r') as f:
    IP_list = json.load(f)

with open('user_agent.json', 'r') as f:
    UA_list = json.load(f)


def get_proxy(proxy_url):
    '''获取代理服务器地址

    proxy_url:  代理池地址
    '''
    try:
        proxy = requests.get(proxy_url + 'get/', timeout=5).json().get('proxy')
        return proxy
    except:
        return None


def delete_proxy(proxy_url, proxy):
    '''通知代理池删除代理服务器

    proxy_url:  代理池地址
    proxy:      通知删除的代理服务器地址
    '''
    try:
        requests.get('{}delete?proxy={}'.format(proxy_url, proxy))
    except:
        pass


def get_IP():
    '''获取随机国内 IP'''
    ip = random.choice(IP_list)
    _ip = ip.split('.')
    for i, v in enumerate(_ip):
        if int(v) == 0:
            _ip[i] = str(random.randint(0, 255))
    ip = '.'.join(_ip)
    return ip


def get_UA():
    '''获取随机 UA'''
    return random.choice(UA_list)
