#!/usr/bin/env Python
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import requests

secret_id = 'oba1h8wcj9wivf185evr'
secret_key = 'sn59cyjeb4srn8nmdbzcdncgo5ai85t4'
SECRET_PATH = './.secret'
PROXY_IP = './.proxy_ip'
# 秒
IP_EXPIRE = 300


def _get_secret_token():
    r = requests.post(url='https://auth.kdlapi.com/api/get_secret_token', data={'secret_id': secret_id, 'secret_key': secret_key})
    if r.status_code != 200:
        raise KdlException(r.status_code, r.content.decode('utf8'))
    res = json.loads(r.content.decode('utf8'))
    code, msg = res['code'], res['msg']
    if code != 0:
        raise KdlException(code, msg)
    secret_token_ = res['data']['secret_token']
    expire = str(res['data']['expire'])
    _time = '%.6f' % time.time()
    return secret_token_, expire, _time


def _read_secret_token():
    with open(SECRET_PATH, 'r') as f:
        token_info = f.read()
    secret_token_, expire, _time, last_secret_id = token_info.split('|')
    if float(_time) + float(expire) - 3 * 60 < time.time() or secret_id != last_secret_id:  # 还有3分钟过期或SecretId变化时更新
        secret_token_, expire, _time = _get_secret_token()
        with open(SECRET_PATH, 'w') as f:
            f.write(secret_token_ + '|' + expire + '|' + _time + '|' + secret_id)
    return secret_token_


def get_secret_token():
    if os.path.exists(SECRET_PATH):
        secret_token_ = _read_secret_token()
    else:
        secret_token_, expire, _time = _get_secret_token()
        with open(SECRET_PATH, 'w') as f:
            f.write(secret_token_ + '|' + expire + '|' + _time + '|' + secret_id)
    return secret_token_


class KdlException(Exception):
    """异常类"""

    def __init__(self, code=None, message=None):
        self.code = code
        if sys.version_info[0] < 3 and isinstance(message, str):
            message = message.encode("utf8")
        self.message = message
        self._hint_message = "[KdlException] code: {} message: {}".format(self.code, self.message)

    @property
    def hint_message(self):
        return self._hint_message

    @hint_message.setter
    def hint_message(self, value):
        self._hint_message = value

    def __str__(self):
        if sys.version_info[0] < 3 and isinstance(self.hint_message, str):
            self.hint_message = self.hint_message.encode("utf8")
        return self.hint_message


def _get_proxy_ip():
    signature = get_secret_token()
    # 提取代理API接口，获取1个代理IP
    api_url = f"https://dps.kdlapi.com/api/getdps/?secret_id={secret_id}&signature={signature}&num=1&pt=1&sep=1"

    # 获取API接口返回的代理IP
    proxy_ip_ = requests.get(api_url).text
    _time = '%.6f' % time.time()
    ip_expire_ = '%.f' % IP_EXPIRE
    return proxy_ip_, ip_expire_, _time


def _read_proxy_ip():
    with open(PROXY_IP, 'r') as f:
        proxy_ip_info = f.read()
    proxy_ip_, ip_expire, _time, last_secret_id = proxy_ip_info.split('|')
    if float(_time) + float(ip_expire) - 3 * 60 < time.time() or secret_id != last_secret_id:
        proxy_ip_, ip_expire_, _time = _get_proxy_ip()
        with open(PROXY_IP, 'w') as f:
            f.write(proxy_ip_ + '|' + ip_expire_ + '|' + _time + '|' + secret_id)
    return proxy_ip_


def get_proxy_ip():
    """
    获取代理ip，ip时间300s，即五分钟
    :return:
    """
    if os.path.exists(PROXY_IP):
        proxy_ip_ = _read_proxy_ip()
    else:
        proxy_ip_, ip_expire_, _time = _get_proxy_ip()
        with open(PROXY_IP, 'w') as f:
            f.write(proxy_ip_ + '|' + ip_expire_ + '|' + _time + '|' + secret_id)
    return proxy_ip_


if __name__ == '__main__':
    secret_token = get_secret_token()
    print(secret_token)
    # print('%.f' % IP_EXPIRE)
