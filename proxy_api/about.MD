## 公司爬取脚本

### 代理使用

proxy_api.py

```python
#!/usr/bin/env Python
# -*- coding: utf-8 -*-

"""
使用requests请求代理服务器
请求http和https网页均适用
"""

import requests
from proxy_api import get_proxy_ip

# 获取API接口返回的代理IP
proxy_ip = get_proxy_ip()


# 白名单方式（需提前设置白名单）
proxies = {
    "http": "http://%(proxy)s/" % {"proxy": proxy_ip},
    "https": "http://%(proxy)s/" % {"proxy": proxy_ip}
}

# 要访问的目标网页
target_url = "https://dev.kdlapi.com/testproxy"

# 使用代理IP发送请求
response = requests.get(target_url, proxies=proxies)

# 获取页面内容
if response.status_code == 200:
    print(response.text)

```