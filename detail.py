import requests
from lxml import html

def mapbar_masget(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36'
    }
    result= {
        'name': None,
        'address': {
            'city': None,
            'district': None,
            'street': None
        },
        'phone_number' : None,
        'type': None,
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        response.encoding = 'utf-8'  # 设置编码
        tree = html.fromstring(response.text)

        name_list = tree.xpath("//div[@class='POILeftA']/h1/text()")
        city_list = tree.xpath("//ul[@class='POI_ulA']//li[2]/a[1]/text()")
        district_list = tree.xpath("//ul[@class='POI_ulA']//li[2]/a[2]/text()")
        street_list = tree.xpath("//ul[@class='POI_ulA']//li[2]/text()[4]")
        phone_number_list = tree.xpath("//li[@class='telCls']/text()[2]")
        if phone_number_list == "无，":
            phone_number_list =  None

        type_list = tree.xpath("//ul[@class='POI_ulA']/li[4]/text()")

        result['name'] = name_list[0].strip() # name
        result['address']['city'] = city_list[0].strip() # city
        result['address']['district'] = district_list[0].strip() # 
        result['address']['street'] = street_list[0].strip() # street
        result['phone_number'] = phone_number_list[0].strip() # phone_number
        result['type'] = type_list[0].strip() # type

        return result
    else:
        return None


url="https://poi.mapbar.com/beijing/MAPASAZOYNJRQWJRHMFAZEZ"
res = mapbar_masget(url)
print(res)

"""
交通设施
公司企业
地产小区
餐饮服务
宾馆饭店
其它类型
商务大厦
文化教育
汽车服务
生活服务
金融行业
旅游景点
休闲娱乐
医疗卫生
邮政电信
运动场馆
政府机关
综合商场
"""