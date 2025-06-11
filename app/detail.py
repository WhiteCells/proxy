import requests
import html


def get_ele_by_xPath(tree, path):
    try:
        return tree.xpath(path)
    except Exception as e:
        return None


"""
详细内容接口
"""
def get_detail(url, proxy_header):
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

        name_list = get_ele_by_xPath(tree, "//div[@class='POILeftA']/h1/text()")
        city_list = get_ele_by_xPath(tree, "//ul[@class='POI_ulA']//li[2]/a[1]/text()")
        district_list = get_ele_by_xPath(tree, "//ul[@class='POI_ulA']//li[2]/a[2]/text()")
        street_list = get_ele_by_xPath(tree, "//ul[@class='POI_ulA']//li[2]/text()[4]")
        phone_number_list = get_ele_by_xPath(tree, "//li[@class='telCls']/text()[2]")
        if phone_number_list == "无，":
            phone_number_list = None

        type_list = get_ele_by_xPath(tree, "//ul[@class='POI_ulA']/li[4]/text()")

        result['name'] = name_list[0].strip() if len(name_list) > 0 else ''  # name
        result['address']['city'] = city_list[0].strip() if len(city_list) > 0 else ''  # city
        result['address']['district'] = district_list[0].strip() if len(district_list) > 0 else ''  # district
        result['address']['street'] = street_list[0].strip() if len(street_list) > 0 else ''  # street
        result['phone_number'] = phone_number_list[0].strip() if len(phone_number_list) > 0 else ''  # phone_number
        result['type'] = type_list[0].strip() if len(type_list) > 0 else ''  # type

        return result
    else:
        return None