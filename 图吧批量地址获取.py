import requests
from lxml import html
import pandas as pd
import os
import time
import sys
import subprocess
from proxy_api import get_proxy_ip

# === 文件路径设置 ===
input_path = r"C:\Users\81027\Desktop\图吧全国字段 - 副本.xlsx"
label_path = r'C:\Users\81027\Desktop\图吧全国字段_已修改.xlsx'
done_log_path = 'done_records.txt'
base_url = 'http://poi.mapbar.com'

# === 加载 Excel 数据 ===
city_df = pd.read_excel(input_path)
label_df = pd.read_excel(label_path)

# === 读取已完成记录 ===
if os.path.exists(done_log_path):
    with open(done_log_path, 'r', encoding='utf-8') as f:
        done_records = set(line.strip() for line in f.readlines())
else:
    done_records = set()

# === 开始爬取 ===
for label_index, label_row in label_df.iterrows():
    classify = label_row['分类']
    subitem = label_row['子项']
    link = label_row['链接']

    max_retries = 5  # 每次请求最大重试次数

    for city_idx, city_row in city_df.iterrows():
        province = city_row['省']
        city = city_row['市']
        url = city_row['链接'] + link

        record_key = f"{province}-{city}-{classify}-{subitem}"
        if record_key in done_records:
            print(f"已处理，跳过：{record_key}")
            continue

        print(f"\n正在处理：{record_key} - {url}")

        for attempt in range(max_retries):
            proxy_ip = get_proxy_ip()
            proxies = {
                "http": f"http://{proxy_ip}/",
                "https": f"http://{proxy_ip}/"
            }

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36'
            }

            try:
                # 请求初始页判断分页
                response = requests.get(url, headers=headers, timeout=10, proxies=proxies)
                response.encoding = 'utf-8'

                if not response.text.strip():
                    raise ValueError("页面为空")

                tree = html.fromstring(response.text)
                page_links = tree.xpath('//div[@class="sortPage cl"]//a')
                total_pages = len(page_links) if page_links else 1
                print(f"共 {total_pages} 页")

                all_data = []

                # 如果只有一页，直接用当前页面数据
                if total_pages == 1:
                    texts = tree.xpath('//div[@class="sortC"]//dd//a//text()')
                    hrefs = tree.xpath('//div[@class="sortC"]//dd//a//@href')

                    if not texts or not hrefs:
                        print("页面无数据，跳过并记录")
                        with open(done_log_path, 'a', encoding='utf-8') as f:
                            f.write(record_key + '\n')
                        done_records.add(record_key)
                        break  # 跳出 retry 循环，进入下一个任务

                    clean_texts = [t.strip() for t in texts]
                    Datatime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                    df = pd.DataFrame({
                        '省份': province,
                        '市': city,
                        f'{subitem}名': clean_texts,
                        '链接': hrefs,
                        '时间': Datatime,
                        '来源': base_url,
                        '分类': classify,
                        '子项': subitem,
                    })

                    all_data.append(df)

                else:
                    # 多页处理
                    for page in range(1, total_pages + 1):
                        page_success = False

                        for page_attempt in range(max_retries):
                            try:
                                if page == 1:
                                    paged_url = url  # 第一页不加后缀
                                else:
                                    paged_url = f"{url}_{page}"

                                print(f"  第 {page} 页（尝试 {page_attempt + 1}/{max_retries}）：{paged_url}")

                                page_resp = requests.get(paged_url, headers=headers, timeout=10, proxies=proxies)
                                page_resp.encoding = 'utf-8'
                                page_tree = html.fromstring(page_resp.text)

                                texts = page_tree.xpath('//div[@class="sortC"]//dd//a//text()')
                                hrefs = page_tree.xpath('//div[@class="sortC"]//dd//a//@href')

                                # 如果页面没有数据，跳过当前页面
                                if not texts or not hrefs:
                                    print(f"    第 {page} 页无数据，跳过此页")
                                    continue  # 跳过当前页，进入下一个页面的处理

                                clean_texts = [t.strip() for t in texts]
                                Datatime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                                df = pd.DataFrame({
                                    '省份': province,
                                    '市': city,
                                    f'{classify}名': clean_texts,
                                    '链接': hrefs,
                                    '时间': Datatime,
                                    '来源': base_url,
                                    '分类': classify,
                                    '子项': subitem,
                                })

                                all_data.append(df)
                                page_success = True
                                break  # 成功获取数据后退出重试循环

                            except Exception as e:
                                print(f"    第 {page_attempt + 1} 次失败：{e}")
                                if page_attempt < max_retries - 1:
                                    time.sleep(2)
                                else:
                                    print("    当前页连续失败，重启程序中...\n")
                                    time.sleep(5)
                                    subprocess.Popen([sys.executable, 'restart.py'])
                                    sys.exit(0)

                        if not page_success:
                            print(f"  第 {page} 页处理失败，跳过")
                            continue  # 如果当前页没有成功获取数据，跳过当前页

                # 保存数据
                if all_data:
                    final_df = pd.concat(all_data, ignore_index=True)
                    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop", '图吧data2025-4-23', classify, subitem, province)
                    os.makedirs(desktop_path, exist_ok=True)
                    save_path = os.path.join(desktop_path, f"{city}.xlsx")
                    final_df.to_excel(save_path, index=False)
                    print(f"已保存：{save_path}")

                    with open(done_log_path, 'a', encoding='utf-8') as f:
                        f.write(record_key + '\n')
                else:
                    print("无内容，记录并跳过。")
                    with open(done_log_path, 'a', encoding='utf-8') as f:
                        f.write(f"{record_key}\n")

                break  # 成功退出重试

            except Exception as e:
                print(f"第 {attempt + 1} 次总请求失败：{e}")
                if attempt < max_retries - 1:
                    print("正在重试初始页...\n")
                    time.sleep(2)
                else:
                    print("初始页失败，程序将重启...\n")
                    time.sleep(5)
                    subprocess.Popen([sys.executable, 'restart.py'])
                    sys.exit(0)