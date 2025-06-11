import os
import csv
import time
import pandas as pd
from .detail import get_detail


output_dir = "output_dir"
done_log_dir = "done_log_dir"
fieldnames = ['name', 'city', 'district', 'street', 'phone_number', 'type']

def task(file_path: str):
    print(f"[file]: {file_path}")

    pid = os.getpid()

    base_name = os.path.basename(file_path)
    output_path = os.path.join(output_dir, f"detail_{base_name}")
    done_log_path = os.path.join(done_log_dir, f"done_{base_name}.log")

    MAX_RETRIES = 3
    RETRY_DELAY = 1 # 秒

    try:
        n = 0
        # 如果日志文件是存在的，则从日志文件中读取上一次处理到的行
        if os.path.exists(done_log_path):
            with open(done_log_path, "r", encoding="utf-8") as f:
                last_line = f.readlines()[-1].strip()
                if last_line.isdigit():
                    n = int(last_line)
        reader = pd.read_csv(
            file_path, 
            chunksize=1000, 
            usecols=['link'],
            skiprows=range(1, n + 1))

        header_written = False

        # proxy_header = get_proxy_ip()
        proxy_header = ""
        cur_line = n

        for chunk in reader:
            for url in chunk['link']:
                print(f"[url]: {url}")

                cur_line += 1

                if not isinstance(url, str) or not url.strip():
                    continue

                for attempt in range(MAX_RETRIES):
                    try:
                        res = get_detail(url, proxy_header)
                        if res is None:
                            raise Exception()
                        res_address = res.get('address', {})
                        row = {
                            'name': res.get('name', ''),
                            'city': res_address.get('city', ''),
                            'district': res_address.get('district', ''),
                            'street': res_address.get('street', ''),
                            'phone_number': res.get('phone_number', ''),
                            'type': res.get('type', '')
                        }
                        print(">>>>> mapbar_masget", res)
                        print(">>>>> mapbar_masget row", row)
                        with open(output_path, 'a', newline='', encoding='utf-8-sig') as f:
                            """
                            {'name': '苹果树儿童摄影', 'address': {'city': '芜湖', 'district': '', 'street': ''}, 'phone_number': '无，', 'type': '摄影冲印、印务'}
                            需要写成 excel 格式
                            name,city,district,street,phone_number,type
                            苹果树儿童摄影,芜湖,,,,摄影冲印、印务
                            """
                            
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            # 如果是第一次写入，先写表头
                            if not header_written and f.tell() == 0:
                                writer.writeheader()
                                header_written = True
                            writer.writerow(row)
                        break

                    except Exception as e:
                        print(f"    [Worker {pid}] URL '{url}' 第 {attempt + 1}/{MAX_RETRIES} 次尝试失败: {e}")
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(RETRY_DELAY)
                        else:
                            print(f"!!! [Worker {pid}] FATAL: URL '{url}' 所有重试均失败。正在终止工作进程... !!!")
                    finally:
                        # 记录日志
                        with open(done_log_path, "a", encoding="utf-8") as f:
                            f.write(f"{cur_line}\n") # 记录当前的行数
        # raise Exception()
        # os._exit(1)
        # return
    except FileNotFoundError:
        print(f"!!! [Worker {pid}] ERROR: 文件未找到 {file_path}")
        # raise Exception()
        # os._exit(1)
    except Exception as e:
        print(f"!!! [Worker {pid}] ERROR: 处理文件 {file_path} 时发生严重错误: {e}")
        # raise Exception()
        # os._exit(1)
