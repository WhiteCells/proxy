import multiprocessing
import threading
import time
import os
import queue
import traceback
import random
import pandas as pd
import csv
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

_STOP_SENTINEL = object()

class ResilientPool:
    """
    一个健壮的“即发即忘”型进程池。
    - 当工作进程崩溃时会自动重启。
    - 不处理任务的返回值，但会报告任务执行中的异常。
    """
    def __init__(self, processes=None):
        if processes is None:
            processes = os.cpu_count() or 1
        if processes < 1:
            raise ValueError("进程数必须至少为1")

        self._num_workers = processes
        # 使用 JoinableQueue 来跟踪任务完成情况
        self._task_queue = multiprocessing.JoinableQueue()
        # 错误队列仍然有用，用于记录失败的任务
        self._error_queue = multiprocessing.Queue()
        self._workers = []
        self._shutdown_event = threading.Event()

        # 启动后台线程来报告错误和监控工人健康
        self._error_thread = threading.Thread(target=self._handle_errors, daemon=True)
        self._monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
        self._error_thread.start()
        self._monitor_thread.start()

        # 创建并启动初始的工作进程
        for _ in range(self._num_workers):
            self._start_worker()

    def _start_worker(self):
        """创建一个新的工作进程并启动它。"""
        worker = multiprocessing.Process(
            target=self._worker_loop,
            args=(self._task_queue, self._error_queue)
        )
        worker.start()
        self._workers.append(worker)
        print(f"[Pool Supervisor] 启动了新工人，PID: {worker.pid}")
        return worker

    @staticmethod
    def _worker_loop(task_queue, error_queue):
        """工作进程运行的循环。"""
        while True:
            try:
                # 获取任务
                task = task_queue.get()
                if task is _STOP_SENTINEL:
                    # 收到停止信号，必须调用 task_done() 才能让主进程的 join() 继续
                    task_queue.task_done()
                    break

                func, args, kwargs = task
                try:
                    func(*args, **kwargs)
                except Exception as e:
                    print(f"[{os.getpid()}] Exception: {e}")
                    exc_info = (e, traceback.format_exc())
                    error_queue.put(exc_info)
            finally:
                # 确保每个从队列中取出的任务（除了哨兵）都有对应的 task_done() 调用
                # 哨兵已经在自己的逻辑里调用了
                if task is not _STOP_SENTINEL:
                    task_queue.task_done()
    
    def _handle_errors(self):
        """后台线程：从错误队列中取出信息并打印。"""
        while not self._shutdown_event.is_set() or not self._error_queue.empty():
            try:
                exception, tb_string = self._error_queue.get(timeout=0.1)
                print("--- [Pool Supervisor] 捕获到子进程错误 ---")
                print(f"异常类型: {type(exception).__name__}")
                print(f"异常信息: {exception}")
                print("追溯信息:")
                print(tb_string)
                print("-----------------------------------------")
            except queue.Empty:
                continue

    def _monitor_workers(self):
        """后台线程：定期检查工作进程是否存活，如果死亡则重启。"""
        while not self._shutdown_event.wait(timeout=1.0):
            for i, worker in enumerate(self._workers):
                if not worker.is_alive():
                    print(f"!!! [Pool Supervisor] (PID: {worker.pid}) down。replaceing...")
                    # 直接在列表中替换，避免并发问题
                    new_worker = self._start_worker()
                    self._workers[i] = new_worker

    def apply_async(self, func, args=(), kwds={}):
        if self._shutdown_event.is_set():
            raise RuntimeError("pool closed")
        task = (func, args, kwds)
        self._task_queue.put(task)

    def wait_completion(self):
        self._task_queue.join()

    def close(self):
        if not self._shutdown_event.is_set():
            print("[Pool Supervisor] 正在关闭... 发送停止信号。")
            self._shutdown_event.set()
            for _ in range(self._num_workers):
                self._task_queue.put(_STOP_SENTINEL)

    def join(self):
        """等待所有工作进程退出。必须在 close() 之后调用。"""
        # 等待所有工人进程终止
        for worker in self._workers:
            worker.join()
        
        # 等待后台线程结束
        self._error_thread.join()
        self._monitor_thread.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("[Pool Supervisor] 所有任务已提交，等待完成...")
        self.wait_completion()
        self.close()
        self.join()
        print("[Pool Supervisor] 进程池已完全关闭。")


def task(file_path: str):
    print(f"[{os.getpid()}] start {dir}...")

    # # 从 file_path 中读取文件
    # # 使用 pandas 逐行从 csv 文件中读取 link 列的行
    # urls = ""
    # for url in urls:
    #     try:
    #         res = mapbar_masget(url)
    #         # res 为字典类型

    #         # 创建 csv 文件名字，名字从 file_path 中取出基本的名字，然后前面加上 detail
            
    #         # 向新建的 csv 文件中写入结果

    #         os._exit(1)
    #     except Exception as e:
    #         # 异常后重试，如果超出充实次数，退出
    #         # os._exit(1)

    # 模拟可恢复的异常
    # if x == 3:
    #     print(f"[Worker {pid}] 任务 {x} 将触发一个 ValueError。")
    #     raise ValueError(f"任务 {x} 的特定错误")
        
    # # 模拟致命的、导致进程崩溃的错误
    # if x == 7:
    #     print(f"!!! [Worker {pid}] 任务 {x} 将导致进程崩溃 !!!")
    #     time.sleep(0.1) # 确保 print 语句能被刷新
    #     os._exit(1) # 强制退出

    # time.sleep(random.uniform(1, 2))
    # print(f"[Worker {pid}] 完成任务 {x}。")

    pid = os.getpid()
    output_dir = "output_details"
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.basename(file_path)
    output_path = os.path.join(output_dir, f"detail_{base_name}")

    # --- 关键：如果输出文件已存在，可以选择跳过或删除。这里我们选择删除重做。---
    if os.path.exists(output_path):
        os.remove(output_path)
        print(f"[{pid}] 已存在旧的输出文件 {os.path.basename(output_path)}，已删除。")

    MAX_RETRIES = 3
    RETRY_DELAY = 1 # 秒

    try:
        # 使用 chunksize 逐块读取大文件，非常节省内存
        # 假设 link 列是我们要处理的列
        reader = pd.read_csv(file_path, chunksize=1000, usecols=['link'])
        
        header_written = False

        for chunk in reader:
            for url in chunk['link']:
                if not isinstance(url, str) or not url.strip():
                    continue

                # --- 重试逻辑 ---
                success = False
                for attempt in range(MAX_RETRIES):
                    try:
                        res = mapbar_masget(url)
                        
                        # --- 成功处理，写入结果 ---
                        # 使用'a'模式追加写入
                        with open(output_path, 'a', newline='', encoding='utf-8-sig') as f:
                            writer = csv.DictWriter(f, fieldnames=res.keys())
                            # 如果是第一次写入，先写表头
                            if not header_written and f.tell() == 0:
                                writer.writeheader()
                                header_written = True
                            writer.writerow(res)
                        
                        success = True
                        break # 成功，跳出重试循环

                    except Exception as e:
                        print(f"    [Worker {pid}] URL '{url}' 第 {attempt + 1}/{MAX_RETRIES} 次尝试失败: {e}")
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(RETRY_DELAY)
                        else:
                            # 所有重试均失败
                            print(f"!!! [Worker {pid}] FATAL: URL '{url}' 所有重试均失败。正在终止工作进程... !!!")
                            # 这是你要求的关键行为：终止进程
                            os._exit(1)

                # # 注释掉这个，因为一个进程应该处理完整个文件，而不是只处理一个URL就退出
                # # print(f"[{pid}] 成功处理一个URL，现在退出以供测试")
                # # os._exit(1)

    except FileNotFoundError:
        print(f"!!! [Worker {pid}] ERROR: 文件未找到 {file_path}")
    except Exception as e:
        # 捕获其他文件级别的错误，例如pandas读取错误
        print(f"!!! [Worker {pid}] ERROR: 处理文件 {file_path} 时发生严重错误: {e}")
        # 这里也可以选择 os._exit(1) 来终止进程
        raise e # 将异常传递给 ResilientPool 的错误处理线程



if __name__ == '__main__':
    import random
    multiprocessing.set_start_method('spawn', force=True)

    dirs = [
        "/home/cells/dev/py-project/test/test",
        # "/home/address/all_data/生活服务",
    ]
    
    with ResilientPool() as pool:
        for dir in dirs:
            if not dir:
                continue
            for root, dirs, files in os.walk(dir):
                # 再遍历目录下的所有文件
                for file in files:
                    print(file)
                    # pool.apply_async(task, args=(files,))
        
        print(f"all task commit")
