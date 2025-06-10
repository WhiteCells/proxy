import multiprocessing
import time
import random
import os
import queue # 用于捕获队列的 Empty 异常

# --------------------
# 1. 定义工作函数
# --------------------
def worker_function(task_queue, result_queue):
    """
    工人进程持续运行的函数。
    它从任务队列获取任务，处理后将结果放入结果队列。
    """
    pid = os.getpid()
    print(f"[Worker {pid}] 已启动。")

    while True:
        try:
            # 从任务队列获取一个任务，如果队列为空则阻塞等待
            task_data = task_queue.get()

            # 'STOP' 是我们定义的停止信号
            if task_data == 'STOP':
                print(f"[Worker {pid}] 收到停止信号，即将退出。")
                break

            print(f"[Worker {pid}] 正在处理任务: {task_data}")

            # --- 模拟可能发生的异常 ---
            # 1) 模拟可恢复的任务级异常 (不会杀死进程)
            if task_data % 5 == 0:
                print(f"[Worker {pid}] 任务 {task_data} 触发了一个可恢复的异常。")
                raise ValueError(f"处理任务 {task_data} 时发生错误")

            # 2) 模拟致命的进程级崩溃 (会杀死进程)
            if task_data == 7:
                print(f"!!! [Worker {pid}] 发生致命错误，进程即将崩溃！!!!")
                # 使用 os._exit(1) 来模拟一个无法捕获的、直接退出的致命错误
                # 这比 sys.exit() 更能模拟真实世界的崩溃
                os._exit(1)

            # 模拟耗时工作
            time.sleep(random.uniform(0.5, 1.5))
            result = f"任务 {task_data} 的结果"
            result_queue.put(('SUCCESS', pid, result))

        except ValueError as e:
            # 捕获可恢复的异常，将错误信息放入结果队列
            result_queue.put(('ERROR', pid, str(e)))
        except Exception as e:
            # 捕获其他意想不到的异常
            print(f"[Worker {pid}] 捕获到意外异常: {e}")
            result_queue.put(('FATAL', pid, str(e)))


# --------------------
# 2. 定义主管/管理器
# --------------------
def main():
    """
    主进程，负责：
    1. 创建和管理工作进程列表。
    2. 将任务放入队列。
    3. 监控工作进程的健康状况，如果发现有进程死掉，就重启一个。
    4. 从结果队列中收集结果。
    """
    num_workers = 4
    total_tasks = 20

    # 创建用于进程间通信的队列
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()

    # 创建并启动初始的工作进程
    workers = []
    for i in range(num_workers):
        process = multiprocessing.Process(
            target=worker_function,
            args=(task_queue, result_queue)
        )
        process.start()
        workers.append(process)
        print(f"[Main] 启动了 Worker，PID: {process.pid}")

    # 将所有任务放入任务队列
    for i in range(total_tasks):
        task_queue.put(i)

    # 主循环：收集结果并监控工人状态
    tasks_completed = 0
    while tasks_completed < total_tasks:
        # --- 核心：监控并重启死掉的进程 ---
        for i, p in enumerate(workers):
            if not p.is_alive():
                print(f"!!! [Main] 检测到 Worker (PID: {p.pid}) 已经死亡。正在重启... !!!")
                # 创建一个新进程来替代死掉的进程
                new_process = multiprocessing.Process(
                    target=worker_function,
                    args=(task_queue, result_queue)
                )
                new_process.start()
                workers[i] = new_process # 在列表中替换掉旧的进程对象
                print(f"[Main] 新的 Worker (PID: {new_process.pid}) 已启动。")

        # 从结果队列获取结果 (使用超时以避免永久阻塞)
        try:
            status, worker_pid, data = result_queue.get(timeout=1.0)
            if status == 'SUCCESS':
                print(f"[Main] 收到来自 {worker_pid} 的成功结果: {data}")
            elif status == 'ERROR':
                print(f"[Main] 收到来自 {worker_pid} 的可恢复错误: {data}")
            
            tasks_completed += 1
        except queue.Empty:
            # 如果1秒内没有结果，循环继续，这允许我们继续监控进程状态
            print("[Main] 结果队列为空，继续监控...")
            pass

    print("\n[Main] 所有任务都已处理完毕。")

    # ---- 清理工作 ----
    # 向队列发送停止信号，让所有工人进程正常退出
    for _ in range(num_workers):
        task_queue.put('STOP')

    # 等待所有工人进程终止
    print("[Main] 等待所有 Worker 进程退出...")
    for p in workers:
        p.join(timeout=5) # 等待最多5秒
        if p.is_alive():
            print(f"[Main] Worker {p.pid} 未能正常退出，强制终止。")
            p.terminate() # 如果超时仍未退出，则强制终止

    print("[Main] 程序执行完毕。")


if __name__ == '__main__':
    # 在Windows或macOS上，最好使用 'forkserver' 或 'spawn' 启动方法
    # 'spawn' 是跨平台最兼容的
    multiprocessing.set_start_method('spawn', force=True)
    main()