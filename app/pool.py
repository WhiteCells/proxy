import os
import multiprocessing
import threading
import traceback
import queue

_STOP_SENTINEL = object()

class ResilientPool:
    def __init__(self, processes=None, max_retries=2):
        if processes is None:
            processes = os.cpu_count() or 1
        if processes < 1:
            raise ValueError("进程数必须至少为1")

        self._num_workers = processes
        self._max_retries = max_retries
        self._task_queue = multiprocessing.JoinableQueue()
        self._error_queue = multiprocessing.Queue()
        self._workers = []
        self._shutdown_event = threading.Event()

        self._error_thread = threading.Thread(target=self._handle_errors, daemon=True)
        self._monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
        self._error_thread.start()
        self._monitor_thread.start()

        for _ in range(self._num_workers):
            self._start_worker()

    def _start_worker(self):
        worker = multiprocessing.Process(
            target=self._worker_loop,
            args=(self._task_queue, self._error_queue, self._max_retries)
        )
        worker.start()
        self._workers.append(worker)
        print(f"[Pool Supervisor] 启动进程 PID: {worker.pid}")
        return worker

    @staticmethod
    def _worker_loop(task_queue, error_queue, max_retries):
        while True:
            task = None
            try:
                task = task_queue.get()
                if task is _STOP_SENTINEL:
                    task_queue.task_done()
                    break

                if not isinstance(task, tuple) or len(task) != 4:
                    raise TypeError(f"非法任务格式: {task!r}")

                func, args, kwargs, retries_left = task

                try:
                    func(*args, **kwargs)
                except Exception as e:
                    print(f"[{os.getpid()}] 任务异常: {e}, 剩余重试: {retries_left}")
                    if retries_left > 0:
                        # 重新加入队列重试
                        task_queue.put((func, args, kwargs, retries_left - 1))
                    else:
                        # 最终失败才报告
                        error_queue.put((e, traceback.format_exc()))
            except Exception as e:
                error_queue.put((e, traceback.format_exc()))
            finally:
                if task is not None:
                    try:
                        task_queue.task_done()
                    except ValueError:
                        pass

    def _handle_errors(self):
        while not self._shutdown_event.is_set() or not self._error_queue.empty():
            try:
                exception, tb_string = self._error_queue.get(timeout=0.1)
                print("--- [Pool Supervisor] 捕获异常 ---")
                print(f"类型: {type(exception).__name__}")
                print(f"信息: {exception}")
                print("追溯:")
                print(tb_string)
                print("---------------------------------")
            except queue.Empty:
                continue

    def _monitor_workers(self):
        while not self._shutdown_event.wait(timeout=1.0):
            for i, worker in enumerate(self._workers):
                if not worker.is_alive():
                    print(f"[Pool Supervisor] 检测到 PID: {worker.pid} 挂了，重启中...")
                    new_worker = self._start_worker()
                    self._workers[i] = new_worker

    def apply_async(self, func, args=(), kwds={}):
        if self._shutdown_event.is_set():
            raise RuntimeError("pool closed")
        self._task_queue.put((func, args, kwds, self._max_retries))

    def wait_completion(self):
        self._task_queue.join()

    def close(self):
        if not self._shutdown_event.is_set():
            print("[Pool Supervisor] 正在关闭，发送停止信号...")
            self._shutdown_event.set()
            for _ in range(self._num_workers):
                self._task_queue.put(_STOP_SENTINEL)

    def join(self):
        for worker in self._workers:
            worker.join()
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
