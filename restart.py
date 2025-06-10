# restart.py
import subprocess
import sys
import time

def restart_program():
    """函数用于重新启动程序"""
    print("正在重新启动程序...")
    time.sleep(8)  # 可以设置延迟，确保程序有足够的时间退出
    python = sys.executable
    subprocess.Popen([python] + ['图吧批量地址获取.py'])  # 重新启动 main.py

if __name__ == "__main__":
    restart_program()
