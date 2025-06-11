import multiprocessing
import os
from app import task, output_dir, done_log_dir
from app import ResilientPool


if __name__ == '__main__':
    import random
    multiprocessing.set_start_method('spawn', force=True)

    dirs = [
        "/home/cells/dev/py-project/proxy/test",
    ]

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(done_log_dir, exist_ok=True)

    with ResilientPool(3) as pool:
        for dir in dirs:
            if not dir:
                continue
            for root, dirs, files in os.walk(dir):
                for file in files:
                    print(file)
                    file_path = os.path.join(root, file)
                    pool.apply_async(task, args=(file_path,))
            
        print(f"all task commit")
