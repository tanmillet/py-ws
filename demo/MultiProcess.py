import multiprocessing
import os
import time

class MultiProcess:
    worker_num = 8
    class_name = object
    func_name = None

    def __init__(self, class_name=object, func_name='func', worker_num=4):
        self.worker_num = worker_num
        self.class_name = class_name
        self.func_name = func_name

    def run_log(self, lists, page_rows):
        pool = multiprocessing.Pool(processes=self.worker_num)
        process_result = []
        func_name = self.func_name
        for p in range(self.worker_num):
            process_result.append(pool.apply_async(func_name, (p, page_rows, lists)))
        pool.close()
        pool.join()
        for result in process_result:
            print(result.get())

    def run_es(self, lists, page_rows):
        pool = multiprocessing.Pool(processes=self.worker_num)
        process_result = []
        func_name = self.func_name
        for p in range(self.worker_num):
            process_result.append(pool.apply_async(func_name, (p, lists, page_rows)))
        pool.close()
        pool.join()
        for result in process_result:
            print(result.get())


def func(self):
    f = open('a.log', 'a+')
    f.write(str(time.time()) + '|pid:' + str(os.getpid()) + '\r\n')
    return os.getpid()
