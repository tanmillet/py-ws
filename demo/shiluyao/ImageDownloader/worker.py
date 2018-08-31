# -*- coding:utf-8 -*-

#################分布式爬虫imageDownloader，worker###########################

import time, sys, Queue
from multiprocessing.managers import BaseManager
from multiprocessing import Process, cpu_count
import logging

import imageDownload

# 创建类似的QueueManager:
class QueueManager(BaseManager):
    pass


def start_client():
    # 创建或打开日志文件
    console = logging.FileHandler('./log_downloader.log')
    # 设置日志打印格式
    formatter = logging.Formatter('%(asctime)s -[%(name)s] - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    # 将定义好的console日志handler添加到日志文件
    logger = logging.getLogger('client')
    logger.setLevel(logging.INFO)
    logger.addHandler(console)

    # 由于这个QueueManager只从网络上获取Queue，所以注册时只提供名字:
    QueueManager.register('get_task_queue')
    QueueManager.register('get_result_queue')

    # 连接到服务器，也就是运行taskmanager.py的机器:
    server_addr = '127.0.0.1'
    logger.info('Connect to server %s...' % server_addr)
    # 端口和验证码注意保持与taskmanager.py设置的完全一致:
    m = QueueManager(address=(server_addr, 5000), authkey='cuckoo')
    # 从网络连接:
    m.connect()
    # 获取Queue的对象:
    task = m.get_task_queue()
    result = m.get_result_queue()

    spider1 = imageDownload.ImageDownloader(table="website_shein", floder='image/')

    # 从task队列取任务,并把结果写入result队列:
    while True:
        try:
            n = task.get()
            logger.info(u'开始下载图片 %s...' % n[2])
            spider1.image_download(n)
            r = '{}图片下载完毕'.format(n[2])
            result.put(r)
        except Queue.Empty:
            logger.info('task queue is empty.')
            break

if __name__ == '__main__':
    #启动进程做这个事情
    for n in range(cpu_count()):
        p1 = Process(target=start_client)
        p1.start()