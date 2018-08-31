# -*- coding:utf-8 -*-

#################分布式爬虫imageDownloader，manager###########################

import imageDownload
import threading
import random, time, Queue,logging
# import dill
from multiprocessing.managers import BaseManager

# 创建或打开日志文件
console = logging.FileHandler('./log_downloader.log')
# 设置日志打印格式
formatter = logging.Formatter('%(asctime)s -[%(threadName)s] - %(levelname)s - %(message)s')
console.setFormatter(formatter)
# 将定义好的console日志handler添加到日志文件
logger = logging.getLogger('manager')
logger.setLevel(logging.INFO)
logger.addHandler(console)


# 从BaseManager继承的QueueManager:
class QueueManager(BaseManager):
    pass


def make_server_manager():

    # 发送任务的队列:
    task_queue = Queue.Queue(10000)
    # 接收结果的队列:
    result_queue = Queue.Queue()

    # 把两个Queue都注册到网络上, callable参数关联了Queue对象:
    QueueManager.register('get_task_queue', callable=lambda: task_queue)
    QueueManager.register('get_result_queue', callable=lambda: result_queue)
    # 绑定端口5000, 设置验证码:
    manager = QueueManager(address=('127.0.0.1', 5000), authkey='cuckoo')
    # 启动Queue:
    manager.start()
    t1 = threading.Thread(target=startTask, name='TaskQueue',args=(manager,))
    t2 = threading.Thread(target=startresultQueue, name='ResultQueue',args=(manager,))
    t1.start()
    t2.start()
    # t1.join()
    # t2.join()

def startTask(manager):

    # 初始化爬虫
    spider1 = imageDownload.ImageDownloader(table='website_shein', floder='image/')
    # 获得通过网络访问的Queue对象:
    task = manager.get_task_queue()
    info_list = spider1.query_url()
    for v in info_list:
        logger.info(u'下载任务放入队列 %s...' % str(v[3]))
        task.put(v)
    for v in spider1.wait_crawl:
        logger.info(u'失败的下载任务放入队列 %s...' % str(v[3]))
        task.put(v)

        
# 从result队列读取结果:
def startresultQueue(manager):
    result = manager.get_result_queue()
    # logger.info(u'尝试获取下次结果...')
    while True:
        try:
            r = result.get(timeout=10)
            logger.info(u'结果: %s' % r)
        except Queue.Empty:
            logger.warning('result queue is empty.')
            # break
    # 关闭:
    manager.shutdown()

if __name__=='__main__':
    make_server_manager()