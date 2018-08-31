import time
import datetime
import urllib.parse
from Db import Db
from MultiProcess import MultiProcess
import os, math, hashlib
from django.utils.html import escape


class filterData:
    @staticmethod
    def getLogFile(file_path):
        return open(file_path, 'r')

    @staticmethod
    def getLogRows(file_source):
        return len(file_source.readlines())

    def process(self, page_rows, lists):
        process_no = self
        dbconfig = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'password': 'buguniao.com',
            'db': 'tracking',
            'charset': 'utf8'
        }
        db_class = Db(dbconfig)
        for line in lists[process_no * page_rows:(process_no + 1) * page_rows]:
            data_list = line.split(' ')
            if not data_list:
                continue
            user_agent = ''
            for index in range(12, len(data_list)):
                user_agent = user_agent + (' ' if user_agent else '') + data_list[index]
            try:
                visit_time = str(datetime.datetime.strptime(data_list[4][1:], "%d/%b/%Y:%H:%M:%S"))
            except Exception as e:
                print(e)
                visit_time = ''
            sub_pos = user_agent.find('"', 2) if user_agent.find('"', 2) else len(user_agent)
            referer = data_list[11] if data_list[11] else ''
            insert_data = {'ip': data_list[0],
                           'website_url': urllib.parse.unquote_plus(data_list[2] + data_list[7]),
                           'user_agent': escape(user_agent[1:sub_pos]), 'visit_time': visit_time,
                           'referer': referer[1:-1]}
            insert_data['created_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_data['md5_hash'] = hashlib.md5(
                (insert_data['ip'] + insert_data['user_agent'][0:255]).encode('utf8')).hexdigest()  # 截取255个字符进行hash
            table_name = 'statistics_data'
            print(insert_data)
            id = db_class.insert(table_name, insert_data)
            print(id)
        return str(os.getpid()) + ' done '


file_name = '1.log'
log_file = open(file_name, 'r')
lists = log_file.readlines()
log_rows = len(lists)

if log_rows > 0:
    worker_num = 8
    page_rows = math.ceil(log_rows / worker_num)
    fd = filterData()
    mp = MultiProcess(fd, filterData.process, worker_num)
    mp.run_log(lists, page_rows)
