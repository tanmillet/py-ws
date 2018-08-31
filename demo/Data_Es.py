from datetime import datetime, timedelta
import datetime as datetime_now
import urllib.parse
from Db import Db
import math, hashlib
from django.utils.html import escape
from Es import Es
import multiprocessing
import os


# `

def process_scroll(process_no, lists, page_rows):
    dbconfig = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': 'bgn123',
        'db': 'tracking',
        'charset': 'utf8'
    }
    db_class = Db(dbconfig)
    try:
        for line in lists[process_no * page_rows:(process_no + 1) * page_rows]:
            try:
                if '_source' not in line:
                    continue
                data = line['_source']['message']
                data_list = data.split(' ')
                if not data_list:
                    continue
                if not isinstance(data_list, list):
                    continue
                user_agent = ''
                for index in range(12, len(data_list)):
                    user_agent = user_agent + (' ' if user_agent else '') + data_list[index]
                try:
                    visit_time = str(datetime_now.datetime.strptime(data_list[4][1:], "%d/%b/%Y:%H:%M:%S"))
                except Exception as e:
                    print(e)
                    visit_time = ''
                sub_pos = user_agent.find('"', 2) if user_agent.find('"', 2) else len(user_agent)
                referer = data_list[11] if data_list[11] else ''
                short_url = urllib.parse.unquote_plus(data_list[2] + data_list[7])
                short_url = short_url.split('?')
                short_url = short_url[0] if short_url[0] else ''
                insert_data = {'ip': data_list[0],
                               'website_url': urllib.parse.unquote_plus(data_list[2] + data_list[7]),
                               'user_agent': escape(user_agent[1:sub_pos]), 'visit_time': visit_time,
                               'short_url': short_url,
                               'referer': referer[1:-1]}
                insert_data['created_time'] = datetime_now.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                insert_data['md5_hash'] = hashlib.md5(
                    (insert_data['ip'] + insert_data['user_agent'][0:255]).encode(
                        'utf8')).hexdigest()  # 截取255个字符进行hash
                insert_data['unique_hash'] = hashlib.md5(
                    (insert_data['ip'] + insert_data['user_agent'][0:255] + short_url).encode('utf8')).hexdigest()
                table_name = 'statistics_data'
                print(insert_data)
                id = db_class.insert(table_name, insert_data)
                print(id)
            except Exception as e:
                print(e)
        return str(os.getpid()) + ' done '
    except Exception as e:
        print(e)


yesterday = datetime.today() + timedelta(-1)
yesterday_format = yesterday.strftime('%Y.%m.%d')
index_name = 'single-nginx-access-' + yesterday_format

es = Es(index_name)

# scroll方式分页
size = 1000
data = es.query(scroll='2m', search_type='scan', size=size)  # size 每页条数

if not data:
    print('not data')
    exit()

log_rows = data['hits']['total']

if log_rows > 0 and data['_scroll_id']:
    result = es.scroll_query(scroll='2m', scroll_id=data['_scroll_id'])
    worker_num = 8
    while result:
        if 'hits' not in result:
            break
        log_rows = len(result['hits']['hits'])
        print(log_rows)
        lists = result['hits']['hits'];
        page_rows = math.ceil(log_rows / worker_num)
        pool = multiprocessing.Pool(worker_num)
        process_result = []
        for p in range(worker_num):
            process_result.append(pool.apply_async(process_scroll, (p, lists, page_rows)))
        pool.close()
        pool.join()
        size = size + 1000
        print(str(size) + 'done')
        print(result['_scroll_id'])
        result = es.scroll_query(scroll='2m', scroll_id=result['_scroll_id'])
    exit()