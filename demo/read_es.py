from datetime import datetime, timedelta
import datetime as datetime_now
import urllib.parse
import os, math, hashlib
from django.utils.html import escape
from Es import Es
import multiprocessing

yesterday = datetime.today() + timedelta(-1)
yesterday_format = yesterday.strftime('%Y.%m.%d')
index_name = 'single-nginx-access-' + yesterday_format
base_dir = os.getcwd()
file_name = os.path.join(base_dir, 'data', yesterday_format + '.txt')
sp = '`'


def r_es_to_file(my_open, es, scroll_id):
    result = es.scroll_query(scroll='2m', scroll_id=scroll_id)
    if '_scroll_id' not in result:
        print('not _scroll_id')
        exit()
    scroll_id = result['_scroll_id']
    if 'hits' not in result:
        print('not result')
        exit()
    result_lists = result['hits']['hits']
    for rs in result_lists:
        if '_source' not in rs:
            continue
        data = rs['_source']['message']
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
        try:
            sub_pos = user_agent.find('"', 2) if user_agent.find('"', 2) else len(user_agent)
            referer = data_list[11] if data_list[11] else ''
            short_url = urllib.parse.unquote_plus(data_list[2] + data_list[7])
            short_url = short_url.split('?')
            short_url = short_url[0] if short_url[0] else ''
            insert_data = {'ip': data_list[0], 'website_url': urllib.parse.unquote_plus(data_list[2] + data_list[7]),
                           'user_agent': (escape(user_agent[1:sub_pos])).replace('`', '+'), 'short_url': short_url,
                           'visit_time': visit_time, 'referer': referer[1:-1]}
            insert_data['created_time'] = datetime_now.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            insert_data['md5_hash'] = hashlib.md5(
                (insert_data['ip'] + insert_data['user_agent'][0:255]).encode(
                    'utf8')).hexdigest()  # 截取255个字符进行hash
            insert_data['unique_hash'] = hashlib.md5(
                (insert_data['ip'] + insert_data['user_agent'][0:255] + short_url).encode('utf8')).hexdigest()
            write_string = insert_data['ip'] + sp + insert_data['website_url'] + sp + insert_data[
                'user_agent'] + sp + insert_data['short_url'] + sp + insert_data['visit_time'] + sp + insert_data[
                               'referer'] + sp + insert_data['created_time'] + sp + insert_data['md5_hash'] + sp + \
                           insert_data['unique_hash']
            print(write_string)
            my_open.write(write_string + '\n')
        except Exception as e:
            print(e)
            exit()
    return scroll_id


def lookup_es_data():
    es = Es(index_name)
    size = 5000
    data = es.query(scroll='2m', search_type='scan', size=size)

    if not data:
        print('not data')
        exit()

    log_rows = data['hits']['total']
    index_counts = math.ceil(log_rows / 1000000)

    if log_rows > 0 and data['_scroll_id']:
        my_open = open(file_name, 'a')
        scroll_id = data['_scroll_id']
        while index_counts > 0:
            scroll_id = r_es_to_file(my_open, es, scroll_id)
            index_counts -= 1
            print('exec read es data to file index : ' + str(index_counts))
        else:
            print('count < 0')
            my_open.close()
            exit()
        my_open.close()
    exit()


lookup_es_data()
