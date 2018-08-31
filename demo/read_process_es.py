from datetime import datetime, timedelta
import datetime as datetime_now
import urllib.parse
import os, math, hashlib
from django.utils.html import escape
from Es import Es
import multiprocessing

yesterday = datetime.today() + timedelta(-1)
# yesterday_format = yesterday.strftime('%Y.%m.%d')
yesterday_format = '2018.05.08'
# yesterday_format = '2018.05.07'
# yesterday_format = '2018.05.06'
# yesterday_format = '2018.05.05'
# yesterday_format = '2018.05.04'
# yesterday_format = '2018.05.03'
# yesterday_format = '2018.05.02'
# yesterday_format = '2018.05.01'
index_name = 'single-nginx-access-' + yesterday_format

base_dir = os.getcwd()
sp = '`'
es = Es(index_name)


def process_scroll(process_no, lists, page_rows):
    # file_name = os.path.join(base_dir, 'data', yesterday_format + '-' + str(process_no) + '.txt')
    file_name = os.path.join(base_dir, 'data', yesterday_format + '.txt')
    my_open = open(file_name, 'a')
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
                    my_open.close()
                    visit_time = ''
                sub_pos = user_agent.find('"', 2) if user_agent.find('"', 2) else len(user_agent)
                referer = data_list[11] if data_list[11] else ''
                short_url = urllib.parse.unquote_plus(data_list[2] + data_list[7])
                short_url = short_url.split('?')
                short_url = short_url[0] if short_url[0] else ''
                insert_data = {'ip': data_list[0],
                               'website_url': urllib.parse.unquote_plus(data_list[2] + data_list[7]),
                               'user_agent': (escape(user_agent[1:sub_pos])).replace('`', '+'),
                               'short_url': short_url,
                               'visit_time': visit_time,
                               'referer': referer[1:-1]}
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
                my_open.write(write_string + '\n')
            except Exception as e:
                print(e)
        my_open.close()
        return str(os.getpid()) + ' done '
    except Exception as e:
        print(e)
        my_open.close()


# now_time = datetime_now.datetime.now()
# for num in range(3,11):
#     yes_time = now_time + datetime_now.timedelta(days=-num)
#     print(yes_time)
#     yes_time_nyr = yes_time.strftime('%Y.%m.%d')
#     print(yes_time_nyr)

#exit()


size = 10000
data = es.query(scroll='2m', search_type='scan', size=size)
if not data:
    print('not data')
    exit()

log_rows = data['hits']['total']
if log_rows > 0 and data['_scroll_id']:
    index_counts = math.ceil(log_rows / 10000)
    scroll_id = data['_scroll_id']
    worker_num = 8
    print('exec read es data to file index : ' + str(index_counts) + ' es data size' + str(size) + 'done')
    while index_counts > 0:
        result = es.scroll_query(scroll='2m', scroll_id=scroll_id)
        if 'hits' not in result:
            break
        log_rows = len(result['hits']['hits'])
        lists = result['hits']['hits'];
        page_rows = math.ceil(log_rows / worker_num)
        pool = multiprocessing.Pool(worker_num)
        process_result = []
        for p in range(worker_num):
            process_result.append(pool.apply_async(process_scroll, (p, lists, page_rows)))
        pool.close()
        pool.join()
        index_counts -= 1
        size += 10000
        scroll_id = result['_scroll_id']

        print('exec read es data to file index : ' + str(index_counts) + ' es data size' + str(size) + 'done')
    else:
        print('count < 0')
        exit()
exit()
