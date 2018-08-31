# _*_coding:utf-8_*_

import pymysql
import requests
import time
import logging
import os


db_config = {
    'host': '47.74.247.130',
    'port': 3306,
    'user': 'root',
    'password': 'cuckoo787',
    'db': 'website_data_scraping',
    'charset': 'utf8mb4'
}

class ImageDownloader(object):
	"""docstring for ImageDownloader"""
	def __init__(self,table, floder, condition=None):
		# 创建or创建日志记录文件
		console = logging.FileHandler('./log_downloder.log')
		# 设置日志打印格式
		formatter = logging.Formatter('%(asctime)s-[%(name)s]-[%(funcName)s] - %(levelname)s - %(message)s')
		console.setFormatter(formatter)
		# 将定义好的console日志handler添加到日志文件
		self.pdlogger = logging.getLogger('imageDownload')
		self.pdlogger.setLevel(logging.INFO)
		self.pdlogger.addHandler(console)

		self.host = db_config['host']
		self.port = db_config['port']
		self.user = db_config['user']
		self.password = db_config['password']
		self.charset = db_config['charset']
		self.db = db_config['db']
		self.connect()

		self.table = table
		self.__columns = 'website_id, hash_url, id, product_image,local_image'
		self.condition = condition
		self.image_column = 'local_image'

		# 记录抓取失败的url,利用set不会重复添加请求失败的url
		self.wait_crawl = set()

		# 初始化文件夹
		self.floder_name = floder
		self.path = floder
		self.__mkdir()



	def connect(self):
		try:
			self.conn = pymysql.connect(host=self.host,user=self.user, password=self.password, port=self.port,db=self.db,charset=self.charset)
		except Exception as e:
			self.pdlogger.error(u'连接数据库失败'.format(e))


	def query_url(self):
		cursor = self.conn.cursor()
		if self.condition:
			self.condition = ' WHERE ' + self.condition
			sql = "SELECT {} FROM {} {}".format(self.__columns, self.table, self.condition) 
		else:
			sql = "SELECT {} FROM {}".format(self.__columns, self.table)
		try:
			cursor.execute(sql)
			print('Count: {}'.format(cursor.rowcount))
			row = cursor.fetchone()
			while row:
			    print('Row: {}'.format(row))
			    # 判断是否存在本地url，如果存在则不返回,作用1：在这里去重，保证不下载已经下载过的图片；作用2：避免在获取第一次下载失败图片的下标时获取到本地图片的路径而使图片命名出错
			    local_pic = None
			    try:
			    	local_pic = str(row[4])
			    except:
			    	pass
			    if local_pic is not "":
			    	row = cursor.fetchone()
			    	continue
			    yield row
			    row = cursor.fetchone()
		except Exception as e:
		    self.pdlogger.error(u'查询图片链接失败or查询完成'.format(e))
		cursor.close()


	def image_download(self, image_info):
		# 查询数据库的字符串类型是unicode
		website_id = str(image_info[0])
		hash_url = str(image_info[1])
		product_id = str(image_info[2])
		image_url = str(image_info[3])
		
		try:
			image_url = eval(image_url)
		except Exception as e:
			pass
		
		all_local_pic = ""
		if not isinstance(image_url, str):
			for url in image_url:
				index = str(image_url.index(url))
				local_image_url = self.__send_requset(url, website_id, hash_url, product_id, index)
				all_local_pic += local_image_url

		else:
			# 尝试获取失败任务的下标
			index = None
			try:
				index = str(image_info[4])
			except Exception as e:
				pass
			index = index if index else '0'

			all_local_pic = self.__send_requset(image_url, website_id, hash_url, product_id, index)

		# 修改数据库中local_image字段	
		all_local_pic = '"'+ all_local_pic + '"'	
		self.__update_local_image(all_local_pic, product_id)


	def __send_requset(self, link, website_id, hash_url, product_id, index):
		if not link.startswith('http'):
			link = 'https:' + link
		pic_url = None
		if '?' in link:
			name = link.split('?')[0]
		else:
			name =link
		try:
			response = requests.get(link)
			# print '响应状态码是%s'  % response.status_code
			pic_name = './'+self.path+website_id+'_'+hash_url+'_'+product_id+'_'+index +'.'+name.split('.')[-1]
			with open(pic_name, 'wb') as pic:
				pic.write(response.content)
			pic_url = os.path.abspath(pic_name)+', '
		except Exception as e:
			wait_crawl_info = (website_id,hash_url,product_id,link,index)
			# 将未请求成功的url加入列表
			self.wait_crawl.add(wait_crawl_info)
			self.pdlogger.error(u'图片请求失败，重新请求{}'.format(e))
		
		return pic_url if pic_url else ""


	def __update_local_image(self,value,id_num):
		cursor = self.conn.cursor()
		sql = "UPDATE %s SET %s = CONCAT(%s, %s) WHERE id = %s" % (self.table, self.image_column, self.image_column, value, id_num)
		try:
			result = cursor.execute(sql)
			self.conn.commit()
		except Exception as e:
			self.pdlogger.error(u'本地图片路径更新失败{}'.format(e))
		cursor.close()

	def __mkdir(self):
		flode = os.path.exists(self.floder_name)
		if not flode:
			os.makedirs(self.floder_name)
		

if __name__ == '__main__':
	downloader = ImageDownloader(table='website_thishop', floder='thishop_image/')
	info_list = downloader.query_url()
	for info in info_list:
		downloader.image_download(info)
