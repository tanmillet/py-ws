from lxml import etree
import requests
from Db import Db
import pymysql
import time

dbconfig = {
    'host': '47.74.247.130',
    'port': 3306,
    'user': 'root',
    'password': 'cuckoo787',
    'db': 'website_data_scraping',
    'charset': 'utf8mb4'
}

sql = 'select product_url from website_lazadatb where day(created_time) = 13'


def get_urls(sql, dbconfig):
    connection = pymysql.connect(**dbconfig)
    cur = connection.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    connection.close()
    return result


def parse_page(page):
    try:
        epage = etree.HTML(page)
        result = {}
        try:
            result['brand_name'] = epage.xpath('//a[@class="pdp-link pdp-link_size_s pdp-link_theme_blue pdp-product-brand__brand-link"]/text()')[0]
        except:
            result['brand_name'] = ''
        try:
            result['brand_url'] = epage.xpath('//a[@class="pdp-link pdp-link_size_s pdp-link_theme_blue pdp-product-brand__brand-link"]/@href')[0]
        except:
            result['brand_url'] = ''
        try:
            result['rating'] = epage.xpath('//div[@class="score"]//text()')[0]
        except:
            result['rating'] = ''
        try:
            result['rating_num'] = epage.xpath('//a[@class="pdp-link pdp-link_size_s pdp-link_theme_blue pdp-review-summary__link"]/text()')[0]
        except:
            result['rating_num'] = ''
        return result
    except Exception as e:
        pass


def get_page(url):
    try:
        page = requests.get(url).text
        return page
    except:
        return None


def main():

    db = Db(dbconfig)
    urls = get_urls(sql, dbconfig)
    for url in urls:
        url = url[0]
        urlnew = 'https:' + url
        page = get_page(urlnew)
        item = parse_page(page)
        print(item)
        db.update('website_lazadatb', item, 'product_url="{0}"'.format(url))
        time.sleep(2)
    db.close()

main()

'''
def main():
    db = Db(dbcongfig)
    urls = 
    old_urls = set(old_urls)
    for url in urls:
        if url not in old_urls:
            page = get(url)
            item = parse_page(page)
            db.update('tb', item)
        else:
            item = db.query
'''