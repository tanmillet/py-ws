import requests
import re
from bs4 import BeautifulSoup
import datetime
from Db import Db
import time
from random import randint
from tmcate import tmcate

initial_url = 'https://list.tmall.com/search_product.htm?spm=a211ko.11357265-cn.5884710685.2.62ea42effNjLKm&q=%CC%EC%C3%A8%C8%AB%C7%F2%B9%D9%B7%BD%B5%EA&closedKey=zyd&style=g&from=zyd.list.pc_1_searchbutton&acm=lb-zebra-345600-3889893.1003.4.3411413&type=p&scm=1003.4.lb-zebra-345600-3889893.OTHER_15240739755092_3411413'

baseurl = 'https://list.tmall.com/search_product.htm?'
sort = ['s','rq','new','d']
sort2id = {'s': '0', 'rq': '7', 'new': '2', 'd': '5'}

dbconfig = {
    'host': '47.74.247.130',
    'port': 3306,
    'user': 'root',
    'password': 'cuckoo787',
    'db': 'website_data_scraping',
    'charset': 'utf8mb4'
}

cookies = {'Cookie': 'hng=""; uc1=cookie14=UoTfK7MwBo3gIQ%3D%3D&lng=zh_CN&cookie16=WqG3DMC9UpAPBHGz5QBErFxlCA%3D%3D&existShop=false&cookie21=VT5L2FSpccLuJBreK%2BBd&tag=8&cookie15=U%2BGCWk%2F75gdr5Q%3D%3D&pas=0; uc3=nk2=tMvPb7XxyUsq%2B18VWJY%3D&id2=UUpksCe29owkSg%3D%3D&vt3=F8dBzr7Bozd72WLCW9k%3D&lg2=VFC%2FuZ9ayeYq2g%3D%3D; tracknick=%5Cu81EA%5Cu7531%5Cu843D%5Cu4F53%5Cu7684%5Cu732A93; _l_g_=Ug%3D%3D; ck1=""; unb=2258011093; lgc=%5Cu81EA%5Cu7531%5Cu843D%5Cu4F53%5Cu7684%5Cu732A93; cookie1=BxT5Z0NaHfT4Mrk14I%2FK08eOOv2oM48otXIRACaREkg%3D; login=true; cookie17=UUpksCe29owkSg%3D%3D; cookie2=1dcbf7514f0e44395514ba8b454a87e1; _nk_=%5Cu81EA%5Cu7531%5Cu843D%5Cu4F53%5Cu7684%5Cu732A93; t=cbaaac8489fff04f3ccd45701282a7a8; uss=""; csg=bb24c238; skt=eba575c99a9410ea; _tb_token_=75f9561e3dbe6; cna=CdefElk21yoCAXBfh3IqPivY; _m_h5_tk=a9dbe63e547581ccd3d4016a0e4e28da_1530003243548; _m_h5_tk_enc=1f2d9f15864717ea81ae535c58020b82; isg=BL29TnwLuu2eOB6QQjW_2TqhzBl38i1Wm1T14n8C1ZRHtt3oRqoBfItMZKpVNglk'}

def get_one_page(url):
    try:
        html = requests.get(url, cookies=cookies).text
        print(html)
        return html
    except Exception as e:
        print(e)
        return None


def parse_home_page(page):
    pattern = re.compile('data-c="cat".*?href="\?(.*?)"', re.S)
    urllist = re.findall(pattern, page)
    urllist = [url.replace('&amp;','&').replace('sort=s','') for url in urllist][1:]
    return urllist


def parse_one_page(item, category, sort_nums, sortid):
    if item:
        product_name = item('p', 'productTitle')[0].a['title']
        try:
            sale_price = str(re.findall(r"\d+\.?\d*", item.p.em.text)[0])
        except:
            sale_price =  item('p','productPrice')[0].em['title']
        try:
            sale_num = str(re.findall(r"\d+\.?\d*", item.span.em.string)[0])
        except:
            sale_num = re.findall(r"\d+\.?\d*", item('p', 'productStatus')[0].span.em.string)[0]
        comment_count = str(re.findall(r"\d+\.?\d*", item.find_all('a')[-1].text)[0])
        product_url = item('p', 'productTitle')[0].a['href']
        try:
            product_image = item('a', 'productImg')[0].img['src']
        except:
            product_image = item('a', 'productImg')[0].img['data-ks-lazyload']
    #   category_id = re.findall(re.compile(r"\d+\.?\d*"), item['data-atp'])[0]
        category_id = tmcate[category]
        website_id = '13'
        sort_type = sortid
        sort_num = str(sort_nums)
        goods_sn = item['data-id']
        created_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = {
            'product_name': product_name, 'sale_price': sale_price, 'sale_num': sale_num,
            'comment_count': comment_count, 'product_url': product_url,
            'product_image': product_image, 'category_id': category_id,
            'website_id': website_id, 'sort_type': sort_type, 'sort_num': sort_num,
            'goods_sn': goods_sn, 'created_time': created_time}
        return result
    else:
        category_id = tmcate[category]
        sort_type = sortid
        sort_num = str(sort_nums)
        created_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result = {
            'product_name': 'nan', 'sale_price': '0', 'sale_num': '0',
            'comment_count': '0', 'product_url': 'nan',
            'product_image': 'nan', 'category_id': category_id,
            'website_id': '13', 'sort_type': sort_type, 'sort_num': sort_num,
            'goods_sn': 'nan', 'created_time': created_time}
        return result

def main():
    db = Db(dbconfig)
    print(initial_url)
    home_page = get_one_page(initial_url)
    urllist = parse_home_page(home_page)
    print(urllist)

    for url in urllist:
        category = re.findall(r'cat=(.*?)&', url)[0]
        for sorttype in sort:
            sortid = sort2id[sorttype]
            sort_nums = 0
            for i in range(10):
                # realurl = '{0}sort={1}&s={2}&{3}'.format(baseurl, sorttype, 60*i, tailurl)
                realurl = '{0}sort={1}&s={2}&{3}'.format(baseurl, sorttype, i*60, url)
                print(realurl)
                itempage = get_one_page(realurl)
#                print(itempage)
                soup = BeautifulSoup(itempage, 'lxml')
                itemlist = soup.findAll(name="div", attrs={"class":re.compile(r"product (\s\w+)?")})
#                print(itemlist)
                
                print(sort_nums)
                for item in itemlist:
#                    print(item)
                    result = parse_one_page(item, category, sort_nums, sortid)
                    db.insert('website_tmall_inter', result)
                    sort_nums = sort_nums + 1
                time.sleep(randint(25, 45))
                if len(itemlist) < 60:
                    break
main()
