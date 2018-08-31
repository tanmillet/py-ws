from mysqlpool import ConPool
from datetime import date, datetime
from publicConfig import BasicConfig as cfg

class GetFacebook():

    def __init__(self, dbconfig):
        self.pool = ConPool(dbconfig)
        self.yesterday, self.today, self.time_range = cfg.get_time_range()
        '''
        self.ad_basic = tables['ad_basic']
        self.ad_stat = tables['ad_basic']
        self.ad_set_basic = tables['ad_set_basic']
        self.ad_set_stat = tables['ad_set_stat']
        self.campaign_basic = tables['campaign_basic']
        self.campaign_stat = tables['campaign_stat']
        '''

    def write_log(self, start_time=None, end_time=None):
        if start_time:
            self.pool.update(
                'ams_batch_info',
                {'valid_flag': '0'},
                'valid_flag=1')
            item = dict()
            item['stat_dt'] = str(date.today())
            item['batch_tms'] = str(datetime.now().hour)
            item['valid_flag'] = str(1)
            item['start_time'] = start_time
            self.pool.update('ams_batch_info', item)
        elif end_time:
            self.pool.update(
                'ams_batch_info',
                {'end_time': end_time},
                'valid_flag=1')
        else:
            print('请设置开始时间或结束时间')

    def get_active_ads(self):
        result = self.pool.fetch_all(
            table=self.ad_basic,
            column='facebook_adset_id',
            condition='''
                status="ACTIVE" and effective_status="ACTIVE"
                or created_time >= date_sub(CURRENT_TIMESTAMP, interval 1 hour)
                ''')
        return result

    def get_active_campaigns(self):
        result = self.pool.fetch_all(
            table='ams_ad_compaign',
            column='facebook_ad_campaign_id',
            condition='''
                status="ACTIVE" and effective_status="ACTIVE"
                or created_time >= date_sub(CURRENT_TIMESTAMP, interval 1 hour)
                ''')
        return result

    def get_active_adsets(self):
        result = self.pool.fetch_all(
            table='ams_ad_set',
            column='facebook_adset_id',
            condition='''
                status="ACTIVE" and effective_status="ACTIVE"
                or created_time >= date_sub(CURRENT_TIMESTAMP, interval 2 hour)
                ''')
        return result

    def delete_from_ad_stat(self, ad_id):
        condition_fmt = 'stat_dt="{0}" and facebook_ad_id={1}'
        try:
            self.pool.delete(
                'facebook_ad', condition_fmt.format(self.yesterday, ad_id))
        except Exception as e:
            error_info = '删除广告统计数据时发生错误:{}'.format(e)
            print(error_info)

    def delete_from_campaign_stat(self, campaign_id):
        condition_fmt = 'stat_dt="{0}" and ad_compaign_id={1}'
        try:
            self.pool.delete(
                'facebook_ad_compaign_stat_h',
                condition_fmt.format(self.yesterday, campaign_id))
        except Exception as e:
            error_info = '删除广告统计系列数据时发生错误:{}'.format(e)
            print(error_info)

    def delete_from_adset_stat(self, adset_id):
        condition_fmt = 'stat_dt="{0}" and ad_set_id={1}'
        try:
            self.pool.delete(
                'facebook_ad_set',
                condition_fmt.format(self.yesterday, adset_id))
        except Exception as e:
            error_info = '删除广告组统计数据时发生错误:{}'.format(e)
            print(error_info)

    def replace_into_ad_basic(self, item):
        try:
            self.pool.replace('facebook_ad', item)
        except Exception as e:
            error_info = '替换广告基础数据时发生错误:{}'.format(e)
            print(error_info)

    def replace_into_adset_basic(self, item):
        try:
            self.pool.replace('facebook_ad_set', item)
        except Exception as e:
            error_info = '替换广告组基础数据时发生错误:{}'.format(e)
            print(error_info)

    def replace_into_campaign_basic(self, item):
        try:
            self.pool.replace('facebook_ad_compaign', item)
        except Exception as e:
            error_info = '替换广告系列基础数据时发生错误:{}'.format(e)
            print(error_info)

    def replace_into_adcreative(self, item):
        try:
            self.pool.replace('ams_ad_creative', item)
        except Exception as e:
            error_info = '替换广告创意数据时发生错误:{}'.format(e)
            print(error_info)

    def insert_into_adstat(self, item):
        try:
            self.pool.insert('facebook_ad_stat_h', item)
        except Exception as e:
            error_info = '插入广告统计数据时发生错误:{}'.format(e)
            print(error_info)

    def insert_into_adsetstat(self, item):
        try:
            self.pool.insert('facebook_ad_set_stat_h', item)
        except Exception as e:
            error_info = '插入广告组统计数据时发生错误:{}'.format(e)
            print(error_info)

    def insert_into_campaignstat(self, item):
        try:
            self.pool.insert('facebook_ad_compaign_stat_h', item)
        except Exception as e:
            error_info = '插入广告系列统计数据时发生错误:{}'.format(e)
            print(error_info)

    def __del__(self):
        try:
            self.pool.close()
        except Exception as e:
            print('连接池未正常关闭原因：{}'.format(e))

    def __enter__():
        pass

    def __exit__():
        pass
