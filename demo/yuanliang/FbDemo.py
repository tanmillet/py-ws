from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adcreative import AdCreative
from datetime import datetime
import datetime
from Db import Db

my_app_id = '1442954805800139'
my_app_secret = '147ef52da9160910fbc50f247e36ea45'
my_access_token = 'EAAUgXBigbMsBAMM7BpKUpFJJoDs7bMlZCadD76\
    exvwsAUchqbWP4WJRZAQODWuVtMvoDKyX4UFIo1gIreTjkVqM0VLjq\
    gtBMSIQcEbOSl0rOVOYLHUxi25aFzbkdaXEyCZC4q4ZBIyCvOaeLcO\
    VFv6m73blNZAhbx81vZCsDQw9wZDZD'
proxies = {'https': 'https://127.0.0.1:1080', 'http': 'http://127.0.0.1:1080'}
dbconfig = {'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'password': '',
            'db': 'website_data_scraping',
            'charset': 'utf8mb4'}

yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

today = str(datetime.date.today())

tomorrow = str(datetime.date.today() + datetime.timedelta(days=1))
accounts = ['act_1624101030999583']
yesterday = str(datetime.date.today() - datetime.timedelta(days=1))
today = str(datetime.date.today())

# 获取账号下所有广告系列的信息
def fetch_campaigns(adaccount):
    campaigns = adaccount.get_campaigns(fields=[
        Campaign.Field.name,
        Campaign.Field.account_id,
        Campaign.Field.id,
        Campaign.Field.status,
        Campaign.Field.objective,
        Campaign.Field.effective_status,
        Campaign.Field.created_time,
        Campaign.Field.updated_time])
    for campaign in campaigns:
        result = {}
        result['name'] = campaign['name']
        result['facebook_account_id'] = campaign['account_id']
        result['facebook_ad_campaign_id'] = campaign['id']
        result['status'] = campaign['status']
        result['objective'] = campaign['objective']
        result['effective_status'] = campaign['effective_status']
        result['campaign_created_time'] = campaign['created_time']
        result['updated_time'] = campaign['updated_time']
        result['created_time'] =\
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield result


# 获取账号下所有广告组的信息
def fetch_adsets(adaccount):
    adsets = adaccount.get_ad_sets(fields=[
        AdSet.Field.name,
        AdSet.Field.account_id,
        AdSet.Field.id,
        AdSet.Field.campaign_id,
        AdSet.Field.status,
        AdSet.Field.effective_status,
        AdSet.Field.created_time,
        AdSet.Field.pacing_type,
        AdSet.Field.start_time,
        AdSet.Field.end_time,
        AdSet.Field.bid_info,
        AdSet.Field.daily_budget,
        AdSet.Field.billing_event,
        AdSet.Field.attribution_spec,
        AdSet.Field.targeting,
        AdSet.Field.optimization_goal,
        AdSet.Field.updated_time])
    for adset in adsets:
        # adset = dict(adset[0])
        result = {}
        result['name'] = adset['name'].replace("'", '\"') if\
            'name' in adset else ''
        result['facebook_account_id'] = adset['account_id'].\
            replace("'", "\'") if 'account_id' in adset else ''
        result['facebook_campaign_id'] = adset['campaign_id'].\
            replace("'", '\"') if 'campaign_id' in adset else ''
        result['facebook_adset_id'] = adset['id'].\
            replace("'", '\"') if 'id' in adset else ''
        result['status'] = adset['status'] if 'status' in adset else ''
        result['adset_created_time'] = adset['created_time'] if\
            'created_time' in adset else ''
        result['updated_time'] = adset['updated_time'] if\
            'updated_time' in adset else ''
        result['effective_status'] = adset['effective_status'].\
            replace("'", '\"') if 'effective_status' in adset else ''
        result['daily_budget'] = adset['daily_budget'].replace("'", '\"') if\
            'daily_budget' in adset else ''
        result['pacing_type'] = str(adset['pacing_type']).\
            replace("'", '\"') if 'pacing_type' in adset else ''
        result['start_time'] = adset['start_time'].replace("'", '\"') if\
            'start_time' in adset else ''
        result['end_time'] = adset['end_time'] if 'end_time' in adset else ''
        result['bid_info'] = adset['bid_info'].\
            replace("'", '\"') if 'bid_info' in adset else ''
        result['billing_event'] = adset['billing_event'].replace("'", '\"') if\
            'billing_event' in adset else ''
        result['attribution_spec'] = str(adset['attribution_spec']).\
            replace("'", '\"') if 'attribution_spec' in adset else ''
#       result['targeting'] = str(adset['targeting']) if\
#            'targeting' in adset else ''
        result['optimization_goal'] = str(adset['optimization_goal']).\
            replace("'", '\"') if 'optimization_goal' in adset else ''
        result['created_time'] =\
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield result


# 获取账号下所有广告的基本信息
def fetch_ads(adaccount):
    ads = adaccount.get_ads(fields=[
        Ad.Field.name,
        Ad.Field.account_id,
        Ad.Field.id,
        Ad.Field.adset_id,
        Ad.Field.status,
        Ad.Field.effective_status,
        Ad.Field.created_time,
        Ad.Field.updated_time])
    for ad in ads:
        # ad = dict(ad[0])
        ad_ins = Ad(ad['id'])
        ad_creative = fetch_ad_creative(ad_ins)
        ad_creative = dict(ad_creative[0])
        result = {}
        result['name'] = ad['name'] if 'name' in ad else ''
        result['facebook_account_id'] = ad['account_id'] if\
            'account_id' in ad else ''
        result['facebook_ad_id'] = ad['id'] if 'id' in ad else ''
        result['facebook_adset_id'] = ad['adset_id'] if\
            'adset_id' in ad else ''
        result['status'] = ad['status'] if 'status' in ad else ''
        result['effective_status'] = ad['effective_status'] if\
            'effective_status' in ad else ''
        result['name'] = ad['name'] if 'name' in ad else ''
        result['ad_created_time'] = ad['created_time'] if\
            'created_time' in ad else ''
        result['updated_time'] = ad['updated_time'] if\
            'updated_time' in ad else ''
        result['created_time'] =\
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result['thumbnail'] = ad_creative['thumbnail_url'] if\
            'thumbnail_url' in ad_creative else ''
        result['url_tags'] = ad_creative['url_tags'] if\
            'url_tags' in ad_creative else ''
        # result['video_id'] = ad_creative['video_id'] if\
        #    'video_id' in ad_creative else ''
        result['big_image'] = ad_creative['image_url'] if\
            'image_url' in ad_creative else ''
        result['title'] = ad_creative['title'] if\
            'title' in ad_creative else ''
        try:
            result['destination'] = ad_creative['object_story_spec']['video_data']['call_to_action']['value']['link']
            result['short_destination'] = ad_creative['object_story_spec']['video_data']['call_to_action']['value']['link_caption']
        except Exception as e:
            result['destination'] = ''
            result['short_destination'] = ''
#            print(result)
        yield result


def fetch_ad_creative(ad):
    ad_creative = ad.get_ad_creatives(fields=[
        AdCreative.Field.thumbnail_url,
        AdCreative.Field.url_tags,
        AdCreative.Field.video_id,
        AdCreative.Field.object_story_spec,
        AdCreative.Field.title,
        AdCreative.Field.image_url])
    return ad_creative


# 获取广告统计信息
def fetch_ad_data(ad_id):
    ad = Ad(ad_id)
    ad_datas = ad.get_insights(fields=[
        AdsInsights.Field.ad_id,
        AdsInsights.Field.spend,
        AdsInsights.Field.impressions,
        AdsInsights.Field.reach,
        AdsInsights.Field.unique_actions,
        AdsInsights.Field.ad_name],
        params={
        'breakdowns': ['hourly_stats_aggregated_by_audience_time_zone'],
        'time_range': {'since': yesterday, 'until': today}
    })
    for ad_data in ad_datas:
        print(ad_data)
        result = {}
        result['spend'] = ad_data['spend'] if 'spend' in ad_data else ''
        result['impression_count'] = ad_data['impressions'] if\
            'impressions' in ad_data else ''
        result['reach_count'] = ad_data['reach'] if 'reach' in ad_data else ''
        result['link_click_count'] = ad_data['link_click'] if\
            'link_click_count' in ad_data else ''
        result['ad_id'] = ad_data['ad_id'] if 'ad_id' in ad_data else ''
        result['stat_dt'] = ad_data['date_start']
        print(result['stat_dt'])
        result['stat_hour'] = \
            str(int(ad_data['hourly_stats_aggregated_by_audience_time_zone'][0:2]) + 1)
        try:
            for actions in ad_data['unique_actions']:
                if actions['action_type'] == 'link_click':
                    result['link_click_count'] = actions['value']
                if actions['action_type'] == 'offsite_conversion.fb_pixel_purchase':
                    result['effective_count'] = actions['value']
        except Exception as e:
            result['effective_count'] = ''
            result['link_click_count'] = ''
        result['create_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield result


# 获取广告组统计信息
def fetch_adset_data(adset_id):
    adset = AdSet(adset_id)
    adset_datas = adset.get_insights(fields=[
        AdsInsights.Field.adset_id,
        AdsInsights.Field.spend,
        AdsInsights.Field.impressions,
        AdsInsights.Field.reach,
        AdsInsights.Field.unique_actions,
        AdsInsights.Field.adset_name],
        params={
        'breakdowns': ['hourly_stats_aggregated_by_audience_time_zone'],
        'time_range': {'since': yesterday, 'until': today}
    })
    for adset_data in adset_datas:
        print(adset_data)
        result = {}
        result['spend'] = adset_data['spend'] if 'spend' in adset_data else ''
        result['impression_count'] = adset_data['impressions'] if\
            'impressions' in adset_data else ''
        result['reach_count'] = adset_data['reach'] if\
            'reach' in adset_data else ''
        result['link_click_count'] = adset_data['link_click'] if\
            'link_click_count' in adset_data else ''
        result['ad_set_id'] = adset_data['adset_id'] if\
            'adset_id' in adset_data else ''
        result['stat_dt'] = adset_data['date_start']
        print(result['stat_dt'])
        result['stat_hour'] = \
            str(int(adset_data['hourly_stats_aggregated_by_audience_time_zone'][0:2]) + 1)
        try:
            for actions in adset_data['unique_actions']:
                if actions['action_type'] == 'link_click':
                    result['link_click_count'] = actions['value']
                if actions['action_type'] == 'offsite_conversion.fb_pixel_purchase':
                    result['effective_count'] = actions['value']
        except Exception as e:
            result['effective_count'] = ''
            result['link_click_count'] = ''
        result['create_time'] =\
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield result


# 获取广告系列统计信息
def fetch_campaign_data(campaign_id):
    campaign = Campaign(campaign_id)
    campaign_datas = campaign.get_insights(fields=[
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.spend,
        AdsInsights.Field.impressions,
        AdsInsights.Field.unique_actions,
        AdsInsights.Field.reach],
        params={
        'breakdowns': ['hourly_stats_aggregated_by_audience_time_zone'],
        'time_range': {'since': yesterday, 'until': today}
    })
    for campaign_data in campaign_datas:
        result = {}
        result['spend'] = campaign_data['spend'] if\
            'spend' in campaign_data else ''
        result['impression_count'] = campaign_data['impressions'] if\
            'impressions' in campaign_data else ''
        result['reach_count'] = campaign_data['reach'] if\
            'reach' in campaign_data else ''
        result['ad_compaign_id'] = campaign_data['campaign_id'] if\
            'campaign_id' in campaign_data else ''
        result['stat_dt'] = campaign_data['date_start']
        print(result['stat_dt'])
        result['stat_hour'] = \
            str(int(campaign_data['hourly_stats_aggregated_by_audience_time_zone'][0:2]) + 1)
        try:
            for actions in campaign_data['unique_actions']:
                if actions['action_type'] == 'link_click':
                    result['link_click_count'] = actions['value']
                if actions['action_type'] == 'offsite_conversion.fb_pixel_purchase':
                    result['effective_count'] = actions['value']
        except Exception as e:
            result['effective_count'] = ''
            result['link_click_count'] = ''
        result['create_time'] =\
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        yield result


# 写入数据库
def insert_into_db(db, table, item):
    try:
        db.insert(table, item)
    except Exception as e:
        print('insert error')
        print(e)

# 写入数据库
def replace_into_db(db, table, item):
    try:
        db.replace(table, item)
    except Exception as e:
        print('replace error')
        print(e)

def delete_from_db(db, table, condition):
    try:
        db.delete(table, condition)
    except Exception as e:
        print('delete error')
        print(e)


def main():
    db = Db(dbconfig)
    FacebookAdsApi.init(
        my_app_id, my_app_secret, my_access_token)
    for account in accounts:
        my_account = AdAccount(fbid=account)
        adcampaigns = fetch_campaigns(my_account)
        adsets = fetch_adsets(my_account)
        ads = fetch_ads(my_account)
        delete_from_db(db, 'ams_ad_compaign_stat_h', 'stat_dt="{}"'.format(yesterday))
        delete_from_db(db, 'ams_ad_set_stat_h', 'stat_dt="{}"'.format(yesterday))
        delete_from_db(db, 'ams_ad_stat_h', 'stat_dt="{}"'.format(yesterday))
        '''
        for adcampaign in adcampaigns:
            print(adcampaign)
            replace_into_db(db, 'ams_ad_compaign', adcampaign)
            campaign_stats = fetch_campaign_data(adcampaign['facebook_ad_campaign_id'])
            
            for campaign_stat in campaign_stats:
                insert_into_db(db, 'ams_ad_compaign_stat_h', campaign_stat)
        '''
        for adset in adsets:
            # 如果广告组数据为当天创建或更新，则插入
            print(adset)
            replace_into_db(db, 'ams_ad_set', adset)
            adset_stats = fetch_adset_data(adset['facebook_adset_id'])
            
            for adset_stat in adset_stats:
                print(adset_stat)
                insert_into_db(db, 'ams_ad_set_stat_h', adset_stat)
'''
        for ad in ads:
            print(ad)
            replace_into_db(db, 'ams_ad', ad)
            ad_stats = fetch_ad_data(ad['facebook_ad_id'])
            
            for ad_stat in ad_stats:
                insert_into_db(db, 'ams_ad_stat_h', ad_stat)
'''

if __name__ == '__main__':
    main()
