from facebook_business.api import FacebookAdsApi
from concurrent import futures
from datetime import datetime, date
from facebook_business.adobjects.adaccount import AdAccount
from fetchCampaign import GetCampaigns
from fetchAd import GetAds
from fetchAdSet import GetAdsets
from publicConfig import BasicConfig as cfg
from model import GetFacebook
from fetchAdcreative import GetCreative

proxies = {
    'https': 'https://127.0.0.1:1080',
    'http': 'http://127.0.0.1:1080'
}


def worker_basic(basic_func, basics):
    with futures.ThreadPoolExecutor(40) as executor:
        executor.map(basic_func, basics)


def worker_stat(stats_func, stats):
    with futures.ThreadPoolExecutor(40) as executor:
        executor.map(stats_func, stats)


def manager(adaccount):

    FacebookAdsApi.init(cfg.my_app_id, cfg.my_app_secret, cfg.my_access_token, proxies=proxies)
    my_account = AdAccount(adaccount)
    db_model = GetFacebook(cfg.dbconfig)
    print('asss')

    # ad_campaign_object = GetCampaigns(my_account, db_model)
    # ad_set_object = GetAdsets(my_account, db_model)
    # ad_object = GetAds(my_account, db_model)
    creative_object = GetCreative(my_account, db_model)
    print('fghhh')
    # adcampaigns = ad_campaign_object.fetch_campaigns()

    # worker_basic(ad_campaign_object.handle_campaign, adcampaigns)
    # active_campaigns = db_model.get_active_campaigns()
    # active_campaigns = [active_campaign[0] for active_campaign in active_campaigns]
    # print(active_campaigns)
    # worker_stat(ad_campaign_object.handle_campaign_data, active_campaigns)

    # adsets = ad_set_object.fetch_ad_sets()
    # worker_basic(ad_set_object.handle_ad_sets, adsets)
    # active_adsets = db_model.get_active_adsets()
    # active_adsets = ['23842905693710040']
    # active_campaigns = [active_adset[0] for adset in adsets]
    # worker_stat(ad_set_object.handle_adset_data, active_adsets)

    # ads = ad_object.fetch_ads()
    # worker_basic(ad_object.handle_ads, ads)
    creative_object = GetCreative(my_account, db_model)
    creatives = creative_object.fetch_adcreative()
    worker_basic(creative_object.handle_creatives, creatives)


def main():
    # db = GetFacebook(cfg.dbconfig_single)
    # db.write_log(start_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # write_log(db, 'ams_batch_info', start_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print('gggh')
    with futures.ProcessPoolExecutor(4) as executor:
        print('fff')
        executor.map(manager, cfg.accounts)
    # db.write_log(end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # write_log(db, 'ams_batch_info', end_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    # manager('act_1624101030999583')

if __name__ == '__main__':
    main()
