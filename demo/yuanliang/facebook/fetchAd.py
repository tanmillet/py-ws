from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from publicConfig import BasicConfig as cfg
from datetime import datetime
from model import GetFacebook


class GetAds():

    def __init__(self, adaccount, model):
        self.model = model
        self.adaccount = adaccount
#        self.dbconfig = cfg.dbconfig
        self.yesterday, self.today, self.time_range = cfg.get_time_range()

    def fetch_ads(self):
        print(self.yesterday, self.today)
        ads = self.adaccount.get_ads(fields=[
            Ad.Field.name,
            Ad.Field.account_id,
            Ad.Field.id,
            Ad.Field.adset_id,
            Ad.Field.status,
            Ad.Field.effective_status,
            Ad.Field.created_time,
            Ad.Field.updated_time,
            Ad.Field.campaign_id])
        recent_ads = list()
        current_hour = datetime.now().hour
        last_hour = str(datetime.now().hour - 1)
        for ad in ads:
            updated_dt = ad['updated_time'][:10]
            print(updated_dt)
            updated_hour = ad['updated_time'][11:13]
            if (updated_dt == self.yesterday or updated_dt == self.today):
#                    and updated_hour in (current_hour, last_hour, '23'):
                recent_ads.append(ad)
            else:
                break
        return recent_ads

    def handle_ads(self, ad):
        ad_ins = Ad(ad['id'])
        ad_creative = self.fetch_adcreatives(ad_ins)
        ad_creative = dict(ad_creative[0])
        result = dict()
        result['facebook_campaign_id'] = ad.get('campaign_id', '')
        result['name'] = ad.get('name', '').replace("'", '\"')
        result['facebook_account_id'] = ad.get('account_id', '')
        result['facebook_ad_id'] = ad.get('id', '')
        result['facebook_adset_id'] = ad.get('adset_id', '')
        result['status'] = ad.get('status', '')
        result['effective_status'] = ad.get('effective_status', '')
        result['ad_created_time'] = ad.get('created_time', '')
        result['updated_time'] = ad.get('updated_time', '')
        result['created_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result['thumbnail'] = ad_creative.get('thumbnail_url', '')
        result['url_tags'] = ad_creative.get('url_tags', '')
        result['big_image'] = ad_creative.get('image_url', '')
        result['title'] = ad_creative.get('title', '')
        result['object_type'] = ad_creative.get('object_type', '')
        story = ad_creative.get('object_story_spec', '')
        if story:
            try:
                result['object_type'] = story['object_type']
            except:
                result['object_type'] = ''
            try:
                result['destination'] = story['link_data']['link']
            except Exception as e:
                result['destination'] = story['link_data']['call_to_action']['value']['link']
            try:
                result['short_destination'] = story['video_data']['call_to_action']['value']['link']
            except Exception as e:
                result['short_destination'] = ''
            finally:
                self.model.replace_into_ad_basic(result)
        else:
            self.model.replace_into_ad_basic(result)

    def fetch_adcreatives(self, ad):
        ad_creative = ad.get_ad_creatives(fields=[
            AdCreative.Field.thumbnail_url,
            AdCreative.Field.url_tags,
            AdCreative.Field.video_id,
            AdCreative.Field.object_story_spec,
            AdCreative.Field.title,
            AdCreative.Field.image_url])
        return ad_creative
