from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adsinsights import AdsInsights
from datetime import datetime
from publicConfig import BasicConfig as cfg


class GetAdsets():

    def __init__(self, adaccount, model):
        self.model = model
        self.adaccount = adaccount
        self.dbconfig = cfg.dbconfig
        self.yesterday, self.today, self.time_range = cfg.get_time_range()

    def fetch_ad_sets(self):
        print(self.adaccount)
        try:
            ad_sets = self.adaccount.get_ad_sets(fields=[
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
        except Exception as e:
            print(e)
        recent_adsets = list()
        current_hour = datetime.now().hour
        last_hour = str(datetime.now().hour - 1)
        for adset in ad_sets:
            updated_dt = adset['updated_time'][:10]
            updated_hour = adset['updated_time'][11:13]
            if (updated_dt == self.yesterday or updated_dt == self.today)\
                    and updated_hour in (current_hour, last_hour, '23'):
                recent_adsets.append(adset)
            else:
                break
        return recent_adsets

    def handle_ad_sets(self, ad_set):
        result = dict()
        result['name'] = ad_set.get('name', '').replace("'", '\"')
        result['facebook_account_id'] = ad_set.get('account_id', '')
        result['facebook_campaign_id'] = ad_set.get('campaign_id', '')
        result['facebook_adset_id'] = ad_set.get('id', '')
        result['status'] = ad_set.get('status', '')
        result['adset_created_time'] = ad_set.get('created_time', '')
        result['updated_time'] = ad_set.get('updated_time', '')
        result['effective_status'] = ad_set.get('effective_status', '')
        result['daily_budget'] = ad_set.get('daily_budget', '')
        result['pacing_type'] = str(ad_set.get('pacing_type', '')).\
            replace("'", '\"')
        result['start_time'] = ad_set.get('start_time', '')
        result['end_time'] = ad_set.get('end_time', '')
        result['bid_info'] = ad_set.get('bid_info', '').replace("'", '\"')
        result['billing_event'] = ad_set.get('billing_event', '').\
            replace("'", '\"')
        result['attribution_spec'] = str(ad_set.get('attribution_spec', '')).\
            replace("'", '\"')
        #       result['targeting'] = str(ad_set.get('targeting', ''))
        result['optimization_goal'] = str(ad_set.get('optimization_goal', ''))\
            .replace("'", '\"')
        result['created_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.model.replace_into_adset_basic(result)

    def fetch_adset_data(self, adset_id):
        adset = AdSet(adset_id)
        adset_datas = adset.get_insights(fields=[
            AdsInsights.Field.adset_id,
            AdsInsights.Field.spend,
            AdsInsights.Field.impressions,
            AdsInsights.Field.reach,
            AdsInsights.Field.unique_actions,
            AdsInsights.Field.adset_name],
            params={
                'breakdowns':
                    ['hourly_stats_aggregated_by_audience_time_zone'],
                'time_range': self.time_range})
        return adset_datas


    def handle_adset_data(self, adset_id):
        adset_datas = self.fetch_adset_data(adset_id)
#        self.model.delete_from_adset_stat(
#            'facebook_ad_set_stat_h',
#            'stat_dt="{0}" and ad_set_id={1}'.format(self.yesterday, adset_data['adset_id']))
        for adset_data in adset_datas:
            result = dict()
            result['spend'] = adset_data.get('spend', '0')
            result['impression_count'] = adset_data.get('impressions', '0')
            result['reach_count'] = adset_data.get('reach', '0')
            result['link_click_count'] = adset_data.get('link_click', '0')
            result['ad_set_id'] = adset_data.get('adset_id', '0')
            result['stat_dt'] = adset_data.get('date_start', '')
            result['stat_hour'] = adset_data['hourly_stats_aggregated_by_audience_time_zone'][0:2]
            result['create_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            actions = adset_data.get('unique_actions', '')
            if actions:
                for action in actions:
                    if action['action_type'] == 'link_click':
                        result['link_click_count'] = action['value']
                    if action['action_type'] == 'offsite_conversion.fb_pixel_purchase':
                        result['effective_count'] = action['value']
                    if action['action_type'] == '':
                        result['add_to_cart'] = action['value']
            print(result)
            self.model.insert_into_adsetstat(result)
