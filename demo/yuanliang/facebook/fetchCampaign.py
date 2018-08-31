from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adsinsights import AdsInsights
from datetime import datetime
from publicConfig import BasicConfig as cfg


class GetCampaigns():

    def __init__(self, adaccount, model):
        self.adaccount = adaccount
 #       self.dbconfig = cfg.dbconfig
        self.model = model
        self.yesterday, self.today, self.time_range = cfg.get_time_range()

    def fetch_campaigns(self):
        try:
            campaigns = self.adaccount.get_campaigns(fields=[
                Campaign.Field.name,
                Campaign.Field.account_id,
                Campaign.Field.id,
                Campaign.Field.status,
                Campaign.Field.objective,
                Campaign.Field.effective_status,
                Campaign.Field.created_time,
                Campaign.Field.updated_time])
                # params={Campaign.Field.effective_status:
                # [AdSet.Status.archived, AdSet.Status.active,
                         # AdSet.Status.paused]})

        except Exception as e:
            print(e)
        recent_campaigns = list()
        #current_hour = str(datetime.now().hour)
        current_hour = '01'
        last_hour = str(datetime.now().hour - 1)
        for campaign in campaigns:
            updated_dt = campaign['updated_time'][:10]
            updated_hour = campaign['updated_time'][11:13]
            if (updated_dt == self.yesterday or updated_dt == self.today)\
                    and updated_hour in (current_hour, last_hour, '23'):
                recent_campaigns.append(campaign)
            else:
                break
        return recent_campaigns

    def handle_campaign(self, campaign):
        result = dict()
        result['name'] = campaign.get('name', '').replace("'", '\"')
        result['facebook_account_id'] = campaign.get('account_id', '')
        result['facebook_ad_campaign_id'] = campaign.get('id', '')
        result['status'] = campaign.get('status', '')
        result['objective'] = campaign.get('objective', '')
        result['effective_status'] = campaign.get('effective_status', '')
        result['campaign_created_time'] = campaign.get('created_time', '')
        result['updated_time'] = campaign.get('updated_time', '')
        result['created_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(result)
        self.model.replace_into_campaign_basic(result)

    def fetch_campaign_data(self, campaign_id):
        campaign = Campaign(campaign_id)
        campaign_datas = campaign.get_insights(fields=[
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.spend,
            AdsInsights.Field.impressions,
            AdsInsights.Field.unique_actions,
            AdsInsights.Field.reach],
            params={
                'breakdowns':
                    ['hourly_stats_aggregated_by_audience_time_zone'],
                'time_range': self.time_range})
        return campaign_datas

    def handle_campaign_data(self, campaign_id):
        # try:
        campaign_datas = self.fetch_campaign_data(campaign_id)
        '''
        except Exception as e:
            error_info = '获取广告系列：{} 统计数据发生错误'.format(campaign_id)
            print(error_info)
        '''
        # self.model.delete_from_campaign_stat(campaign_id)
        for campaign_data in campaign_datas:
            result = dict()
            result['spend'] = campaign_data.get('spend', '0')
            result['impression_count'] = campaign_data.get('impressions', '0')
            result['reach_count'] = campaign_data.get('reach', '0')
            result['ad_compaign_id'] = campaign_data.get('campaign_id', '0')
            result['stat_dt'] = campaign_data.get('date_start', '0')
            result['stat_hour'] = campaign_data['hourly_stats_aggregated_by_audience_time_zone'][0:2]
            result['create_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            actions = campaign_data.get('unique_actions', '')
            if actions:
                for action in actions:
                    if action['action_type'] == 'link_click':
                        result['link_click_count'] = action['value']
                    if action['action_type'] == 'offsite_conversion.fb_pixel_purchase':
                        result['effective_count'] = action['value']
                    if action['action_type'] == '':
                        result['add_to_cart'] = action['value']
                print(result)
            self.model.insert_into_campaignstat(result)
