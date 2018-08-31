from facebook_business.adobjects.adcreative import AdCreative
from publicConfig import BasicConfig as cfg


class GetCreative():

    def __init__(self, adaccount, model):
        self.model = model
        self.adaccount = adaccount
        self.yesterday, self.today, self.time_range = cfg.get_time_range()
#        self.creatives = self.fetch_adcreative()

    def fetch_adcreative(self):
        creatives = self.adaccount.get_ad_creatives(
            fields=[
                AdCreative.Field.account_id,
                AdCreative.Field.effective_object_story_id,
                AdCreative.Field.object_story_id,
                AdCreative.Field.object_story_spec,
                AdCreative.Field.id,
                AdCreative.Field.image_url,
                AdCreative.Field.thumbnail_url,
                AdCreative.Field.object_type,
                AdCreative.Field.object_url,
                AdCreative.Field.name,
                AdCreative.Field.title,
                AdCreative.Field.video_id])
        creatives = list(creatives)
        return creatives

    def handle_creatives(self, adcreative):
#        try:
#            adcreative = self.creatives.get_one()
#        except Exception as e:
#            error_info = '获取广告创意信息时发生错误{}'.format(e)
#            print(error_info)
        result = dict()
        result['image_url'] = adcreative.get('image_url', '')
        result['account_id'] = adcreative.get('account_id', '')
        result['facebook_creative_id'] = adcreative.get('creative_id', '')
        result['effective_object_story_id'] =\
            adcreative.get('effective_object_story_id', '')
        result['object_story_id'] = adcreative.get('object_story_id', '')
        result['object_type'] = adcreative.get('object_type', '')
        result['thumbnail_url'] = adcreative.get('thumbnail_url', '')
        result['name'] = adcreative.get('name', '')
        result['title'] = adcreative.get('title', '')
#        result['object_url'] = adcreative.get('object_url', '')
        result['video_id'] = adcreative.get('video_id', '')
        story = adcreative.get('object_story_spec', '')
        if story:
            result['page_id'] = story.get('page_id', '')
            video = story.get('video_data', '')
            link = story.get('link_data', '')
            if video:
                try:
                    result['video_link'] =\
                        video['call_to_action']['value']['link']
                except Exception as e:
                    result['video_link'] =\
                        video['call_to_action']['value']['link_caption']
            if link:
                result['caption'] = link.get('caption', '')
                # result['description'] = link.get('description', '')
                result['link_name'] = link.get('name', '')
                result['link'] = link.get('link', '')
        self.model.replace_into_adcreative(result)
