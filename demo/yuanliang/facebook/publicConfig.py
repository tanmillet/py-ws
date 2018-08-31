from datetime import date, datetime, timedelta


class BasicConfig():
    my_app_id = '1442954805800139'
    my_app_secret = '147ef52da9160910fbc50f247e36ea45'
    my_access_token = 'EAAUgXBigbMsBAMM7BpKUpFJJoDs7bMlZCadD76exvwsAUchqbWP4WJRZAQODWu\
                       VtMvoDKyX4UFIo1gIreTjkVqM0VLjqgtBMSIQcEbOSl0rOVOYLHUxi25aFzbkd\
                       aXEyCZC4q4ZBIyCvOaeLcOVFv6m73blNZAhbx81vZCsDQw9wZDZD'
    dbconfig = {
                'host': '47.74.247.130',
                'port': 3306,
                'user': 'root',
                'password': 'cuckoo787',
                'db': 'website_data_scraping',
                'charset': 'utf8mb4',
                'maxsize': 50
                }
    dbconfig_single = {
                'host': '47.74.247.130',
                'port': 3306,
                'user': 'root',
                'password': 'cuckoo787',
                'db': 'website_data_scraping',
                'charset': 'utf8mb4',
                'maxsize': 1
                }
    accounts = [
                'act_1624101030999583', 'act_2073471799587838',
                'act_658302644509277', 'act_227746614479671',
                'act_1891140967849956', 'act_2105643122980492',
                'act_1763514533738273', 'act_349407535588856',
                'act_263090694261264', 'act_246112529474945',
                'act_395445774307646'
                ]

    @classmethod
    def get_time_range(cls):
        if datetime.now().hour <= 2:
            yesterday = str(date.today() - timedelta(days=1))
            today = str(date.today())
        else:
            yesterday = str(date.today())
            today = str(date.today())
        time_range = {'since': yesterday, 'until': yesterday}
        return yesterday, today, time_range
