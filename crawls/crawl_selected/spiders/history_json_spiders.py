from crawl_commons.spiders.common_spider import *

#官方网站：列表页普通,详情页普通,一天二次
class HistoryJsonSpider1(CommonSpider):  # 需要继承scrapy.Spider类
    name= "history_json_spider1" # 定义蜘蛛名

#官方：列表页js,详情页普通，一天2次
class HistoryJsonSpider2(CommonSpider):  # 需要继承scrapy.Spider类
    name= "history_json_spider2" # 定义蜘蛛名

#官方：列表页js,详情页js，一天2次
class HistoryJsonSpider3(CommonSpider):  # 需要继承scrapy.Spider类
    name= "history_json_spider3" # 定义蜘蛛名
