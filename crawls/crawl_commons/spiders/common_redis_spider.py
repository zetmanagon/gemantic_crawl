import scrapy
from crawl_commons.items import CrawlResultItem

from crawl_commons.repository.seed import *
from scrapy_redis.spiders import RedisSpider
from crawl_commons.repository.crawl import *
from crawl_commons.utils.article_util import *
from crawl_commons.utils.string_util import *
from crawl_commons.utils.set_redis import *
import pickle
import scrapy_redis.defaults as srd
from crawl_commons.utils.set_redis import SetRedis
import json
#一级页面抓取通用爬虫，该爬虫不作爬取
class CommonRedisSpider(RedisSpider):  # 需要继承scrapy.Spider类
    name= "common_redis_spider" # 定义蜘蛛名
    _dupefilter_template = None  # df_key模板, from settings,静态即可
    crawlId = 0
    def __init__(self, name=None, **kwargs):
        super(CommonRedisSpider,self).__init__(name=name,kwargs=kwargs)
        self.seedDB = SeedRepository()
        self.crawlDB = CrawlRepository()
        self.df_key = None  # dupefilter key

    def init_set_redis(self):
        use_set = self.settings.getbool('REDIS_START_URLS_AS_SET', srd.START_URLS_AS_SET)
        SetRedis.init_set_redis(self.server, use_set)

    def init_df_key(self):
        if not CommonRedisSpider._dupefilter_template:
            # 如果在__init__中初始化self.crawler.engine.slot.scheduler尚不存在
            try:
                CommonRedisSpider._dupefilter_template = self.crawler.engine.slot.scheduler.dupefilter_key
            except AttributeError:
                CommonRedisSpider._dupefilter_template = self.settings.get("SCHEDULER_DUPEFILTER_KEY",
                                                                      srd.defaults.SCHEDULER_DUPEFILTER_KEY)
        self.df_key = CommonRedisSpider._dupefilter_template % {'spider': self.name}

    def get_metakey(self, url):
        return self.redis_key + "+" + url

    def _set_redis_key(self, url, meta, clear=False):
        """url存入start_url,同时
        序列化meta data至redis, key: metakey
        :param url:
        :param meta:
        :param clear: bool清除dupefileter标志
        :return:
        """
        metas = pickle.dumps(meta)
        self.server.set(self.get_metakey(url), metas)
        SetRedis.fill_seed(url, self.redis_key, self.df_key, clear=clear)

    def start_tasks(self):  # 由此方法通过下面链接爬取页面, 原start_requests()
        crawlName = self.name.replace("history_", "")
        timestamp = time.strftime('%Y-%m-%d %H-%M-%S', time.localtime(time.time()))  # 该次爬虫的时间戳
        # '''
        seeds = self.seedDB.get_seed(crawlName)
        '''
        seeds = self._get_seed_offline()
        # '''
        # 定义爬取的链接
        for seed in seeds:
            if self.crawlId == 0:
                self.crawlId = seed.crawlId
            regex = self.seedDB.get_regex(seed.regexName)
            if len(regex) > 0:
                meta = {}
                meta["seedRegex"] = regex
                meta["depthNumber"] = 0
                meta["pageNumber"] = 1
                meta["seedInfo"] = seed
                meta["renderType"] = seed.renderType
                meta["pageRenderType"] = seed.pageRenderType
                meta["renderSeconds"] = seed.renderSeconds
                meta["nocontentRender"] = seed.nocontentRender
                self._set_redis_key(seed.url, meta, True)
                # yield scrapy.Request(url=seed.url,meta=meta, callback=self.parse)
            else:
                self.log("%s no regex" % seed.url)

    def _get_meta_by_url(self, url):
        urlkey = self.get_metakey(url)
        metas = self.server.get(urlkey)
        if metas:
            meta = pickle.loads(metas)
            self.server.delete(urlkey)
            return meta
        else:
            self.logger.info("%s not found in redis_key" % url)

    def make_requests_from_url(self, url):
        meta = self._get_meta_by_url(url)
        parse = self.parse
        try:
            if meta is not None and "parse" in meta and meta["parse"] is not None and meta["parse"] == "detail":
                parse = self.parseDetail
        except KeyError:
            pass
        return scrapy.Request(url=url, meta=meta, callback=parse)

    def parse(self, response):
        meta = response.meta
        regexList = meta["seedRegex"]
        seed = meta["seedInfo"]
        depthNumber = int(meta["depthNumber"])
        regexDict = regexList[depthNumber].regexDict
        if "list" not in regexDict:
            self.log("%s no list regex" % response.url)
            yield
        listRegexs = regexDict["list"]
        domain = meta["seedInfo"].domain
        detailUrls = ArticleUtils.getResponseContents4WebRegex(listRegexs, response)
        listDataAll = {}
        for (k, v) in regexDict.items():
            if "nextPage" == k or "list" == k:
                continue
            itemValues = ArticleUtils.getResponseFieldValue(k, False, v, response)
            listDataAll[k] = itemValues
        listRegex = listRegexs[-1]

        isDetail = True
        if depthNumber + 1 < regexList[-1].depthNumber:
            isDetail = False
        for i, detailUrl in enumerate(detailUrls):
            isVaildUrl = True
            if StringUtils.isNotEmpty(listRegex.resultFilterRegex):
                isVaildUrl = re.match(listRegex.resultFilterRegex, detailUrl)
            if not isVaildUrl:
                continue
            targetUrl = ArticleUtils.getFullUrl(detailUrl, response.url)
            if depthNumber == 0:
                targetUrl = ArticleUtils.getFullUrl(detailUrl, seed.url)
            self.logger.info("isDetail %s targetUrl %s" % (str(isDetail), targetUrl))
            # if domain not in targetUrl:
            #     continue
            listData = {}
            metaCopy = meta.copy()
            if "listData" in meta and len(meta["listData"]) > 0:
                listData = meta["listData"]
            for (k, v) in listDataAll.items():
                if v is not None and i < len(v) and v[i] is not None and StringUtils.isNotEmpty(str(v[i])):
                    listDataValue = v[i]
                    if "category" == k and k in listData:
                        listDataValue = listData["category" + "/" + listDataValue]
                    listData[k] = listDataValue
            metaCopy["listData"] = listData
            metaCopy["contentPageNumber"] = 1
            metaCopy["depthNumber"] = depthNumber + 1
            metaCopy["refererLink"] = response.url
            metaCopy["renderType"] = listRegex.renderType
            metaCopy["pageRenderType"] = listRegex.pageRenderType
            metaCopy["renderSeconds"] = listRegex.renderSeconds
            # metaCopy["renderBrowser"] = listRegex.renderBrowser
            if ArticleUtils.isFile(targetUrl):
                # 不知何时会被调用, 没有使用redis机制,有可能有集群重复问题
                self.crawlDB.saveFileCrawlDetail(metaCopy, targetUrl)
            elif isDetail:
                metaCopy['parse'] = 'detail'
                self._set_redis_key(targetUrl, metaCopy)
                # yield scrapy.Request(url=targetUrl,meta=metaCopy, callback=self.parseDetail)
            else:
                self.log("next level %s" % targetUrl)
                self._set_redis_key(targetUrl, metaCopy)
                # yield scrapy.Request(url=targetUrl, meta=metaCopy, callback=self.parse)

        pageNumber = meta["pageNumber"]
        maxPageNumber = 0
        nextPageRegex = []
        if "nextPage" in regexDict:
            nextPageRegex = regexDict["nextPage"]
            maxPageNumber = nextPageRegex[-1].maxPageNumber
        if self.name.startswith("history_") and (
                (maxPageNumber > 0 and pageNumber <= maxPageNumber) or maxPageNumber <= 0):
            nextUrls = ArticleUtils.getNextPageUrl(nextPageRegex, response)
            if len(nextUrls) > 0 and StringUtils.isNotEmpty(nextUrls[0]):
                targetNextUrl = nextUrls[0]
                self.log("nextPage %s" % targetNextUrl)
                meta["pageNumber"] = meta["pageNumber"] + 1
                self._set_redis_key(targetNextUrl, meta)
                yield scrapy.Request(url=targetNextUrl, meta=meta, callback=self.parse)
            else:
                self.log("lastPage %s" % (response.url))


    def parseDetail(self, response):
        meta = response.meta
        url = response.url
        regexList = meta["seedRegex"]
        regexDict = regexList[-1].regexDict
        seed = meta["seedInfo"]
        enableDownloadFile = False
        enableDownloadImage = False
        enableSnapshot = False
        nocontentRender = meta["nocontentRender"]
        contentPageNumber = meta["contentPageNumber"]
        if seed.enableDownloadFile == 1:
            enableDownloadFile = True
        if seed.enableDownloadImage == 1:
            enableDownloadImage = True
        if seed.enableSnapshot == 1:
            enableSnapshot = True
        detailData = {}
        if "detailData" in meta:
            detailData = meta["detailData"]
        if contentPageNumber <= 1:
            detailData["url"] = url
        autoDetailData = {}
        if "autoDetailData" in meta:
            autoDetailData = meta["autoDetailData"]

        contentAutoDetailData = ArticleUtils.getAutoDetail(contentPageNumber, response, enableDownloadImage,
                                                           enableSnapshot)

        meta["autoDetailData"] = autoDetailData
        maxPageNumber = 0
        pageContent = None
        contentData = {}
        if enableDownloadFile:
            files = ArticleUtils.getContentFiles(response)
            if files is not None and len(files) > 0:
                contentData["contentFiles"] = files
                # ArticleUtils.mergeDict(detailData, "contentFiles", files)

        for (k, v) in regexDict.items():
            if "nextPage" == k:
                continue
            itemValues = ArticleUtils.getResponseFieldValue(k, True, v, response)
            itemValue = None
            if itemValues is not None and len(itemValues) > 0 and itemValues[0] is not None and StringUtils.isNotEmpty(
                    StringUtils.trim(str(itemValues[0]))):
                itemValue = itemValues[0]
            if itemValue is None:
                continue
            contentData[k] = itemValue
            if "content" == k:
                pageContent = itemValue
                maxPageNumber = v[-1].maxPageNumber
                if enableDownloadImage:
                    images = ArticleUtils.getContentImages(v, response)
                    if images is not None and len(images) > 0:
                        contentData["contentImages"] = images
                        # ArticleUtils.mergeDict(detailData,"contentImages",images)
                contentSnapshots = ArticleUtils.getResponseFieldValue("contentSnapshot", True, v, response)
                if contentSnapshots is not None and len(contentSnapshots) > 0 and StringUtils.isNotEmpty(
                        contentSnapshots[0]):
                    if enableSnapshot:
                        contentData["contentSnapshot"] = contentSnapshots[0]
                        # ArticleUtils.mergeDict(detailData,"contentSnapshot",contentSnapshots[0])

        if pageContent is not None and StringUtils.isEmpty(ArticleUtils.removeAllTag(pageContent)):
            pageContent = None
        if pageContent is None and "content" in contentAutoDetailData and StringUtils.isNotEmpty(
                contentAutoDetailData["content"]):
            pageContent = ArticleUtils.removeAllTag(contentAutoDetailData["content"])
            if StringUtils.isEmpty(pageContent):
                pageContent = None

        if pageContent is None and nocontentRender == 1 and not ArticleUtils.isRender(meta, self.name):
            metaCopy = meta.copy()
            metaCopy["renderType"] = 1
            metaCopy["renderSeconds"] = 5
            metaCopy["detailData"] = detailData
            metaCopy["autoDetailData"] = autoDetailData
            self.log("re render url %s" % url)
            # metaCopy["parse"] = "detail"
            self._set_redis_key(url, metaCopy,True)
            #获取不到正文，尝试使用js渲染方式，针对网站部分链接的详情页使用js跳转
            # yield scrapy.Request(url=url, meta=metaCopy, callback=self.parseDetail,dont_filter=True)
        else:
            # with open(file="/home/yhye/tmp/crawl_data_policy/" + ArticleUtils.getArticleId(response.url) + ".html", mode='w') as f:
            #     f.write("".join(response.xpath("//html").extract()))
            ArticleUtils.mergeNewDict(detailData, contentData)
            ArticleUtils.mergeNewDict(autoDetailData, contentAutoDetailData)
            nextPageRegex = []
            if "nextPage" in regexDict:
                nextPageRegex = regexDict["nextPage"]
                maxPageNumber = nextPageRegex[-1].maxPageNumber
            targetNextUrl = ""
            if maxPageNumber <= 0 or (maxPageNumber > 0 and contentPageNumber < maxPageNumber):
                nextUrls = ArticleUtils.getNextPageUrl(nextPageRegex,response)
                if len(nextUrls) > 0 and StringUtils.isNotEmpty(nextUrls[0]):
                    targetNextUrl = nextUrls[0]
            if StringUtils.isNotEmpty(targetNextUrl):
                meta["detailData"] = detailData
                meta["autoDetailData"] = autoDetailData
                meta["contentPageNumber"] = contentPageNumber+1
                # meta["parse"] = "detail"
                self._set_redis_key(targetNextUrl, meta,True)
                self.log("detail nextPage %s %s" % (str(contentPageNumber+1),targetNextUrl))
                # yield scrapy.Request(url=targetNextUrl, meta=meta, callback=self.parseDetail)
            else:
                item = ArticleUtils.meta2item(meta, detailData["url"])
                for (k,v) in detailData.items():
                    itemValue = None
                    if "category" == k and k in item:
                        itemValue = item[k] + "/" + v
                    elif "contentImages" == k or "contentFiles" == k:
                        itemValue = json.dumps(list(v.values()),ensure_ascii=False)
                    else:
                        itemValue = v
                    item[k] = itemValue
                for (k,v) in autoDetailData.items():
                    if "contentImages" == k and k not in item:
                        item[k] = json.dumps(list(v.values()),ensure_ascii=False)
                    elif k not in item or StringUtils.isEmpty(ArticleUtils.removeAllTag(str(item[k]))):
                        item[k] = v
                # html = "".join(response.xpath("//html").extract())
                # item["html"] = html
                if "title" not in item or StringUtils.isEmpty(item["title"]):
                    item["title"] = response.xpath("//title//text()")
                yield item


    def closed(self,reason):
        self.log("on close start stat seeds %s %s" % (self.crawlId,self.name))
        self.log(reason)
        if self.crawlId > 0:
            self.seedDB.stat_seed(self.crawlId)
        self.log("%s %s stat seeds finished" % (self.crawlId,self.name))