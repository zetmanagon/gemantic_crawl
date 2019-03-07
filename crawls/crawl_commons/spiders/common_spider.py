import scrapy
from crawl_commons.items import CrawlResultItem

from crawl_commons.repository.seed import *
from crawl_commons.repository.crawl import *
from crawl_commons.utils.article_util import *
from crawl_commons.utils.string_util import *
from crawl_commons.utils.http_util import *

import json
#一级页面抓取通用爬虫，该爬虫不作爬取
class CommonSpider(scrapy.Spider):  # 需要继承scrapy.Spider类
    name= "common_spider" # 定义蜘蛛名
    crawlId = 0
    def __init__(self, name=None, **kwargs):
        super(CommonSpider,self).__init__(name=name,kwargs=kwargs)
        self.seedDB = SeedRepository()
        self.crawlDB = CrawlRepository()


    def start_requests(self):  # 由此方法通过下面链接爬取页面
        crawlName = self.name.replace("history_","")
        seeds = self.seedDB.get_seed(crawlName)
        timestamp = time.strftime('%Y-%m-%d %H-%M-%S', time.localtime(time.time()))  # 该次爬虫的时间戳
        # 定义爬取的链接
        for seed in seeds:
            if self.crawlId == 0:
                self.crawlId = seed.crawlId
            regex = self.seedDB.get_regex(seed.regexName)

            if len(regex) > 0:
                self.crawlDB.initCrawlStat(seed.url, timestamp)  # 初始化种子统计
                meta = {}
                meta["timestamp"] = timestamp
                meta["seedRegex"] = regex
                meta["depthNumber"] = 0
                meta["pageNumber"] = 1
                meta["seedInfo"] = seed
                meta["renderType"] = seed.renderType
                meta["pageRenderType"] = seed.pageRenderType
                meta["renderSeconds"] = seed.renderSeconds
                meta["nocontentRender"] = seed.nocontentRender
                yield scrapy.Request(url=seed.url,meta=meta, callback=self.parse)
            else:
                self.log("%s no regex" % seed.url)

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
        detailUrls = ArticleUtils.getResponseContents4WebRegex(listRegexs,response)
        listDataAll = {}
        for (k, v) in regexDict.items():
            if "nextPage" == k or "list" == k:
                continue
            itemValues = ArticleUtils.getResponseFieldValue(k, False, v, response)
            listDataAll[k] = itemValues
        listRegex = listRegexs[-1]

        isDetail = True
        if depthNumber+1 < regexList[-1].depthNumber:
            isDetail = False
        for i,detailUrl in enumerate(detailUrls):
            isVaildUrl = True
            if StringUtils.isNotEmpty(listRegex.resultFilterRegex):
                isVaildUrl = re.match(listRegex.resultFilterRegex, detailUrl)
            if not isVaildUrl:
                continue
            targetUrl = ArticleUtils.getFullUrl(detailUrl,response.url)
            if depthNumber == 0:
                targetUrl = ArticleUtils.getFullUrl(detailUrl,seed.url)
            self.logger.info("isDetail %s targetUrl %s" % (str(isDetail),targetUrl))
            # if domain not in targetUrl:
            #     continue
            listData = {}
            metaCopy = meta.copy()
            if "listData" in meta and len(meta["listData"])>0:
                listData = meta["listData"]
            for (k,v) in listDataAll.items():
                if v is not None and i<len(v) and v[i] is not None and StringUtils.isNotEmpty(str(v[i])):
                    listDataValue = v[i]
                    if "category" == k and k in listData:
                        listDataValue = listData["category"+"/"+listDataValue]
                    listData[k] = listDataValue
            metaCopy["listData"] = listData
            metaCopy["contentPageNumber"] = 1
            metaCopy["depthNumber"] = depthNumber+1
            metaCopy["refererLink"] = response.url
            metaCopy["renderType"] = listRegex.renderType
            metaCopy["pageRenderType"] = listRegex.pageRenderType
            metaCopy["renderSeconds"] = listRegex.renderSeconds
            # metaCopy["renderBrowser"] = listRegex.renderBrowser
            if ArticleUtils.isFile(targetUrl):
                self.crawlDB.saveFileCrawlDetail(metaCopy,targetUrl)
            elif isDetail:
                yield scrapy.Request(url=targetUrl,meta=metaCopy, callback=self.parseDetail)
            else:
                self.log("next level %s" % targetUrl)
                yield scrapy.Request(url=targetUrl, meta=metaCopy, callback=self.parse)

        pageNumber = meta["pageNumber"]
        maxPageNumber = 0
        nextPageRegex = []
        if "nextPage" in regexDict:
            nextPageRegex = regexDict["nextPage"]
            maxPageNumber = nextPageRegex[-1].maxPageNumber
        if self.name.startswith("history_") and ((maxPageNumber > 0 and pageNumber <= maxPageNumber) or maxPageNumber<=0):
            nextUrls = ArticleUtils.getNextPageUrl(nextPageRegex,response)
            if len(nextUrls) > 0 and StringUtils.isNotEmpty(nextUrls[0]):
                targetNextUrl = nextUrls[0]
                self.log("nextPage %s" % targetNextUrl)
                meta["pageNumber"] = meta["pageNumber"]+1
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
        if contentPageNumber <=1:
            detailData["url"] = url
        autoDetailData = {}
        if "autoDetailData" in meta:
            autoDetailData = meta["autoDetailData"]

        contentAutoDetailData = ArticleUtils.getAutoDetail(contentPageNumber, response, enableDownloadImage,enableSnapshot)

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
            if itemValues is not None and len(itemValues) > 0 and itemValues[0] is not None and StringUtils.isNotEmpty(StringUtils.trim(str(itemValues[0]))):
                itemValue = itemValues[0]
            if itemValue is None:
                continue
            contentData[k] = itemValue
            if "content" == k:
                pageContent = itemValue
                maxPageNumber = v[-1].maxPageNumber
                if enableDownloadImage:
                    images = ArticleUtils.getContentImages(v,response)
                    if images is not None and len(images) > 0:
                        contentData["contentImages"] = images
                        # ArticleUtils.mergeDict(detailData,"contentImages",images)
                contentSnapshots = ArticleUtils.getResponseFieldValue("contentSnapshot",True,v,response)
                if contentSnapshots is not None and len(contentSnapshots) > 0 and StringUtils.isNotEmpty(contentSnapshots[0]):
                    if enableSnapshot:
                        contentData["contentSnapshot"] = contentSnapshots[0]
                        # ArticleUtils.mergeDict(detailData,"contentSnapshot",contentSnapshots[0])


        if pageContent is not None and StringUtils.isEmpty(ArticleUtils.removeAllTag(pageContent)):
            pageContent = None
        if pageContent is None and "content" in contentAutoDetailData and StringUtils.isNotEmpty(contentAutoDetailData["content"]):
            pageContent = ArticleUtils.removeAllTag(contentAutoDetailData["content"])
            if StringUtils.isEmpty(pageContent):
                pageContent = None

        if pageContent is None and nocontentRender == 1 and not ArticleUtils.isRender(meta,self.name):
            metaCopy = meta.copy()
            metaCopy["renderType"] = 1
            metaCopy["renderSeconds"] = 5
            metaCopy["detailData"] = detailData
            metaCopy["autoDetailData"] = autoDetailData
            self.log("re render url %s" % url)
            #获取不到正文，尝试使用js渲染方式，针对网站部分链接的详情页使用js跳转
            yield scrapy.Request(url=url, meta=metaCopy, callback=self.parseDetail,dont_filter=True)
        else:
            ArticleUtils.mergeNewDict(detailData,contentData)
            ArticleUtils.mergeNewDict(autoDetailData, contentAutoDetailData)
            # with open(file="/home/yhye/tmp/crawl_data_policy/" + ArticleUtils.getArticleId(response.url) + ".html", mode='w') as f:
            #     f.write("".join(response.xpath("//html").extract()))

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
                self.log("detail nextPage %s %s" % (str(contentPageNumber+1),targetNextUrl))
                yield scrapy.Request(url=targetNextUrl, meta=meta, callback=self.parseDetail)
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
                if "title" not in item or StringUtils.isEmpty(item["title"]):
                    item["title"] = response.xpath("//title//text()")

                yield item


    def closed(self,reason):
        self.log("on close start stat seeds %s %s" % (self.crawlId,self.name))
        self.log(reason)
        if self.crawlId > 0:
            self.seedDB.stat_seed(self.crawlId)
        self.log("%s %s stat seeds finished" % (self.crawlId,self.name))