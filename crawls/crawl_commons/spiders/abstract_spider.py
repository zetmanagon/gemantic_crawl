# @Date:   12-Mar-2019
# @Email:  Tang@jeffery.top
# @Filename: abstract_spider.py
# @Last modified time: 15-Mar-2019



from crawl_commons.items import CrawlResultItem
from crawl_commons.repository.seed import *
from crawl_commons.repository.crawl import *
from crawl_commons.utils.article_util import *
from crawl_commons.utils.string_util import *
from crawl_commons.utils.http_util import *
import scrapy
import logging
import time
from crawl_commons.monitor.template_monitor import TemplateComparator
from scrapy.selector import Selector

class AbstractSpider(object):

    def __init__(self,crawl_name):
        self.LOG = logging.getLogger("abstractSpider")
        self.spiderName = crawl_name
        self.json_new_max_size = 30
        self.crawlName = crawl_name.replace("history_", "")
        self.isHistory = ArticleUtils.isHistory(self.spiderName)
        self.isStat = ArticleUtils.isStat(self.spiderName)
        self.crawlId = 0
        self.seedDB = SeedRepository()
        self.crawlDB = CrawlRepository()
        self.crawlId = self.seedDB.get_crawl_id(self.crawlName)
        self.LOG.info("crawlId=%d crawlName=%s isHistory=%s" % (self.crawlId,self.crawlName,self.isHistory))
        self.templateComparator = TemplateComparator(self.crawlDB)

    def do_start_requests(self):  # 由此方法通过下面链接爬取页面
        seeds_meta = self.get_seeds()
        for seed_meta in seeds_meta:
            url = seed_meta["seedInfo"].url
            if StringUtils.isNotEmpty(seed_meta["seedInfo"].targetUrl):
                url = seed_meta["seedInfo"].targetUrl
            yield self.do_request(url=url,meta=seed_meta,cleanup=True)
            # yield scrapy.Request(url=url, meta=seed_meta, callback=self.parse)


    def get_seeds(self,isRegex=True):
        seeds = self.seedDB.get_seed_crawlId(self.crawlId)
        timestamp = time.strftime('%Y-%m-%d %H-%M-%S', time.localtime(time.time()))  # 该次爬虫的时间戳
        # 定义爬取的链接
        for seed in seeds:
            # if seed.url != 'http://www.sse.com.cn/lawandrules/rules/law/adminis/':
            #     continue
            regex = self.seedDB.get_regex(seed.regexName)
            if isRegex and (regex is None or len(regex)<=0):
                self.LOG.infog("%s no regex" % seed.url)
                continue
            if self.isStat:
                self.crawlDB.saveCrawlStat(seed.url,self.crawlId,self.crawlName, timestamp,"detail")  # 初始化种子统计
            meta = {}
            meta["timestamp"] = timestamp
            meta["seedRegex"] = regex
            meta["depthNumber"] = 0
            meta["pageNumber"] = seed.pagingStartNumber
            meta["seedInfo"] = seed
            meta["renderType"] = seed.renderType
            meta["pageRenderType"] = seed.pageRenderType
            meta["renderSeconds"] = seed.renderSeconds
            meta["nocontentRender"] = seed.nocontentRender
            meta['is_Nextpage'] = False
            yield meta


    def do_parse_list_regex(self, response):
        meta = response.meta
        pageNumber = meta["pageNumber"]
        regexList = meta["seedRegex"]
        seed = meta["seedInfo"]
        depthNumber = int(meta["depthNumber"])
        regexDict = regexList[depthNumber].regexDict
        if pageNumber <=1 and self.isStat:
            html = "".join(response.xpath("//html").extract())
            self.crawlDB.saveCrawlStat(seed.url,self.crawlId,self.crawlName, meta["timestamp"], "list",html,depthNumber)
        if "list" not in regexDict:
            self.log("%s no list regex" % response.url)
            yield

        listRegexs = regexDict["list"]
        listRegex = listRegexs[-1]
        json_data = None
        result_format = None
        if listRegex.regexType == 'json':
            result_format,json_data = ArticleUtils.getJsonContent(listRegexs,response)
            if StringUtils.isEmpty(json_data):
                self.log("%s no json regex failed" % response.url)
                yield
        detailUrls = None
        listDataAll = {}

        if json_data is None:
            detailUrls = ArticleUtils.getResponseContents4WebRegex(listRegexs, response)
            for (k, v) in regexDict.items():
                if "nextPage" == k or "list" == k or "totalPage" == k or k.startswith("pagingUrl"):
                    continue
                itemValues = ArticleUtils.getResponseFieldValue(k, False, v, response)
                listDataAll[k] = itemValues
        else:
            detailUrls, listDataAll = ArticleUtils.getJsonFieldValues(regexDict.items(),result_format,json_data)


        if detailUrls is None or len(detailUrls)<=0:
            self.log("no detailUrls %s " % response.url)
            yield

        isDetail = True
        if depthNumber + 1 < regexList[-1].depthNumber:
            isDetail = False
        detailUrlsLength = len(detailUrls)
        for i, detailUrl in enumerate(detailUrls):
            #json数据解析，当前页只读取前30条记录
            if isDetail and not self.isHistory and json_data is not None and i >= self.json_new_max_size:
                break
            isVaildUrl = True
            if "json" != listRegex.regexType and StringUtils.isNotEmpty(listRegex.resultFilterRegex):
                isVaildUrl = re.match(listRegex.resultFilterRegex, detailUrl)
            if not isVaildUrl:
                continue
            targetUrl = ArticleUtils.getFullUrl(detailUrl, response.url)
            if ArticleUtils.isErrorUrl(targetUrl):
                continue
            if depthNumber == 0:
                targetUrl = ArticleUtils.getFullUrl(detailUrl, seed.url)
            # self.LOG.info("isDetail %s targetUrl %s" % (str(isDetail), targetUrl))
            # if domain not in targetUrl:
            #     continue
            listData = {}
            metaCopy = meta.copy()
            if "listData" in meta and len(meta["listData"]) > 0:
                listData = meta["listData"]
            for (k, v) in listDataAll.items():
                if v is None or len(v) != detailUrlsLength or v[i] is None or StringUtils.isEmpty(str(v[i])):
                    continue
                listDataValue = v[i]
                if "category" == k and k in listData:
                    listDataValue = listData["category" + "/" + listDataValue]
                if "content" == k:
                    listData["contentSnapshot"] = listDataValue
                    images = ArticleUtils.get_content_image_urls(listDataValue, targetUrl)
                    if images is not None and len(images) > 0:
                        listData["contentImages"] = json.dumps(list(images.values()), ensure_ascii=False)
                    selector = Selector(text=listDataValue)
                    files = ArticleUtils.getContentFiles(selector,targetUrl)
                    if files is not None and len(files)>0:
                        listData["contentFiles"] = json.dumps(list(files.values()), ensure_ascii=False)
                    listDataValue = ArticleUtils.removeTag4Content(listDataValue)
                    # print("listDataValue="+listDataValue)
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
                self.crawlDB.saveFileCrawlDetail(metaCopy, targetUrl)
            elif "content" in listData and StringUtils.isNotEmpty(listData["content"]):
                # print("--------------",targetUrl)
                metaCopy['parse'] = 'detail'
                self.crawlDB.saveFileCrawlDetail(metaCopy, targetUrl)
            elif isDetail:
                metaCopy['parse'] = 'detail'
                yield self.do_request(url=targetUrl,meta=metaCopy)
            else:
                self.LOG.info("next level %s" % targetUrl)
                yield self.do_request(url=targetUrl, meta=metaCopy,cleanup=True)


        if self.isHistory:

            if StringUtils.isNotEmpty(seed.pagingUrl):
                if "totalPage" not in regexDict:
                    if len(detailUrls) > 0:
                        pageNumber = pageNumber + 1
                        targetNextUrl = seed.pagingUrl.replace("{pageNumber}",str(pageNumber))
                        metaCopy = meta.copy()
                        metaCopy["pageNumber"] = pageNumber
                        if metaCopy["pageRenderType"] == 1:
                            metaCopy["pageRenderType"] = 0
                        self.LOG.info("pagingUrl %d %s" % (pageNumber, targetNextUrl))
                        yield self.do_request(url=targetNextUrl, meta=metaCopy)
                elif pageNumber <= 1:
                    totalPageRegexs = regexDict["totalPage"]
                    totalPageNumber = ArticleUtils.getTotalPage(totalPageRegexs, json_data, response)
                    self.LOG.info("totalPageNumber %d" % totalPageNumber)
                    if totalPageNumber > 1:
                        while totalPageNumber > pageNumber:
                            pageNumber = pageNumber + 1
                            targetNextUrl = seed.pagingUrl.replace("{pageNumber}",str(pageNumber))
                            if StringUtils.isEmpty(targetNextUrl):
                                break
                            metaCopy = meta.copy()
                            metaCopy["pageNumber"] = pageNumber
                            if metaCopy["pageRenderType"] == 1:
                                metaCopy["pageRenderType"] = 0
                            self.LOG.info("pagingUrl %d %s" % (pageNumber, targetNextUrl))
                            yield self.do_request(url=targetNextUrl, meta=metaCopy)


            elif "pagingUrl" in regexDict:
                pagingUrlRegexs = regexDict["pagingUrl"]
                if "totalPage" not in regexDict:
                    pagingUrlParams = ArticleUtils.getPaggingUrlParams(regexDict, response)
                    # if len(detailUrls) > 0:
                    targetNextUrl = ArticleUtils.getNextPaggingUrl(pageNumber, 10000, pagingUrlRegexs,
                                                                   pagingUrlParams, seed.url)
                    if StringUtils.isNotEmpty(targetNextUrl):
                        metaCopy = meta.copy()
                        pageNumber = pageNumber + 1
                        metaCopy["pageNumber"] = pageNumber
                        if metaCopy["pageRenderType"] == 1:
                            metaCopy["pageRenderType"] = 0
                        self.LOG.info("pagingUrl %d %s" % (pageNumber, targetNextUrl))
                        yield self.do_request(url=targetNextUrl, meta=metaCopy)
                elif pageNumber <=1:
                    totalPageRegexs = regexDict["totalPage"]
                    totalPageNumber = ArticleUtils.getTotalPage(totalPageRegexs,json_data,response)
                    self.LOG.info("totalPageNumber %d" % totalPageNumber)
                    pagingUrlParams = ArticleUtils.getPaggingUrlParams(regexDict,response)
                    self.LOG.info(pagingUrlParams)
                    if totalPageNumber > 1:
                        while totalPageNumber > pageNumber:
                            targetNextUrl = ArticleUtils.getNextPaggingUrl(pageNumber,totalPageNumber,pagingUrlRegexs,pagingUrlParams,seed.url)
                            if StringUtils.isEmpty(targetNextUrl):
                                break
                            metaCopy = meta.copy()
                            pageNumber = pageNumber+1
                            metaCopy["pageNumber"] = pageNumber
                            if metaCopy["pageRenderType"] == 1:
                                metaCopy["pageRenderType"] = 0
                            self.LOG.info("pagingUrl %d %s" % (pageNumber,targetNextUrl))
                            yield self.do_request(url=targetNextUrl, meta=metaCopy)

            else:
                nextPageRegex = None
                if "nextPage" in regexDict:
                    nextPageRegex = regexDict["nextPage"]
                nextUrls = ArticleUtils.getNextPageUrl(nextPageRegex,response, pageNumber)
                if len(nextUrls) > 0 and StringUtils.isNotEmpty(nextUrls[0]):
                    nextPageUrl = nextUrls[0]
                    pageNumber = pageNumber + 1
                    self.LOG.info("nextPageUrl %d %s" % (pageNumber, nextPageUrl))
                    meta["pageNumber"] = pageNumber
                    yield self.do_request(url=nextPageUrl, meta=meta)



    def do_request(self,url, meta,dont_filter=False,cleanup=False):
        if "parse" in meta and meta["parse"] == "detail" :
            return scrapy.Request(url=url, meta=meta, callback=self.parseDetail,dont_filter=dont_filter)
        else:
            return scrapy.Request(url=url, meta=meta, callback=self.parse,dont_filter=dont_filter)



    def do_parse_detal_regex(self, response):
        meta = response.meta
        url = response.url
        regexList = meta["seedRegex"]
        regexDict = regexList[-1].regexDict
        seed = meta["seedInfo"]
        listData = {}
        if "listData" in meta:
            listData = meta["listData"]
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

        contentAutoData = None
        html = "".join(response.xpath("//html").extract())
        html_body = ArticleUtils.removeHtmlSpecialTag(ArticleUtils.removeHtmlComment("".join(response.xpath("//html//body").extract())),response)
        html_remove = ArticleUtils.removeHtmlSpecialTag4Content(ArticleUtils.removeHtmlComment(html), response)
        meta["autoDetailData"] = autoDetailData
        maxPageNumber = 0
        pageContent = ""
        pageContentImages = None
        contentData = {}
        if enableDownloadFile:
            files = ArticleUtils.getContentFiles(response,response.url)
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
                pageContent = ArticleUtils.removeAllTag(itemValue)
                maxPageNumber = v[-1].maxPageNumber
                images = ArticleUtils.getContentImages(v, response)
                if images is not None and len(images) > 0:
                    contentData["contentImages"] = images
                    pageContentImages = images

                        # ArticleUtils.mergeDict(detailData,"contentImages",images)
                contentSnapshots = ArticleUtils.getResponseFieldValue("contentSnapshot", True, v, response)
                if contentSnapshots is not None and len(contentSnapshots) > 0 and StringUtils.isNotEmpty(contentSnapshots[0]):
                    if enableSnapshot:
                        contentData["contentSnapshot"] = contentSnapshots[0]
                        # ArticleUtils.mergeDict(detailData,"contentSnapshot",contentSnapshots[0])

        if StringUtils.isEmpty(pageContent) and pageContentImages is None:
            if contentAutoData is None:
                contentAutoData = ArticleUtils.getAutoDetail(contentPageNumber, html_remove, enableDownloadImage,enableSnapshot)
            if "contentImages" in contentAutoData:
                pageContentImages = contentAutoData["contentImages"]
            if "content" in contentAutoData:
                pageContent = ArticleUtils.removeAllTag(contentAutoData["content"])


        if StringUtils.isEmpty(pageContent) and pageContentImages is None and nocontentRender == 1 and not ArticleUtils.isRender(meta, self.name):
            metaCopy = meta.copy()
            metaCopy["renderType"] = 1
            metaCopy["renderSeconds"] = 5
            metaCopy["detailData"] = detailData
            metaCopy["autoDetailData"] = autoDetailData
            self.LOG.info("re render url %s" % url)
            yield self.do_request(url=url,meta=metaCopy,dont_filter=True,cleanup=True)
            # 获取不到正文，尝试使用js渲染方式，针对网站部分链接的详情页使用js跳转
            # yield scrapy.Request(url=url, meta=metaCopy, callback=self.parseDetail, dont_filter=True)
        else:
            ArticleUtils.mergeNewDict(detailData, contentData)

            if contentPageNumber <=1 and "publishAt" not in detailData and "publishAt" not in autoDetailData and "publishAt" not in listData:
                autoDetailData["publishAt"] = TimeUtils.get_conent_time(html_body,-1)
            if contentAutoData is None and (("title" not in detailData and "title" not in listData) or (StringUtils.isEmpty(pageContent)) and pageContentImages is None):
                contentAutoData = ArticleUtils.getAutoDetail(contentPageNumber,html_remove, enableDownloadImage, enableSnapshot)
            ArticleUtils.mergeNewDict(autoDetailData, contentAutoData)


            # with open(file="/home/yhye/tmp/crawl_data_policy/" + ArticleUtils.getArticleId(response.url) + ".html", mode='w') as f:
            #     f.write("".join(response.xpath("//html").extract()))

            nextPageRegex = []
            if "nextPage" in regexDict:
                nextPageRegex = regexDict["nextPage"]
                maxPageNumber = nextPageRegex[-1].maxPageNumber
            targetNextUrl = ""
            if maxPageNumber <= 0 or (maxPageNumber > 0 and contentPageNumber < maxPageNumber):
                meta["contentPageNumber"] = contentPageNumber + 1
                nextUrls = ArticleUtils.getNextPageUrl(nextPageRegex, response,meta["contentPageNumber"])
                if len(nextUrls) > 0 and StringUtils.isNotEmpty(nextUrls[0]):
                    targetNextUrl = nextUrls[0]
            #防止死循环翻页
            if StringUtils.isNotEmpty(targetNextUrl) and contentPageNumber <= 100 and targetNextUrl != response.url:
                meta["detailData"] = detailData
                meta["autoDetailData"] = autoDetailData
                self.LOG.info("detail nextPage %s %s" % (str(contentPageNumber + 1), targetNextUrl))
                yield self.do_request(url=url, meta=meta,dont_filter=False,cleanup=True)
                # yield scrapy.Request(url=targetNextUrl, meta=meta, callback=self.parseDetail)
            else:
                detailImages = None
                if "contentImages" in detailData:
                    detailImages = detailData["contentImages"]
                item = ArticleUtils.meta2item(meta, detailData["url"])
                for (k, v) in detailData.items():
                    itemValue = None
                    if "category" == k and k in item:
                        itemValue = item[k] + "/" + v
                    elif "contentImages" == k or "contentFiles" == k:
                        itemValue = json.dumps(list(v.values()), ensure_ascii=False)
                    else:
                        itemValue = v
                    item[k] = itemValue
                    if "content" == k and (StringUtils.isNotEmpty(ArticleUtils.removeAllTag(str(item[k]))) or detailImages is not None):
                        item["contentParser"] = "rules"

                for (k, v) in autoDetailData.items():
                    if detailImages is not None and ("content" == k or "contentSnapshot" == k):
                        continue
                    if "contentImages" == k and k not in item:
                        item[k] = json.dumps(list(v.values()), ensure_ascii=False)
                    elif k not in item or StringUtils.isEmpty(ArticleUtils.removeAllTag(str(item[k]))):
                        item[k] = v

                item["headTitle"] = StringUtils.trim(ArticleUtils.removeAllTag("".join(response.xpath("//title//text()").extract())))
                if "title" not in item or StringUtils.isEmpty(item["title"]):
                    item["title"] = ArticleUtils.cleanHeadTitle(item["headTitle"])
                item["html"] = html
                yield item

    def do_closed(self,reason):
        self.LOG.info("on close start stat seeds %s %s" % (self.crawlId,self.crawlName))
        self.LOG.info(reason)
        if self.crawlId > 0:
            self.seedDB.stat_seed(self.crawlId)
            if "auto_" not in self.crawlName and "test_" not in self.crawlName and not self.isHistory:
                self.templateComparator.run_task(self.crawlId)

        self.LOG.info("%s %s stat seeds finished" % (self.crawlId,self.name))
