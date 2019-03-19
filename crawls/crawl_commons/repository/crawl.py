# @Date:   11-Jan-2019
# @Email:  Tang@jeffery.top
# @Filename: crawl.py
# @Last modified time: 18-Jan-2019



import pymongo
from scrapy.utils.project import get_project_settings
from crawl_commons.utils.time_util import *
from crawl_commons.utils.annotation import *
from crawl_commons.utils.article_util import *
from crawl_commons.utils.string_util import *
from crawl_commons.repository.filedownload import *

import logging


class CrawlRepository:

    def __init__(self):
        settings = get_project_settings()

        self.client = pymongo.MongoClient(settings.get('MONGO_URI'))
        self.db = self.client[settings.get('MONGO_DB')]
        self.logger = logging.getLogger("crawlRepository")
        self.downloadDB = FileDownloadRepository()
        self.crawlDetail = "crawlDetail"
        self.crawl = "crawl"
        self.crawlSnapshot = "crawlSnapshot"
        self.crawlStat = "crawlStat"
        self.crawlDetailTest = "crawlDetailTest"
        self.crawlSnapshotTest = "crawlSnapshotTest"
        self.crawlStatTest = "crawlStatTest"
        self.crawlTest = "crawlTest"
        # self.downloadFiles = "downloadFiles"

    def saveCrawlDetail(self,item):
        now = TimeUtils.getNowMill()
        detail = dict(item)
        id = ArticleUtils.getArticleId(detail["url"])
        detail["_id"] = id;
        detail["createAt"] = now
        detail["updateAt"] = now
        isErrorPage = ArticleUtils.isErrorPage(detail)
        if isErrorPage:
            self.logger.info("errorPage %s %s" % (detail["url"], id))
            return
        if "headTitle" in detail:
            detail.pop("headTitle")
        isTest = False
        if "test_" in detail["crawlName"]:
            isTest = True
        if "html" in detail:
            detail.pop("html")
        if "timestamp" in detail:
            detail.pop("timestamp")
        if "content" in detail and StringUtils.isNotEmpty(detail["content"]):
            if "contentSnapshot" in detail:
                snapshotDetail = {"_id":id,"content":detail["contentSnapshot"],"url":detail["url"],"updateAt":now}
                if isTest:
                    self.db[self.crawlSnapshotTest].save(snapshotDetail)
                else:
                    self.db[self.crawlSnapshot].save(snapshotDetail)

                detail.pop("contentSnapshot")
            if isTest:
                self.db[self.crawlDetailTest].save(detail)
            else:
                self.db[self.crawlDetail].save(detail)

            self.logger.info("save %s %s" % (item["url"], id))
            urls = []
            if "contentImages" in detail:
                contentImages = json.loads(detail["contentImages"])
                for img in contentImages:
                    urls.append(img["url"])
                detail.pop("contentImages")
            if "contentFiles" in detail:
                contentFiles = json.loads(detail["contentFiles"])
                for fileUrl in contentFiles:
                    urls.append(fileUrl["url"])
                detail.pop("contentFiles")
            if len(urls) > 0 and "publishAt" in detail:
                self.downloadDB.download(urls,str(detail["publishAt"]))
            detail.pop("content")
            if isTest:
                self.db[self.crawlTest].save(detail)
            else:
                self.db[self.crawl].save(detail)
            # files = ArticleUtils.getDownloadFile(urls,detail["publishAt"])
            # for file in files:
            #     self.db[self.downloadFiles].save(file)
        elif ArticleUtils.isFile(detail["url"]):
            detail["fileType"] = "file"
            if isTest:
                self.db[self.crawlDetail].save(detail)
                self.db[self.crawl].save(detail)
            else:
                self.db[self.crawlDetail].save(detail)
                self.db[self.crawl].save(detail)
            if "publishAt" in detail:
                self.downloadDB.download([detail["url"]],str(detail["publishAt"]))
            # files = ArticleUtils.getDownloadFile([detail["url"]],detail["publishAt"])
            # for file in files:
            #     self.db[self.downloadFiles].save(file)
            self.logger.info("save file %s %s" % (item["url"], id))
        else :
            self.logger.info("no content %s" % (item["url"]))


    def saveFileCrawlDetail(self,meta,url):
        item = ArticleUtils.meta2item(meta,url)
        self.saveCrawlDetail(item)

    def saveCrawlStat(self, item):
        if "content" not in item:
            return
        content = ArticleUtils.removeAllTag(item["content"])
        url = item["url"]
        referer = item["referer"]
        urlSite = ArticleUtils.getSite(url)
        if urlSite not in referer:
            return
        postiveItem = 0  # 标示爬取是否成功（content是否有内容）
        if StringUtils.isNotEmpty(content):
            postiveItem = 1
        condition = {'seed': referer, 'time': item["timestamp"]}
        count = self.db[self.crawlStat].find_one(condition)  # 查询是否存在记录
        if count is None:
            self.db[self.crawlStat].save({'seed': referer, "time": item["timestamp"], "all": 1, "success": postiveItem,
                                "html": item['html']})
        else:
            if len(item['html']) > len(count['html']):
                count['html'] = item['html']
            count['all'] += 1
            count['success'] += postiveItem
            self.db[self.crawlStat].update(condition, count)

    def initCrawlStat(self, url, timestamp):
        self.db[self.crawlStat].save({'seed': url, "time": timestamp, "all": 0, "success": 0,
                                "html": ''})
