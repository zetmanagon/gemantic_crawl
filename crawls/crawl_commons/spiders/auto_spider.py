from readability import Document
from newspaper import Article
import scrapy
import xlrd
import time
import re
from urllib.parse import urljoin
import jieba
import json
from crawl_commons.spiders.abstract_spider import AbstractSpider
from crawl_commons.items import CrawlResultItem
from crawl_commons.repository.seed import *
from crawl_commons.repository.crawl import *
from crawl_commons.utils.article_util import *
from crawl_commons.utils.string_util import *
from crawl_commons.utils.time_util import *


# 一级页面抓取通用爬虫，该爬虫不作爬取


class AutoSpider(scrapy.Spider, AbstractSpider):  # 需要继承scrapy.Spider类
    name = "auto_spider"  # 定义蜘蛛名

    def __init__(self, name=None, **kwargs):
        scrapy.Spider.__init__(self, name=name, kwargs=kwargs)
        AbstractSpider.__init__(self, self.name)
        # super(AutoSpider, self).__init__(name=name, kwargs=kwargs)
        # self.seedDB = SeedRepository()
        # self.crawlDB = CrawlRepository()

    def start_requests(self):  # 由此方法通过下面链接爬取页面
        return self.do_start_requests()

    def parse(self, response):
        '''起始页面解析'''
        # 起始页面url抽取'''
        meta = response.meta
        start_url = meta["seedInfo"].url
        link_list = self.get_list_urls(start_url, response)
        for url in link_list.keys():
            metaCopy = meta.copy()
            metaCopy['anchorText'] = link_list[url]
            metaCopy['parse'] = 'detail'
            if not ArticleUtils.isFile(url):
                yield self.do_request(url=url, meta=metaCopy)
            else:
                metaCopy['title'] = metaCopy['anchorText']
                metaCopy['publishAt'] = TimeUtils.getNowMill()
                self.crawlDB.saveFileCrawlDetail(metaCopy, url)
                # item = self.parseFileurl(url=url, meta=metaCopy)
                # self.crawlDB.saveCrawlDetail(item)
            # yield scrapy.Request(url=url, meta=meta, callback=self.parseDetail)
        if self.isHistory:
            # 如果有下一页,爬下一页
            nextpage_urls = ArticleUtils.getNextPageUrl('', response)
            for url in nextpage_urls:
                self.log("nextPage %s" % url)
                # time.sleep(20)
                meta['is_Nextpage'] = True
                yield self.do_request(url=url, meta=meta, cleanup=True)
                # yield scrapy.Request(url=url, meta=meta, callback=self.parse)

    # def parseFileurl(self, url, meta):
    #     '''
    #     处理正文页是纯文件的response
    #     @param response：
    #     @return item
    #     '''
    #     detailData = {}
    #     if "detailData" in meta:
    #         detailData = meta["detailData"]
    #     if len(detailData) <= 0:
    #         detailData["title"] = meta['anchorText'].strip()
    #         if detailData["title"].find('...') != -1 or detailData["title"] == '':
    #             detailData["title"] = "NoNameFile"
    #         # ts = time.strptime(meta["timestamp"], "%Y-%m-%d %H-%M-%S")
    #         # ts = int(time.mktime(ts)) * 1000
    #         detailData["publishAt"] = TimeUtils.getNowMill()
    #         detailData["url"] = url
    #     # detailData["html"] = "This page doesn't have content, it's a file's url."
    #     # ArticleUtils.mergeDict(detailData, "content", "This page doesn't have content, it's a file url.")
    #     # file = {url: {"id": ArticleUtils.getArticleId(url), "name": detailData["title"], "contentUrl": url, "url": url}}
    #     # ArticleUtils.mergeDict(detailData, "contentFiles", file)
    #     # item = ArticleUtils.meta2item(meta, detailData["url"])
    #     # for (k, v) in detailData.items():
    #     #     itemValue = None
    #     #     if "category" == k and k in item:
    #     #         itemValue = item[k] + "/" + v
    #     #     elif "contentImages" == k or "contentFiles" == k:
    #     #         itemValue = json.dumps(list(v.values()), ensure_ascii=False)
    #     #     else:
    #     #         itemValue = v
    #     #     item[k] = itemValue
    #     return item

    def parseDetail(self, response):
        '''
        详情页解析
        '''
        meta = response.meta
        url = response.url
        seed = meta["seedInfo"]
        enableDownloadFile = False
        enableDownloadImage = False
        enableSnapshot = False
        if seed.enableDownloadFile == 1:
            enableDownloadFile = True
        if seed.enableDownloadImage == 1:
            enableDownloadImage = True
        if seed.enableSnapshot == 1:
            enableSnapshot = True
        detailData = {}
        html = "".join(response.xpath("//html").extract())
        doc = Document(html)  # 利用readabilty处理文件
        if "detailData" in meta:
            detailData = meta["detailData"]
        if len(detailData) <= 0:
            # '''是否用readabilty的title'''
            # detailData["title"] = doc.title()  # 详情第一页时读入标题和url
            # if len(detailData["title"]) <= len(meta['anchorText']):
            #     detailData["title"] = meta['anchorText']
            detailData["title"] = meta['anchorText'].strip()
            if detailData["title"].find('...') != -1 or detailData["title"] == '':
                detailData["title"] = doc.title()
            detailData["publishAt"] = TimeUtils.get_conent_time(ArticleUtils.removeTag4Content(html))
            if detailData["publishAt"] == '':
                ts = time.strptime(meta["timestamp"], "%Y-%m-%d %H-%M-%S")
                ts = str(int(time.mktime(ts)) * 1000)
                detailData["publishAt"] = ts
            detailData["url"] = url
        content_snap = doc.summary()
        useNewspapaer = False  # 是否使用了newspaper
        if len(ArticleUtils.removeTag4Content(content_snap).strip()) < 3:
            article = Article(response.url, language='zh', keep_article_html=True, fetch_images=False)
            article.download(input_html=response.text)
            article.parse()
            content = article.text
            content_snap = article.article_html
            useNewspapaer = True
        # 获取正文
        if useNewspapaer == False:
            content = ArticleUtils.removeTag4Content(content_snap)  # 如果没用newspaper，将快照去标签作正文
        ArticleUtils.mergeDict(detailData, "content", content)
        if enableDownloadImage:
            images = ArticleUtils.get_content_image_urls(content_snap, url)
            if images is not None and len(images) > 0:
                ArticleUtils.mergeDict(detailData, "contentImages", images)
        if enableDownloadFile:
            files = ArticleUtils.getContentFiles(response)
            if files is not None and len(files) > 0:
                ArticleUtils.mergeDict(detailData, "contentFiles", files)
        if enableSnapshot:
            ArticleUtils.mergeDict(detailData, "contentSnapshot", content_snap)
        # 爬取下一页
        nextpage_urls = ArticleUtils.getNextPageUrl('', response)
        if len(nextpage_urls) != 0:
            meta["detailData"] = detailData
            yield scrapy.Request(url=nextpage_urls, meta=meta, callback=self.parseDetail)
        else:
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
            item['html'] = html
            item["headTitle"] = StringUtils.trim(
                ArticleUtils.removeAllTag("".join(response.xpath("//title//text()").extract())))
            yield item

    def get_list_urls(self, starturl, response):
        print('*******************************************')
        print(starturl)
        '''
        从初始页面中提取列表url
        @param starturl：初始url
        @parm response
        @return url 字典{url：锚文本}
        '''
        a_tags = response.xpath('//a')
        print('-------------------------------')
        print('所有的链接数目', len(a_tags))

        href_parent = self.getSameParent(starturl, a_tags, fine=False)
        final_urls = self.listFilter(href_parent, 10.8, 5.5, only=True, max=False)
        print('过滤后的链接数目', len(final_urls))
        if len(final_urls) == 0:
            final_urls = self.listFilter(href_parent, 10.8, 5.5, only=True, max=True)
            print('max过滤后的链接数目', len(final_urls))
        return final_urls

    def listFilter(self, href_parent, averageLength, averageWordCounts, only, max):
        '''
        列表筛选
        @
        @averageLength:阈值平均长度
        @averageWordCounts：阈值平均字数
        @only：是否只去一个列表页
        @max:是否允许在没有符合要求数据时，取平均长度最长列表
        @return：urls
        '''
        listList = []  # 列表的列表
        fibbden = [10.375, 10.1875]  # 禁用的，重构后用静态
        maxLength = 0
        maxName = ''
        for father_node in href_parent.keys():
            urls = dict()
            child_count = 0
            child_total_length = 0
            word_count = 0
            print('------------------------')
            for child_tag, text, length, href in href_parent[father_node]:
                child_count += 1
                child_total_length += len(text.strip())
                word_count += len(" ".join(jieba.cut(text.strip())).split(" "))
                # 链接描述平均字数和次数都大于阈值
                print(text, '|', href)
            # 记录max
            if (child_total_length / child_count) > maxLength and child_count != 1 and (
                    child_total_length / child_count) not in fibbden:
                maxLength = child_total_length / child_count
                maxName = father_node
            print(father_node, child_count, child_total_length / child_count, word_count / child_count)
            if child_total_length / child_count > averageLength and word_count / child_count > averageWordCounts and child_count > 1:
                print("ture")

                for _, text, _, href in href_parent[father_node]:
                    urls[href] = text
                    print('------------------------')
                listList.append(urls)
        print('-------------------------------')
        final_list = dict()
        if max is True:
            for _, text, _, href in href_parent[maxName]:
                final_list[href] = text
            return final_list
        if only is True:
            for l in listList:
                if len(l) > len(final_list):
                    final_list = l
            return final_list
        else:
            for l in listList:
                for u in l:
                    final_list.update(u)
            return final_list

    def getSameParent(self, starturl, a_tags, fine):
        '''
        获取拥有统一父标签的链接字典
        @a_tags：a标签
        @fine:精细模式
        @return：href_parent字典，（父节点名称：【urls】）
        '''
        href_parent = dict()
        if fine is True:
            i = 0
            lastname = ''
        for a_tag in a_tags:
            # 抽取href，过滤掉无效链接
            href = a_tag.xpath('@href').extract_first()
            if href is None:
                continue

            # 获取a标题文本内容，无内容的链接不抓取
            text = a_tag.xpath('text()').extract_first()
            if text is None:
                continue
            if len(text.strip()) == 0:
                continue

            # 相对地址绝对化
            if 'http' not in href:
                href = urljoin(starturl, href)

            # 获取父节点
            treePath = ''
            father_tag = a_tag.xpath('..')
            while father_tag.xpath('local-name(.)').extract_first() is not None:
                treePath = treePath + str(father_tag.xpath('local-name(.)').extract_first())
                if (father_tag.xpath('@*') is not None):
                    treePath = treePath + self.classExtract(father_tag.xpath('@*').extract_first())
                father_tag = father_tag.xpath('..')

            father_name = treePath
            if father_name is not None:
                father_name = '<' + father_name
                for index, attribute in enumerate(father_tag.xpath('@*'), start=0):
                    attribute_name = father_tag.xpath('name(@*[%d])' % index).extract_first()
                    father_name += ' ' + attribute_name + "=" + attribute.extract()
                father_name += '>'
                if fine is True:
                    if father_name not in href_parent:
                        lastname = father_name
                        href_parent[father_name] = [(a_tag, text, len(text), href)]
                        print(father_name + ":" + href)
                    elif father_name == lastname or lastname.endswith(father_name) == True:
                        href_parent[lastname].append((a_tag, text, len(text), href))
                        print(lastname + ":" + href)
                    else:
                        father_name = str(i) + father_name
                        i = i + 1
                        href_parent[father_name] = [(a_tag, text, len(text), href)]
                        print(father_name + ":" + href)
                        lastname = father_name
                else:
                    if father_name not in href_parent:
                        href_parent[father_name] = [(a_tag, text, len(text), href)]
                    else:
                        href_parent[father_name].append((a_tag, text, len(text), href))
        return href_parent

    def classExtract(self, xpath):
        '''在这里加规则增加列表识别的适配性'''
        '''基金协会适配'''
        if str(xpath).startswith('newsList'):
            return 'None'
        ''' 中国政府网适配 '''
        if str(xpath) == 'line':
            return 'None'
        return str(xpath)

    def closed(self, reason):
        self.do_closed(reason)