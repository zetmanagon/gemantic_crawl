# @Date:   2018-12-24T10:24:01+08:00
# @Email:  tang@jeffery.top
# @Last modified time: 23-Jan-2019
# @Copyright: jeafi

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


class AutoSpider(scrapy.Spider,AbstractSpider):  # 需要继承scrapy.Spider类
    name = "auto_spider"  # 定义蜘蛛名
    # timestamp = time.strftime('%Y-%m-%d %H-%M-%S',time.localtime(time.time()))


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
            yield self.do_request(url=url, meta=metaCopy)
            # yield scrapy.Request(url=url, meta=meta, callback=self.parseDetail)
        if self.isHistory:
            # 如果有下一页,爬下一页
            nextpage_urls = ArticleUtils.getNextPageUrl('', response)
            for url in nextpage_urls:
                self.log("nextPage %s" % url)
                # time.sleep(20)
                meta['is_Nextpage']=True
                yield self.do_request(url=url, meta=meta,cleanup=True)
                # yield scrapy.Request(url=url, meta=meta, callback=self.parse)

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
        article = Article(response.url, language='zh')
        article.download(input_html=response.text)
        article.parse()
        # doc = Document(html)   # 利用readabilty处理文件
        if "detailData" in meta:
            detailData = meta["detailData"]
        if len(detailData) <= 0:
            # '''是否用readabilty的title'''
            # detailData["title"] = doc.title()  # 详情第一页时读入标题和url
            # if len(detailData["title"]) <= len(meta['anchorText']):
            #     detailData["title"] = meta['anchorText']
            detailData["title"] = meta['anchorText'].strip()
            if detailData["title"].find('...') != -1:
                detailData["title"] = article.title
            detailData["publishAt"] = TimeUtils.get_conent_time(html)
            detailData["url"] = url
        # content_snap = doc.summary()
        content_snap = article.text
        # 获取正文
        content = ArticleUtils.removeTag4Content(content_snap)
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
        if StringUtils.isNotEmpty(nextpage_urls):
            meta["detailData"] = detailData
            self.do_request(url=url, meta=meta)
            # yield scrapy.Request(url=nextpage_urls, meta=meta, callback=self.parseDetail)
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

            yield item

        '''
        调试代码段，将没找到时间戳的url输出到文件调试
        '''
        # if(item["publishAt"]) == '':
        #     f = open("urlwithoutime.txt",'a+', encoding = "utf8")
        #     f. write(meta["seedInfo"]+'\n')
        # item["seed"] = meta["url"]

    # def auto_meta2item(cls, meta, url):
    #     seed = meta["seedInfo"]
    #     referer = seed.url
    #     item = CrawlResultItem()
    #     item["mediaFrom"] = seed.mediaFrom
    #     item["referer"] = referer
    #     item["url"] = url
    #     item["site"] = seed.site
    #     item["timestamp"] = meta["timestamp"]
    #     # listData = meta["listData"]
    #     item["category"] = seed.category
    #     item["urlLabel"] = seed.urlLabel.split(",")
    #     item["crawlName"] = seed.crawlName
    #     if StringUtils.isNotEmpty(seed.organization):
    #         item["organization"] = seed.organization
    #     return item

    # def get_image_urls(self, html, source_url):
    #     '''
    #     @param html:正文html
    #     @param url:本站地址
    #     @return ：图片字典dict
    #     '''
    #     replace_pattern = r'<[img|IMG].*?/>'   # img标签的正则式
    #     img_url_pattern = r'.+?src="(\S+)"'  # img_url的正则式
    #     img_url_list = []
    #     need_replace_list = re.findall(replace_pattern, html)  # 找到所有的img标签
    #     for tag in need_replace_list:
    #         url = re.findall(img_url_pattern, tag)
    #         if url != []:
    #             img_url_list.append(url[0])  # 找到所有的img_url
    #     imageDict = {}
    #     for img in img_url_list:
    #         if StringUtils.isEmpty(img):
    #             continue
    #         if img.startswith("data:"):
    #             continue
    #         # 图片以二进制格式写在网页,不需要下载
    #         url = ArticleUtils.getFullUrl(img, source_url)
    #         imageDict[url] = {"id":ArticleUtils.getArticleId(url),"contentUrl":img,"url":url}
    #     return imageDict

    # def get_time(self, response):
    #     '''
    #     提取时间,并转化为时间戳
    #     @param response
    #     @return 时间戳
    #     '''
    #     link_list =re.findall(r"((\d{4}|\d{2})(\-|\/)\d{1,2}\3\d{1,2})(\s?\d{2}:\d{2})?|(\d{4}年\d{1,2}月\d{1,2}日)(\s?\d{2}:\d{2})?" ,response.text)
    #     time_get = ''
    #     if link_list != []:
    #         time_get = link_list[0][0]
    #         for ele in link_list[0]:
    #             if time_get.find(ele) == -1:
    #                 time_get += ele
    #         time_get = TimeUtils.convert2Mill4Default(time_get,"")
    #     return time_get

    # def get_seed_fromxls(self):
    #     '''
    #     从xls中读初始url
    #     '''
    #     data = xlrd.open_workbook('seed.xls')
    #     table = data.sheet_by_name(u'未抓取链接')
    #     seeds = []
    #     for seed in table.col_values(3):
    #         seeds.append(WebSeed(seed))
    #     return seeds
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
        # print(href_parent)
        # 返回的url列表
        final_urls = self.listFilter(href_parent, 10.8, 5.5, only=True, max=False)
        # if final_urls == []:
        #     href_parent = self.getSameParent(starturl, a_tags, fine=False)
        # print(href_parent)
        # 返回的url列表
        # for url in final_urls:
        #     regex = r"(maps?)|(ads)|(adverti(s|z)e(ment))|(outerlink)|(redirect(ion)?)"
        #     pattern = re.compile(regex)
        #     if pattern.search(url):
        #         final_urls.pop(url)
        print('过滤后的链接数目', len(final_urls))
        # if len(final_urls) == 0:
        #     final_urls = self.get_list_urls2(starturl, response)
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
                    # print(child_tag)
                    # print('遍历数据', text, length)
                    child_total_length += len(text.strip())
                    # print(" ".join(jieba.cut(text.strip())))
                    word_count += len(" ".join(jieba.cut(text.strip())).split(" "))
                    # 链接描述平均字数和次数都大于阈值

                    print(text, '|', href)
                if (child_total_length / child_count) > maxLength:
                    maxLength = child_total_length / child_count
                    maxName = father_node
                print(father_node, child_count, child_total_length / child_count, word_count / child_count)
                if child_total_length / child_count > averageLength and word_count / child_count > averageWordCounts and child_count > 1:
                    # count += len(href_parent[father_node])
                    print("ture")

                    for _, text, _, href in href_parent[father_node]:
                        # post.insert_one({'seed': starturl, "text": text, "href": href})
                        # print(text, '|', href)
                        urls[href] = text
                        print('------------------------')
                    listList.append(urls)
            print('-------------------------------')
            final_list = []
            if max is True:
                for _, _, _, href in href_parent[maxName]:
                    final_list.append(href)
                return final_list
            if only is True:
                for l in listList:
                    if len(l) > len(final_list):
                        final_list = l
                return final_list
            else:
                for l in listList:
                    for u in l:
                        final_list.append(u)
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
                # print(a_tag)
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
                    if(father_tag.xpath('@*') is not None):
                        treePath = treePath + str(father_tag.xpath('@*').extract_first())
                    father_tag = father_tag.xpath('..')
                # print(treePath)
                # time.sleep(1)
                # print("********************************8")
                father_name = treePath
                if father_name is not None:
                    father_name = '<' + father_name
                    for index, attribute in enumerate(father_tag.xpath('@*'), start=0):
                        attribute_name = father_tag.xpath('name(@*[%d])' % index).extract_first()
                        father_name += ' ' + attribute_name + "=" + attribute.extract()
                        # print("a:"+attribute_name)
                    father_name += '>'
                    if fine is True:
                        if father_name not in href_parent:
                            lastname = father_name
                            href_parent[father_name] = [(a_tag, text, len(text), href)]
                            print(father_name+":"+href)
                        elif father_name == lastname or lastname.endswith(father_name) == True:
                            href_parent[lastname].append((a_tag, text, len(text), href))
                            print(lastname+":"+href)
                        else:
                            father_name = str(i) + father_name
                            i = i + 1
                            href_parent[father_name] = [(a_tag, text, len(text), href)]
                            print(father_name+":"+href)
                            lastname = father_name
                    else:
                        if father_name not in href_parent:
                            href_parent[father_name] = [(a_tag, text, len(text), href)]
                        else:
                            href_parent[father_name].append((a_tag, text, len(text), href))
            return href_parent

    def get_seed_fromxls(self):
        '''
        从xls中读初始url
        '''
        data = xlrd.open_workbook('seed.xls')
        table = data.sheet_by_name(u'未抓取链接')
        seeds = []
        for seed in table.col_values(3):
            seeds.append(WebSeed(seed))
        return seeds

    def get_list_urls2(self, starturl, response):
        # connection = pymongo.MongoClient('127.0.0.1',27017)
        # tdb = connection.template
        # post = tdb.leibiao
        '''
        从初始页面中提取列表url
        @param starturl：初始url
        @parm response
        @return url列表
        '''
        a_tags = response.xpath('//a')
        print('-------------------------------')
        print('所有的链接数目', len(a_tags))
        count = 0
        href_parent = dict()

        for a_tag in a_tags:
            # print(a_tag)
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
            father_tag = a_tag.xpath('..')[0]
            while len(father_tag):
                treePath = treePath + father_tag[0].xpath('local-name(.)')
                father_tag = father_tag[0].xpath('..')
            # print(father_tag)
            # print("********************************8")
            father_name = father_tag.xpath('local-name(.)').extract_first()
            if father_name is not None:
                father_name = '<' + father_name
                for index, attribute in enumerate(father_tag.xpath('@*'), start=0):
                    attribute_name = father_tag.xpath('name(@*[%d])' % index).extract_first()
                    father_name += ' ' + attribute_name + "=" + attribute.extract()
                    # print("a:"+attribute_name)

                father_name += '>'

                if father_name not in href_parent:
                    href_parent[father_name] = [(a_tag, text, len(text), href)]
                else:
                    href_parent[father_name].append((a_tag, text, len(text), href))

            # print(href_parent)

        # 返回的url列表
        final_urls = []
        # 父节点下面的链接进行算法提取列表页URL
        # 列表页文章标题字数和词数都要大于阈值
        for father_node in href_parent.keys():
            child_count = 0
            child_total_length = 0
            word_count = 0
            for child_tag, text, length, _ in href_parent[father_node]:
                child_count += 1
                # print(child_tag)
                # print('遍历数据', text, length)
                child_total_length += len(text.strip())
                # print(" ".join(jieba.cut(text.strip())))
                word_count += len(" ".join(jieba.cut(text.strip())).split(" "))
            # 链接描述平均字数和次数都大于阈值
            if child_total_length / child_count > 8 and word_count / child_count > 4 and child_count > 1:
                count += len(href_parent[father_node])
                print('------------------------')
                print(father_node, child_count, child_total_length / child_count, word_count / child_count)
                print("")
                for _, text, _, href in href_parent[father_node]:
                    # post.insert_one({'seed': starturl, "text": text, "href": href})
                    print(text, '|', href)
                    final_urls.append(href)
                print('------------------------')
        print('-------------------------------')

        for url in final_urls:
            regex = r"(maps?)|(ads)|(adverti(s|z)e(ment))|(outerlink)|(redirect(ion)?)"
            pattern = re.compile(regex)
            if pattern.search(url):
                final_urls.pop(url)
        print('过滤后的链接数目', len(final_urls))
        return final_urls


    def closed(self,reason):
        self.log("on close start stat seeds %s %s" % (self.crawlId,self.name))
        self.log(reason)
        if self.crawlId > 0:
            self.seedDB.stat_seed(self.crawlId)
        self.log("%s %s stat seeds finished" % (self.crawlId,self.name))