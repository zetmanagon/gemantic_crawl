#!/bin/sh
rm -rf crawl_commons.tar.gz
rm -rf crawl_commons/__pycache__/
rm -rf crawl_commons/repository/__pycache__/
rm -rf crawl_commons/spiders/__pycache__/
rm -rf crawl_commons/utils/__pycache__/
rm -rf crawl_commons/monitor/__pycache__/
tar cvfz crawl_commons.tar.gz crawl_commons
scp crawl_commons.tar.gz crawl@10.0.0.36:/data/gemantic_crawl/crawls

