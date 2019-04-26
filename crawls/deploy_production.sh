#!/bin/sh
./stop.sh
SUFFIX=`date +%Y%m%d-%H%M`
tar cvfz crawl_commons_${SUFFIX}.tar.gz crawl_commons
rm -rf crawl_commons
tar xvf crawl_commons.tar.gz
./start.sh
cd ../crawls_test
./deploy.sh
