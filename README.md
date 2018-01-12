# scrapycluster_url_processing
Process crawler crawl Json data from Kafka Scrapy CLuster, filter out url information and de-duplicate, then put the results into mongodb

Scrapy Cluster完成的事：
  分布式从一个Domain爬取出包含body、header、response_url等信息并时时吐入Kafka。
  
本程序完成的事：
  将从Scrapy Cluster的最后一个信息流：Kafka中获取到的包数据中过滤出所有url，并进行相似性去重，将最终得到的url数据存入mongodb中。
  
本程序的目的：
  借用Scrapy Cluter的高性能，尽可能多的搜集url资产。
