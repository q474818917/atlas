atlas
====
<<<<<<< HEAD

###This is the ElasticSearch showcases<DEV>
=======
 
###This is the ElasticSearch showcases
>>>>>>> master
 + 版本：elasticsearch 2.1.0, lucene 5.3.1 spring-data-elasticsearch 2.0.0
 + 新增了spring data elasticsearch中的elasticsearchTemplate，其中包括
  + 创建索引
  + 后续增加：根据映射创建mapping，以及索引数据操作
 + 增加一个测试的案例 dmai
  + 增加mapping
  + bulk批量建索引
  + 以及查询操作queryStringQuery，以及json映射到bean
  + 增加terms aggregation 搜索
  
###关于爬虫
####开源工具
+ httpClient：下载网页
+ jsoup：json解析html
+ reflections：自定义注解实现
+ htmlunit、Selenium：ajax抓取

####设计
+ 主进程Spider
+ queue：默认采用blockQueue，分布式采用redisQueue
+ 











