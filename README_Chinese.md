# [Mycat-sync](https://github.com/little-pan/mycat-sync)介绍

[MyCAT](http://mycat.io/)的一个分支。 Mycat sync server为了简单性和数据库分片、读写分离这一最重要目标，使用同步IO（BIO）
而不是复杂的易出错的非阻塞IO（NIO）。

NIO是非常复杂、晦涩难懂，对类似mycat这样的数据库中间件没有多大好处。对于数据库产品，太多的连接只会极大地消耗系统资源，
导致更多的不稳定。

Mycat sync server使用BIO，保持少量的线程和几十到几百的连接，充分使用诸如JDBC驱动和连接池这样已存在的稳定组件，以获得更
简单的、更稳定的服务。

## Mycat的关键特性：

*	保持简单、稳定
*	支持[MyCAT](http://mycat.io/)最重要的特性

## 文档：

github上面的Mycat-doc项目是相关文档 [https://github.com/MyCATApache/Mycat-doc](https://github.com/MyCATApache/Mycat-doc)
