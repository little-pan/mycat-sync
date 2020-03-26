# [Mycat-sync](https://github.com/little-pan/mycat-sync)介绍

[MyCAT](https://github.com/MyCATApache/Mycat-Server)的一个分支。 Mycat sync server为了简单性和数据库分片、读写分离这一最重要目标，
使用同步非阻塞IO模型执行SQL，即单个事务始终在单线程上非阻塞地执行，而不是复杂的易出错的异步非阻塞IO（mycat后端为多线程异步处理，比较复杂）。

像mycat这样的数据库中间件，主要有4种线程模型可选择：
1）前端阻塞 + 后端单线程同步阻塞  前端需要使用大量的线程，后端处理多个连接时不高效
2）前端阻塞 + 后端多线程异步阻塞  前端需要使用大量的线程，后端处理需要考虑多线程问题，复杂且容易出错
3）前端非阻塞 + 后端单线程同步非阻塞 前端只需使用少量的线程，后端处理不用考虑多线程问题，简单、高效。 Mycat sync使用的模型
4）前端非阻塞 + 后端多线程异步非阻塞 前端只需使用少量的线程，后端处理需要考虑多线程问题，复杂且容易出错。 Mycat使用的模型
Mycat sync使用第3种执行模型，可以保持少量的线程和大量的连接，既节省了内存和文件资源，又简化了mycat的SQL处理过程。在这种模式下，mycat sync
进行了取舍，目前只支持native后端（postgreSQL和MySQL native协议实现），不再支持JDBC后端。

## Mycat的关键特性：

* 保持简单、稳定和高效
* 支持[MyCAT](https://github.com/MyCATApache/Mycat-Server)最重要的特性，如分库分表、读写分离

## 文档：

github上面的Mycat-doc项目是相关文档 [https://github.com/MyCATApache/Mycat-doc](https://github.com/MyCATApache/Mycat-doc)
