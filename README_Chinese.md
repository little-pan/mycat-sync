# [Mycat-sync](https://github.com/little-pan/mycat-sync)介绍

[MyCAT](https://github.com/MyCATApache/Mycat-Server)的一个分支。 Mycat sync server为了简单性和数据库分片、读写分离这一最重要目标，
使用同步非阻塞IO模型执行SQL，即单个事务始终在单线程上非阻塞地执行，而不是复杂的易出错的异步非阻塞IO（mycat后端为多线程异步处理，比较复杂）。

像mycat这样的数据库中间件，主要有4种线程模型可选择：
1) 同步阻塞模型：前端processor thread + 阻塞IO -> 后端同一processor thread阻塞IO <br/>
前端需要使用大量的线程，系统消耗极大；后端处理多个连接时不高效

2) 异步阻塞模型：前端processor thread + 阻塞IO -> 后端一个或多个business thread + 阻塞IO <br/>
前端需要使用大量的线程，系统消耗极大；后端处理需要考虑多线程问题，复杂、易错、低效
  
3) 同步非阻塞模型：前端processor thread + 非阻塞IO -> 后端同一processor thread + 非阻塞IO <br/>
系统只需使用少量的线程，系统消耗低；后端处理不用考虑多线程问题，简单、高效。 Mycat sync使用的模型
  
4) 异步非阻塞模式：前端processor thread + 非阻塞IO -> 后端一个或多个business thread + 非阻塞IO <br/>
系统需要使用较多的线程，系统消耗较高；后端处理需要考虑多线程问题，复杂、易错、低效。 Mycat server使用的模型

Mycat sync使用第3种执行模型，可以保持少量的线程和大量的连接，既节省了内存和文件资源，又简化了mycat的SQL处理过程。
在这种模式下，mycat sync进行了取舍，目前只支持native后端（postgreSQL和MySQL native协议实现），不再支持JDBC后端。

## Mycat的关键特性：

* 保持简单、稳定和高效
* 支持[MyCAT](https://github.com/MyCATApache/Mycat-Server)最重要的特性，如分库分表、读写分离

## 文档：

github上面的Mycat-doc项目是相关文档 [https://github.com/MyCATApache/Mycat-doc](https://github.com/MyCATApache/Mycat-doc)
