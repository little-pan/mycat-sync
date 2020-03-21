# [Mycat-sync](https://github.com/little-pan/mycat-sync)

A branch of [MyCAT](http://mycat.io/). Mycat sync server is based on synchronized IO(BIO) for simplity and the most
important goal of database shard and read/write separation instead of complicated and error-prone non-blocking IO(NIO).

NIO is very complex and obscure, not very useful for databaes middlewares such as mycat. For product of database, too
many connections only tremendously consume system resources, lead to more instability.

Mycat-sync server uses BIO, keep dozens to hundreds of connections and a few threads, then earn more simple and
stability, also take full advantage of existing stable components such as massive JDBC driver and connection pool.

## Features

* Keeps simple and stable
* Supports the most important features of [MyCAT](http://mycat.io/)

## Document

There are some documents in Mycat-doc project on github at [Mycat-doc](https://github.com/MyCATApache/Mycat-doc).
