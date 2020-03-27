# [Mycat-sync](https://github.com/little-pan/mycat-sync)

A branch of [MyCAT](https://github.com/MyCATApache/Mycat-Server). Mycat sync is based on single thread synchronous 
non-blocking IO for simplicity and the most important goal of database shard and read/write separation instead of 
complicated and error-prone multi-thread asynchronous non-blocking IO.

Database middleware such as mycat, mainly choices one of four thread models:
1) Synchronous blocking mode: frontend processor thread  + BIO -> the same processor thread  + BIO <br/>
Require massive threads in frontend, high overhead and it's inefficient in backend when handling multi-node.

2) Asynchronous blocking mode: frontend processor thread + BIO -> one or more business threads + BIO in backend <br/>
Require massive threads in frontend, high overhead and need to consider multi-thread issue. It's complicated, inefficient 
and error-prone.

3) Synchronous non-blocking mode: frontend processor thread + NIO -> the same processor thread + NIO in backend <br/>
Require a few threads in system, less overhead and needn't to consider multi-thread issue. It's simple and efficient. 
Mycat sync uses this mode.

4) Asynchronous non-blocking mode: frontend processor thread + NIO -> one or more business threads + NIO in backend <br/>
Require more threads in system, less overhead and need to consider multi-thread issue. It's complicated, inefficient 
and error-prone. Mycat server uses this mode.

Mycat sync uses the third execution mode, can keep a few threads and massive connection, so that it not only saves memory 
and file resources, but also simplifies the SQL processing of mycat. In this mode, mycat sync only support native backend
such as PostgreSQL and MySQL(native protocol implementation), not includes JDBC backend.

## Features

* Keeps simple, stable and high performance
* Supports the most important features of [MyCAT](https://github.com/MyCATApache/Mycat-Server) such as database shard
and read/write separation

## Document

There are some documents in Mycat-doc project on github at [Mycat-doc](https://github.com/MyCATApache/Mycat-doc).
