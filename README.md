# simple-kv
使用Golang（加分项）、C++、Java任意语言实现一个单机的支持事务的KV内存存储服务。

## 要求

- [x] 采用悲观锁实现
- [x] 实现SI级别的事务隔离级别 
- [x] 支持死锁检测 
- [x] 支持如下KV操作 
  - [x] PUT 保存一个KV
  - [x] GET 查询一个KV 
  - [x] DELETE 删除一个KV
  - [x] SCAN 从某一个KEY开始，顺序的查询指定个数的记录
- [x] 采用C/S架构，自定义基于TCP的二进制私有协议对外提供服务（不能使用现有的协议来实现，比如HTTP） 
- [x] 实现访问KV服务的客户端

## 设计

- 事务并发控制：要求SI隔离级别，同时又要悲观锁。所以选择MV2PL，GC是transaction-level，版本存储是N2O，索引仅支持唯一索引。 
- 索引：为了方便实现，选择了skiplist。
- 锁：为了支持死锁检测，把MV2PL的原子性读写锁换成了condition variable，维护一个等待队列来做唤醒。唤醒是boardcast, 等待事务需要看自己的等待TaskID<allowTaskID来判断是否拿到锁，试图用这种方法做到公平。这里没有参考其他系统，所以可能怪怪的。
- 日志：要求里没提到日志，而且时间紧张就没实现，所以宕机数据就没有了。设想的方法是写value logging日志，全量checkpoint，并行recovery。
- 通信协议：双方都以length + command type + []string的方式发送请求/响应，根据不同的命令来用[]string。

## 使用方法

```shell
$ go build -o server cmd/server/main.go
$ go build -o client cmd/client/main.go
```

```shell
$ ./server -h
Usage:
  server [OPTIONS]

Application Options:
  -h, --host=host    simple-kv server host (default: localhost)
  -p, --port=port    simple-kv server port (default: 8081)

Help Options:
  -h, --help         Show this help message
```

```shell
$ ./client -h
Usage:
  client [OPTIONS]

Application Options:
  -h, --host=host    simple-kv server host (default: localhost)
  -p, --port=port    simple-kv server port (default: 8081)

Help Options:
  -h, --help         Show this help message
```

```shell
$ ./client
[localhost:8081]> put "A" "B"
[localhost:8081]> get "A"
B
[localhost:8081]> del "A"
[localhost:8081]> get "A"

[localhost:8081]> put "A" "B"
[localhost:8081]> put "B" "C"
[localhost:8081]> scan "A" 2
[B C]
[localhost:8081]> ^C
```
