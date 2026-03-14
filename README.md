# TinyRedis

一个从零实现的轻量级Redis服务器，支持多种数据结构、持久化和主从复制。

## 项目概述

TinyRedis 是一个教学性质的Redis实现，旨在通过实践理解Redis的核心工作原理。项目使用C++23开发，基于Boost.Asio实现异步网络通信，完整实现了RESP协议和多种Redis命令。

## 功能特性

### 支持的数据结构

- **字符串 (String)** - 基础键值对存储
- **列表 (List)** - 有序字符串集合，支持LPUSH/RPUSH等操作
- **哈希 (Hash)** - 字段-值对集合
- **集合 (Set)** - 唯一字符串集合
- **有序集合 (Sorted Set)** - 带分数的有序集合，支持ZADD/ZRANK等
- **流 (Stream)** - 日志式数据结构

### 已实现的命令

#### 基础命令
- `PING` - 服务器心跳检测
- `ECHO` - 返回传入的参数
- `SET` - 设置键值对，支持EX/PX/NX/XX选项
- `GET` - 获取键的值
- `DEL` - 删除一个或多个键
- `INCR` - 将键的整数值加1
- `TTL`/`PTTL` - 获取键的剩余过期时间

#### 列表命令
- `LPUSH`/`RPUSH` - 从左侧/右侧推入元素
- `LPOP`/`RPOP` - 从左侧/右侧弹出元素
- `LRANGE` - 获取列表范围内的元素
- `LINDEX` - 获取指定索引的元素
- `LSET` - 设置指定索引的元素
- `LLEN` - 获取列表长度
- `LREM` - 移除指定数量的匹配元素
- `LTRIM` - 修剪列表到指定范围
- `BLPOP`/`BRPOP` - 阻塞式弹出

#### 哈希命令
- `HSET`/`HGET` - 设置/获取字段值
- `HGETALL` - 获取所有字段和值
- `HDEL` - 删除字段
- `HEXISTS` - 检查字段是否存在
- `HLEN` - 获取字段数量
- `HKEYS` - 获取所有字段名
- `HVALS` - 获取所有值
- `HINCRBY`/`HINCRBYFLOAT` - 递增字段值

#### 有序集合命令
- `ZADD` - 添加成员
- `ZRANK` - 获取成员排名
- `ZRANGE`/`ZREVRANGE` - 获取范围内的成员
- `ZRANGEWITHSCORES` - 获取带分数的成员
- `ZSCORE` - 获取成员分数
- `ZCARD` - 获取成员数量
- `ZREM` - 删除成员
- `ZCOUNT` - 统计分数范围内的成员
- `ZINCRBY` - 递增分数
- `ZPOPMIN`/`ZPOPMAX` - 弹出最小/最大分数成员

#### 流命令
- `XADD` - 向流添加条目
- `XRANGE` - 获取范围内的条目
- `XREAD` - 从流读取数据（支持阻塞）

#### 事务命令
- `MULTI`/`EXEC`/`DISCARD` - 事务支持
- `WATCH` - 乐观锁支持

#### 复制命令
- `REPLCONF` - 复制配置
- `PSYNC` - 部分同步
- `WAIT` - 等待副本确认

#### 管理命令
- `INFO` - 服务器信息
- `CONFIG` - 配置管理
- `SAVE`/`BGSAVE` - RDB持久化
- `KEYS` - 键匹配查询
- `TYPE` - 键类型查询
- `AUTH` - 认证

### 核心特性

- ✅ **RESP协议** - 完整实现Redis序列化协议
- ✅ **异步网络** - 基于Boost.Asio的高性能异步IO
- ✅ **线程安全** - 线程安全的键值存储
- ✅ **过期机制** - 支持键过期和智能清理
- ✅ **持久化** - RDB格式的持久化支持
- ✅ **主从复制** - 支持异步主从复制
- ✅ **事务** - 完整的事务和WATCH支持
- ✅ **模块化设计** - 基于JSON的命令配置系统

## 项目结构

```text
tinyredis/
├── CMakeLists.txt					
├── config/								# 命令配置
├── deps/								# 依赖库
├── dump.rdb							# rdb文件
├── include/							# 头文件
├── src/								# 源文件
├── install_deps_to_system.sh			# 安装依赖脚本
└── uninstall_deps_from_system.sh		# 卸载脚步脚本
```

## 编译和安装

### 依赖

- C++23 兼容的编译器 (GCC 13+ / Clang 16+)
- Boost.Asio
- nlohmann-json
- spdlog
- CMake 3.13+

### 编译步骤

```bash
git clone https://github.com/yourusername/tinyredis.git
cd tinyredis
sudo ./install_deps_to_system.sh
mkdir build && cd build
cmake ..
make -j8
```

## 使用说明

### 启动服务器

```bash
# 默认端口6379启动
./redis

# 指定端口
./redis --port 6380

# 以副本模式启动
./redis --port 6380 --replicaof "localhost 6379"

# 指定RDB文件
./redis --dir /var/lib/redis --dbfilename dump.rdb

# 设置密码
./redis --requirepass mypassword

# 查看帮助
./redis --help
```

### 客户端连接

使用标准 `redis-cli` 连接：

```bash
redis-cli -p 6379
> PING
PONG
> SET foo bar
OK
> GET foo
"bar"
```



