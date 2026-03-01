# TinyRedis

这是一个从零开始实现的轻量级Redis服务器，支持基本的Redis命令和RESP协议。项目使用C++开发，基于Boost.Asio实现异步网络通信。

## 功能特性

### 已支持的命令

- **PING** - 服务器心跳检测
- **ECHO** - 返回传入的参数
- **SET** - 设置键值对，支持以下选项：
  - `EX` seconds - 设置过期时间（秒）
  - `PX` milliseconds - 设置过期时间（毫秒）
  - `NX` - 键不存在时设置
  - `XX` - 键存在时设置
- **GET** - 获取键的值
- **DEL** - 删除一个或多个键

### 核心特性

- ✅ RESP（Redis序列化协议）完整实现
- ✅ 异步TCP服务器（Boost.Asio）
- ✅ 线程安全的键值存储
- ✅ 支持键过期（TTL）
- ✅ 智能过期清理策略（抽样清理）
- ✅ 基于JSON的命令配置
- ✅ 模块化的命令注册机制

### 项目依赖

- nlohmann-json
- boost-asio
- C++17及以上
