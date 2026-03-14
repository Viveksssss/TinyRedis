#pragma once

#include "task.hpp"

#include <string>
#include <vector>

namespace Redis::Config {

class CommandHandlers {

public:
    static void register_all();

    // 命令处理器函数
    static std::string ping(const std::vector<std::string>& args);
    static std::string echo(const std::vector<std::string>& args);
    static std::string set(const std::vector<std::string>& args);
    static std::string get(const std::vector<std::string>& args);
    static std::string del(const std::vector<std::string>& args);
    static std::string incr(const std::vector<std::string>& args);
    static std::string multi(const std::vector<std::string>& args);
    static std::string exec(const std::vector<std::string>& args);
    static std::string discard(const std::vector<std::string>& args);
    static std::string watch(const std::vector<std::string>& args);
    static std::string ttl(const std::vector<std::string>& args);
    static std::string pttl(const std::vector<std::string>& args);

    // 有序集合
    static std::string zadd(const std::vector<std::string>& args);
    static std::string zrank(const std::vector<std::string>& args);
    static std::string zrange(const std::vector<std::string>& args);
    static std::string zrangewithscores(const std::vector<std::string>& args);
    static std::string zrevrange(const std::vector<std::string>& args);
    static std::string zscore(const std::vector<std::string>& args);
    static std::string zcard(const std::vector<std::string>& args);
    static std::string zrem(const std::vector<std::string>& args);
    static std::string zcount(const std::vector<std::string>& args);
    static std::string zincrby(const std::vector<std::string>& args);
    static std::string zpopmin(const std::vector<std::string>& args);
    static std::string zpopmax(const std::vector<std::string>& args);

    // 列表命令
    static std::string rpush(const std::vector<std::string>& args);
    static std::string lpush(const std::vector<std::string>& args);
    static std::string llen(const std::vector<std::string>& args);
    static std::string lrange(const std::vector<std::string>& args);
    static std::string lindex(const std::vector<std::string>& args);
    static std::string lset(const std::vector<std::string>& args);
    static std::string lpop(const std::vector<std::string>& args);
    static std::string rpop(const std::vector<std::string>& args);
    static std::string lrem(const std::vector<std::string>& args);
    static std::string ltrim(const std::vector<std::string>& args);
    static Task<std::string> blpop(const std::vector<std::string>& args);
    static Task<std::string> brpop(const std::vector<std::string>& args);

    // Stream流
    static std::string xadd(const std::vector<std::string>& args);
    static std::string xrange(const std::vector<std::string>& args);
    static Task<std::string> xread(const std::vector<std::string>& args);

    // 杂项
    static std::string type(const std::vector<std::string>& args);
    static std::string info(const std::vector<std::string>& args);
    static std::string replconf(const std::vector<std::string>& args);
    static std::string psync(const std::vector<std::string>& args);
    static Task<std::string> wait(const std::vector<std::string>& args);
    static std::string config(const std::vector<std::string>& args);
    static std::string keys(const std::vector<std::string>& args);
    static std::string save(const std::vector<std::string>& args);
    static std::string bgsave(const std::vector<std::string>& args);

    // HASH 命令
    static std::string hset(const std::vector<std::string>& args);
    static std::string hget(const std::vector<std::string>& args);
    static std::string hgetall(const std::vector<std::string>& args);
    static std::string hdel(const std::vector<std::string>& args);
    static std::string hexists(const std::vector<std::string>& args);
    static std::string hlen(const std::vector<std::string>& args);
    static std::string hkeys(const std::vector<std::string>& args);
    static std::string hvals(const std::vector<std::string>& args);
    static std::string hmset(const std::vector<std::string>& args);
    static std::string hmget(const std::vector<std::string>& args);
    static std::string hincrby(const std::vector<std::string>& args);
    static std::string hincrbyfloat(const std::vector<std::string>& args);
    static std::string hsetnx(const std::vector<std::string>& args);
    static std::string hstrlen(const std::vector<std::string>& args);

    static std::string auth(const std::vector<std::string>& args);
};

}