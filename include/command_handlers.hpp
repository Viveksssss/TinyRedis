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
};

}