#pragma once

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
};

}