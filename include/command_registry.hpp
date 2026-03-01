#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

/*
    解析器：解析json获取命令以及命令的参数及handler
*/

namespace Redis::Config {

using CommandHandler = std::function<std::string(const std::vector<std::string>&)>;

struct CommandInfo {
    std::string name;
    std::string handler;
    int min_args;
    int max_args; // -1 表示可变参数
    std::string description;
};

class CommandRegistry {
public:
    static CommandRegistry& instance();

    // 从JSON文件加载命令
    bool load_from_file(const std::string& filename);

    // 注册命令处理器
    void register_handler(const std::string& name, CommandHandler handler);

    // 执行命令
    std::string execute(const std::string& cmd_name, const std::vector<std::string>& args);

    // 检查命令是否存在
    bool has_command(const std::string& cmd_name) const;

    // 获取命令信息
    const CommandInfo* get_command_info(const std::string& cmd_name) const;

private:
    CommandRegistry() = default;

    bool validate_args(const CommandInfo& info, const std::vector<std::string>& args, std::string& error) const;

    std::unordered_map<std::string, CommandInfo> _commands;
    std::unordered_map<std::string, CommandHandler> _handlers;
};

}
