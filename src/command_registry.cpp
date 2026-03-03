#include "command_registry.hpp"

#include "resp_parse.hpp"
#include <algorithm>
#include <boost/asio/io_context.hpp>
#include <cctype>
#include <fstream>
#include <iostream>

namespace Redis::Config {

CommandRegistry& CommandRegistry::instance()
{
    static CommandRegistry registry;
    return registry;
}

bool CommandRegistry::load_from_file(const std::string& filename)
{
    try {
        std::ifstream file(filename);
        if (!file.is_open()) {
            std::cerr << "Failed to open command file: " << filename << std::endl;
            return false;
        }

        nlohmann::json config;
        file >> config;

        for (const auto& cmd_json : config["commands"]) {
            CommandInfo info;
            info.name = cmd_json["name"];
            info.handler = cmd_json["handler"];
            info.min_args = cmd_json["min_args"];
            info.max_args = cmd_json.value("max_args", info.min_args);
            info.description = cmd_json.value("description", "");

            _commands[info.name] = info;
        }
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error loading command config: " << e.what() << std::endl;
        return false;
    }
}

void CommandRegistry::register_handler(const std::string& name, CommandHandler handler)
{
    std::string upper_name = name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    _handlers[upper_name] = handler;
}

bool CommandRegistry::validate_args(const CommandInfo& info, const std::vector<std::string>& args, std::string& error) const
{
    size_t arg_count = args.size() - 1; // 减去命令名

    if (arg_count < static_cast<size_t>(info.min_args)) {
        error = "wrong number of arguments for '" + info.name + "' command";
        return false;
    }

    if (info.max_args != -1 && arg_count > static_cast<size_t>(info.max_args)) {
        error = "wrong number of arguments for '" + info.name + "' command";
        return false;
    }

    return true;
}

std::string CommandRegistry::execute(const std::string& cmd_name, const std::vector<std::string>& args)
{
    std::string upper_name = cmd_name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);

    // 查找命令信息
    auto cmd_it = _commands.find(upper_name);
    if (cmd_it == _commands.end()) {
        return RESPEncoder::encode_error("unknown command '" + cmd_name + "'");
    }

    // 验证参数数量
    std::string error;
    if (!validate_args(cmd_it->second, args, error)) {
        return RESPEncoder::encode_error(error);
    }

    // 查找命令处理器
    auto handler_it = _handlers.find(upper_name);
    if (handler_it == _handlers.end()) {
        return RESPEncoder::encode_error("handler not implemented for '" + cmd_name + "'");
    }

    // 执行命令
    try {
        return handler_it->second(args);
    } catch (const std::exception& e) {
        return RESPEncoder::encode_error(std::string(e.what()));
    }
}

bool CommandRegistry::has_command(const std::string& cmd_name) const
{
    std::string upper_name = cmd_name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    return _commands.find(upper_name) != _commands.end();
}

const CommandInfo* CommandRegistry::get_command_info(const std::string& cmd_name) const
{
    std::string upper_name = cmd_name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
    auto it = _commands.find(upper_name);
    return it != _commands.end() ? &it->second : nullptr;
}

}