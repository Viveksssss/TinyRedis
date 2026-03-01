#include "command_handlers.hpp"
#include "command_registry.hpp"
#include "resp_parse.hpp"
#include "storage.hpp"
#include <cctype>
#include <chrono>
#include <unordered_map>

namespace Redis::Config {

static std::unordered_map<std::string, std::string> _store;

void CommandHandlers::register_all()
{
    auto& registry = CommandRegistry::instance();

    registry.register_handler("ping", ping);
    registry.register_handler("echo", echo);
    registry.register_handler("set", set);
    registry.register_handler("get", get);
    registry.register_handler("del", del);
}

std::string CommandHandlers::ping(const std::vector<std::string>& args)
{
    return RESPEncoder::encode_simple_string("PONG");
}

std::string CommandHandlers::echo(const std::vector<std::string>& args)
{
    // ECHO 返回第一个参数
    return RESPEncoder::encode_bulk_string(args[1]);
}

std::string CommandHandlers::set(const std::vector<std::string>& args)
{
    // SET key value [NX|XX]
    const std::string& key = args[1];
    const std::string& value = args[2];

    // TODO: 处理可选的 NX/XX 参数
    auto& storage = Storage::instance();
    if (args.size() == 3) {
        storage.set(key, value);
        return RESPEncoder::encode_simple_string("OK");
    }

    std::optional<std::chrono::milliseconds> ttl;
    bool nx = false;
    bool ex = false;
    bool px = false;
    bool xx = false;

    for (std::size_t i = 3; i < args.size(); i += 2) {
        std::string option = args[i];
        for (auto& c : option) {
            c = toupper(c);
        }

        if (option == "XX") {
            if (nx) {
                return RESPEncoder::encode_error("XX and NX are mutually exclusive");
            }
            xx = true;
        } else if (option == "NX") {
            if (xx) {
                return RESPEncoder::encode_error("NX and XX are mutually exclusive");
            }
            nx = true;
        } else if (option == "EX" || option == "PX") {
            if (i + 1 > args.size()) {
                return RESPEncoder::encode_error("syntax error");
            }

            try {
                long num = std::stol(args[i + 1]);
                if (option == "EX") {
                    ttl = std::chrono::seconds(num);
                } else if (option == "PX") {
                    ttl = std::chrono::milliseconds(num);
                }
            } catch (...) {
                return RESPEncoder::encode_error("invalid " + option + " value");
            }

        } else {
            return RESPEncoder::encode_error("unknown option for SET");
        }
    }
    bool key_exists = storage.exists(key);
    if (key_exists && nx) {
        return RESPEncoder::encode_null_bulk_string();
    } else if (!key_exists && xx) {
        return RESPEncoder::encode_null_bulk_string();
    }

    if (ttl) {
        storage.set_with_expiry_ms(key, value, *ttl);
    } else {
        storage.set(key, value);
    }

    return RESPEncoder::encode_simple_string("OK");
}

std::string CommandHandlers::get(const std::vector<std::string>& args)
{
    const std::string& key = args[1];

    auto& storage = Storage::instance();
    auto value = storage.get(key);

    if (!value) {
        return RESPEncoder::encode_null_bulk_string();
    }
    return RESPEncoder::encode_bulk_string(*value);
}

std::string CommandHandlers::del(const std::vector<std::string>& args)
{
    // int deleted = 0;
    // for (size_t i = 1; i < args.size(); ++i) {
    //     if (_store.erase(args[i]) > 0) {
    //         deleted++;
    //     }
    // }
    auto& storage = Storage::instance();
    int deleted = storage.del(std::vector<std::string>(args.begin() + 1, args.end()));

    return RESPEncoder::encode_integer(deleted);
}

}