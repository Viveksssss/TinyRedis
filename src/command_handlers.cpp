#include "command_handlers.hpp"
#include "command_registry.hpp"
#include "resp_parse.hpp"
#include "storage.hpp"
#include <cctype>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
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

    registry.register_handler("rpush", rpush);
    registry.register_handler("lpush", lpush);
    registry.register_handler("llen", llen);
    registry.register_handler("lrange", lrange);
    registry.register_handler("lindex", lindex);
    registry.register_handler("lset", lset);
    registry.register_handler("lpop", lpop);
    registry.register_handler("rpop", rpop);
    registry.register_handler("lrem", lrem);
    registry.register_handler("ltrim", ltrim);
    registry.register_handler("blpop", blpop);
    registry.register_handler("brpop", brpop);
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
        storage.set_string(key, value);
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
        storage.set_string_with_expiry_ms(key, value, *ttl);
    } else {
        storage.set_string(key, value);
    }

    return RESPEncoder::encode_simple_string("OK");
}

std::string CommandHandlers::get(const std::vector<std::string>& args)
{
    const std::string& key = args[1];

    auto& storage = Storage::instance();
    auto value = storage.get_string(key);

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

std::string CommandHandlers::rpush(const std::vector<std::string>& args)
{
    if (args.size() < 3) {
        return RESPEncoder::encode_error("wrong number of arguments for 'rpush' command");
    }

    const std::string& key = args[1];
    auto& storage = Storage::instance();

    try {
        std::size_t list_length = 0;
        if (args.size() == 3) {
            list_length = storage.rpush(key, args[2]);
        } else {
            std::vector<std::string> values;
            values.reserve(args.size());
            for (std::size_t i = 2; i < args.size(); ++i) {
                values.push_back(args[i]);
            }
            list_length = storage.rpush_multi(key, values);
        }
        return RESPEncoder::encode_integer(static_cast<int64_t>(list_length));
    } catch (const std::runtime_error& e) {
        return RESPEncoder::encode_error(e.what());
    }
}
std::string CommandHandlers::lpush(const std::vector<std::string>& args)
{
    if (args.size() < 3) {
        return RESPEncoder::encode_error("wrong number of arguments for 'lpush' command");
    }

    const std::string& key = args[1];
    auto& storage = Storage::instance();
    try {
        std::size_t list_length = 0;
        if (args.size() == 3) {
            list_length = storage.lpush(key, args[2]);
        } else {
            std::vector<std::string> values;
            for (std::size_t i = 2; i < args.size(); ++i) {
                values.push_back(args[i]);
            }
            list_length = storage.lpush_multi(key, values);
        }
        return RESPEncoder::encode_integer(static_cast<int64_t>(list_length));
    } catch (const std::runtime_error& e) {
        return RESPEncoder::encode_error(e.what());
    }
}
std::string CommandHandlers::llen(const std::vector<std::string>& args)
{
    if (args.size() != 2) {
        return RESPEncoder::encode_error("wrong number of arguments for 'llen' command");
    }

    const std::string& key = args[1];
    auto& storage = Storage::instance();

    auto len = storage.llen(key);
    if (!len) {
        return RESPEncoder::encode_integer(0);
    }
    return RESPEncoder::encode_integer(static_cast<int64_t>(*len));
}
std::string CommandHandlers::lrange(const std::vector<std::string>& args)
{
    if (args.size() != 4) {
        return RESPEncoder::encode_error("wrong number of arguments for 'lrange' command");
    }

    const std::string& key = args[1];
    int start = std::stoi(args[2]);
    int stop = std::stoi(args[3]);

    auto& storage = Storage::instance();

    // 检查类型
    if (!storage.is_list(key)) {
        // 键不存在，返回空列表
        return RESPEncoder::encode_null_array();
    }

    auto range = storage.lrange(key, start, stop);
    if (!range) {
        return RESPEncoder::encode_null_array();
    }

    return RESPEncoder::encode_array(*range);
}
std::string CommandHandlers::lindex(const std::vector<std::string>& args)
{
    if (args.size() != 3) {
        return RESPEncoder::encode_error("wrong number of arguments for 'lindex' command");
    }

    const std::string& key = args[1];
    int index;
    try {
        std::size_t pos;
        index = std::stoi(args[2], &pos);
        if (pos != args[2].length()) {
            /* 类似“10ab之类输入” */
            return RESPEncoder::encode_error("value is not an integer or out of range");
        }

    } catch (const std::invalid_argument& e) {
        return RESPEncoder::encode_error("value is not an integer or out of range");
    } catch (const std::out_of_range& e) {
        // 数值超出 int 范围
        return RESPEncoder::encode_error("value is not an integer or out of range");
    }

    auto& storage = Storage::instance();

    try {

        if (!storage.is_list(key)) {
            return RESPEncoder::encode_null_bulk_string();
        }
        auto value = storage.lindex(key, index);
        if (!value) {
            return RESPEncoder::encode_null_bulk_string();
        }
        return RESPEncoder::encode_bulk_string(*value);
    } catch (std::runtime_error& e) {
        return RESPEncoder::encode_error(e.what());
    }
}
std::string CommandHandlers::lset(const std::vector<std::string>& args)
{
    if (args.size() != 4) {
        return RESPEncoder::encode_error("wrong number of arguments for 'lset' command");
    }
    const std::string& key = args[1];
    const std::string& value = args[3];
    auto& storage = Storage::instance();

    int index;
    try {
        size_t pos;
        index = std::stoi(args[2], &pos);
        if (pos != args[2].length()) {
            return RESPEncoder::encode_error("value is not an integer or out of range");
        }
    } catch (const std::invalid_argument& e) {
        return RESPEncoder::encode_error("value is not an integer or out of range");
    } catch (const std::out_of_range& e) {
        return RESPEncoder::encode_error("value is not an integer or out of range");
    }

    try {
        if (!storage.is_list(key)) {
            return RESPEncoder::encode_error("no such key");
        }

        storage.lset(key, index, value);
        return RESPEncoder::encode_simple_string("OK");

    } catch (const std::runtime_error& e) {
        return RESPEncoder::encode_error(e.what());
    }
}
std::string CommandHandlers::lpop(const std::vector<std::string>& args)
{
    if (args.size() != 2) {
        return RESPEncoder::encode_error("wrong number of arguments for 'lpop' command");
    }
    const std::string& key = args[1];
    auto& storage = Storage::instance();

    auto value = storage.lpop(key);
    if (!value) {
        return RESPEncoder::encode_null_bulk_string();
    }

    return RESPEncoder::encode_bulk_string(*value);
}
std::string CommandHandlers::rpop(const std::vector<std::string>& args)
{
    if (args.size() != 2) {
        return RESPEncoder::encode_error("wrong number of arguments for 'rpop' command");
    }

    const std::string& key = args[1];
    auto& storage = Storage::instance();

    auto value = storage.rpop(key);
    if (!value) {
        return RESPEncoder::encode_null_bulk_string();
    }

    return RESPEncoder::encode_bulk_string(*value);
}
std::string CommandHandlers::lrem(const std::vector<std::string>& args)
{
    if (args.size() != 4) {
        return RESPEncoder::encode_error("wrong number of arguments for 'lrem' command");
    }

    const std::string& key = args[1];
    int count = std::stoi(args[2]);
    const std::string& value = args[3];

    auto& storage = Storage::instance();

    if (!storage.is_list(key)) {
        return RESPEncoder::encode_integer(0);
    }

    size_t removed = storage.lrem(key, count, value);
    return RESPEncoder::encode_integer(static_cast<int64_t>(removed));
}
std::string CommandHandlers::ltrim(const std::vector<std::string>& args)
{
    if (args.size() != 4) {
        return RESPEncoder::encode_error("wrong number of arguments for 'ltrim' command");
    }

    const std::string& key = args[1];
    int start = std::stoi(args[2]);
    int stop = std::stoi(args[3]);

    auto& storage = Storage::instance();

    if (!storage.is_list(key)) {
        return RESPEncoder::encode_simple_string("OK"); // Redis 行为：不存在的 key 也返回 OK
    }

    storage.ltrim(key, start, stop);
    return RESPEncoder::encode_simple_string("OK");
}
/* 暂时不予实现 */
std::string CommandHandlers::blpop(const std::vector<std::string>& args)
{
    return RESPEncoder::encode_null_bulk_string();
}
std::string CommandHandlers::brpop(const std::vector<std::string>& args)
{
    return RESPEncoder::encode_null_bulk_string();
}

}