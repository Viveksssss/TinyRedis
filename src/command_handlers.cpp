#include "command_handlers.hpp"
#include "command_registry.hpp"
#include "resp_parse.hpp"
#include "server.hpp"
#include "storage.hpp"
#include "stream_utils.hpp"
#include "task.hpp"

#include <cctype>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include <spdlog/spdlog.h>

std::shared_ptr<Redis::Server> _server = nullptr;

void set_global_server(std::shared_ptr<Redis::Server> server)
{
    _server = server;
}

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
    registry.register_handler("incr", incr);
    registry.register_handler("multi", multi);
    registry.register_handler("exec", exec);
    registry.register_handler("watch", watch);
    registry.register_handler("discard", discard);

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
    registry.register_handler("ltrim", ltrim);
    registry.register_handler("ltrim", ltrim);
    registry.register_handler("brpop", brpop);
    registry.register_handler("blpop", blpop);

    registry.register_handler("xadd", xadd);
    registry.register_handler("xrange", xrange);
    registry.register_handler("xread", xread);

    registry.register_handler("type", type);
    registry.register_handler("info", info);
    registry.register_handler("replconf", replconf);
    registry.register_handler("psync", psync);
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

std::string CommandHandlers::incr(const std::vector<std::string>& args)
{
    if (args.size() != 2) {
        return RESPEncoder::encode_error("wrong number of arguments for 'incr' command");
    }
    const std::string& key = args[1];
    auto& storage = Storage::instance();
    try {
        auto result = storage.incr(key);
        // 返回整数结果
        return RESPEncoder::encode_integer(*result);

    } catch (const std::runtime_error& e) {
        return RESPEncoder::encode_error(e.what());
    }
}

/* 实际这里并没有使用，multi需要在外层函数判断 */
std::string CommandHandlers::multi(const std::vector<std::string>& args)
{
    if (args.size() != 1) {
        return RESPEncoder::encode_error("wrong number of arguments for 'multi' command");
    }
    return RESPEncoder::encode_simple_string("OK");
}

std::string CommandHandlers::exec(const std::vector<std::string>& args)
{
    if (args.size() != 1) {
        return RESPEncoder::encode_error("wrong number of arguments for 'exec' command");
    }
    return RESPEncoder::encode_error("EXEC without MULTI");
}

std::string CommandHandlers::discard(const std::vector<std::string>& args)
{
    if (args.size() != 1) {
        return RESPEncoder::encode_error("ERR wrong number of arguments for 'multi' command");
    }
    return RESPEncoder::encode_simple_string("OK");
}

std::string CommandHandlers::watch(const std::vector<std::string>& args)
{
    if (args.size() != 1) {
        return RESPEncoder::encode_error("ERR wrong number of arguments for 'watch' command");
    }
    return RESPEncoder::encode_simple_string("OK");
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

Task<std::string> CommandHandlers::blpop(const std::vector<std::string>& args)
{
    if (args.size() < 3) {
        co_return RESPEncoder::encode_error("wrong number of arguments for 'blpop' command");
    }

    /* 解析秒数 */
    int timeout_seconds;
    try {
        size_t pos;
        timeout_seconds = std::stoi(args.back(), &pos);
        if (pos != args.back().length()) {
            co_return RESPEncoder::encode_error("timeout is not an integer");
        }
    } catch (...) {
        co_return RESPEncoder::encode_error("timeout is not an integer");
    }

    auto timeout = std::chrono::seconds(timeout_seconds);
    auto& storage = Storage::instance();

    /* 1.检查所有key,看是否有立即可用的元素 */
    for (std::size_t i = 1; i < args.size() - 1; ++i) {
        const auto& key = args[i];

        auto value = storage.try_lpop(key);
        if (value) {
            std::vector<std::string> result = { key, *value };
            co_return RESPEncoder::encode_array(result);
        }
    }

    const auto& key = args[1];
    auto value = co_await storage.co_blpop(key, timeout);
    if (value.has_value()) {
        co_return RESPEncoder::encode_array({ key, value.value() });
    } else {
        co_return RESPEncoder::encode_null_bulk_string();
    }
}

Task<std::string> CommandHandlers::brpop(const std::vector<std::string>& args)
{
    if (args.size() < 3) {
        co_return RESPEncoder::encode_error("wrong number of arguments for 'brpop' command");
    }

    /* 解析秒数 */
    int timeout_seconds;
    try {
        size_t pos;
        timeout_seconds = std::stoi(args.back(), &pos);
        if (pos != args.back().length()) {
            co_return RESPEncoder::encode_error("timeout is not an integer");
        }
    } catch (...) {
        co_return RESPEncoder::encode_error("timeout is not an integer");
    }

    auto timeout = std::chrono::seconds(timeout_seconds);
    auto& storage = Storage::instance();

    /* 1.检查所有key,看是否有立即可用的元素 */
    for (std::size_t i = 1; i < args.size() - 1; ++i) {
        const auto& key = args[i];

        auto value = storage.try_lpop(key);
        if (value) {
            std::vector<std::string> result = { key, *value };
            co_return RESPEncoder::encode_array(result);
        }
    }

    const auto& key = args[1];
    auto value = co_await storage.co_blpop(key, timeout);
    if (value.has_value()) {
        co_return RESPEncoder::encode_array({ key, value.value() });
    } else {
        co_return RESPEncoder::encode_null_bulk_string();
    }
}

std::string CommandHandlers::xadd(const std::vector<std::string>& args)
{
    if (args.size() < 4 || (args.size() - 3) % 2 != 0) {
        return RESPEncoder::encode_error("wrong number of arguments for 'xadd' command");
    }
    const std::string& key = args[1];
    const std::string& id = args[2];

    if (!Redis::Utils::StreamUtils::is_auto_spec(id) && !Redis::Utils::StreamUtils::is_valid_id(id)) {
        return RESPEncoder::encode_error("Invalid stream ID format");
    }

    std::vector<std::pair<std::string, std::string>> fields;

    for (std::size_t i = 3; i < args.size(); i += 2) {
        fields.emplace_back(args[i], args[i + 1]);
    }
    auto& storage = Storage::instance();

    try {
        std::string entry_id = storage.xadd(key, id, fields);
        return RESPEncoder::encode_bulk_string(entry_id);
    } catch (const std::runtime_error& e) {
        return RESPEncoder::encode_error(e.what());
    }
}

std::string CommandHandlers::xrange(const std::vector<std::string>& args)
{
    if (args.size() != 4) {
        return RESPEncoder::encode_error("wrong number of arguments for 'xrange' command");
    }

    const std::string& key = args[1];
    const std::string& start = args[2];
    const std::string& end = args[3];
    auto& storage = Storage::instance();

    /*
        这里处理RESP编码的时候，不要把已经处理成RESP的字符串再次传入编码函数导致双重编码。
        对于多层嵌套，可以手动处理。
    */
    try {
        auto entries_opt = storage.xrange(key, start, end);
        if (!entries_opt) {
            return RESPEncoder::encode_array({});
        }

        const auto& entries = *entries_opt;

        std::string result;
        result.reserve(256);
        result = "*" + std::to_string(entries.size()) + "\r\n";
        for (const auto& entry : entries) {
            /* 首先是两个元素的数组，id和fields_array */
            result += "*2\r\n";

            /* id */
            result += "$" + std::to_string(entry.id.length()) + "\r\n";
            result += entry.id + "\r\n";

            /* 键值对数组 */
            result += "*" + std::to_string(entry.fields.size() * 2) + "\r\n";
            std::string fields_content;
            for (const auto& field : entry.fields) {
                fields_content += "$" + std::to_string(field.first.length()) + "\r\n";
                fields_content += field.first + "\r\n";

                fields_content += "$" + std::to_string(field.second.length()) + "\r\n";
                fields_content += field.second + "\r\n";
            }

            result += fields_content;
        }
        return result;

    } catch (const std::runtime_error& e) {
        return RESPEncoder::encode_error(e.what());
    }
}

// command_handlers.cpp
Task<std::string> CommandHandlers::xread(const std::vector<std::string>& args)
{
    // XREAD STREAMS key1 key2 ... id1 id2 ...
    if (args.size() < 4) {
        co_return RESPEncoder::encode_error("wrong number of arguments for 'xread' command");
    }

    std::chrono::milliseconds timeout { 0 };
    std::size_t count = 0;
    std::size_t streams_start = 1;

    std::size_t i = 1;
    while (i < args.size()) {
        if (args[i] == "BLOCK" || args[i] == "block") {
            if (i + 1 >= args.size()) {
                co_return RESPEncoder::encode_error("wrong number of arguments for 'xread' command");
            }
            try {
                uint64_t block_ms = std::stoull(args[i + 1]);
                timeout = std::chrono::milliseconds(block_ms);
                i += 2;
            } catch (...) {
                co_return RESPEncoder::encode_error("invalid BLOCK argument");
            }
        } else if (args[i] == "COUNT" || args[i] == "count") {
            if (i + 1 >= args.size()) {
                co_return RESPEncoder::encode_error("wrong number of arguments for 'xread' command");
            }
            try {
                count = std::stoull(args[i + 1]);
                i += 2;
            } catch (...) {
                co_return RESPEncoder::encode_error("invalid COUNT argument");
            }
        } else if (args[i] == "STREAMS" || args[i] == "streams") {
            streams_start = i;
            break;
        } else {
            co_return RESPEncoder::encode_error("unknown option for 'xread' command");
        }
    }

    if (streams_start >= args.size()) {
        co_return RESPEncoder::encode_error("expected STREAMS keyword");
    }

    std::size_t streams_index = streams_start + 1;
    std::size_t total_args = args.size() - streams_index;

    if (total_args % 2 != 0) {
        co_return RESPEncoder::encode_error("wrong number of arguments for 'xread' command");
    }

    size_t num_streams = total_args / 2;

    // 分离 keys 和 ids
    std::vector<std::string> keys;
    std::vector<std::string> ids;

    for (size_t i = 0; i < num_streams; ++i) {
        keys.push_back(args[streams_index + i]); // 前半部分是 keys
    }

    for (size_t i = 0; i < num_streams; ++i) {
        ids.push_back(args[num_streams + streams_index + i]); // 后半部分是 ids
    }

    // 构建流请求对
    std::vector<std::pair<std::string, std::string>> stream_requests;
    for (size_t i = 0; i < num_streams; ++i) {
        stream_requests.emplace_back(keys[i], ids[i]);
    }

    auto& storage = Storage::instance();

    try {
        std::unordered_map<std::string, std::vector<StreamEntry>> results;

        if (timeout.count() > 0) {
            auto results_opt = co_await storage.xread_block(stream_requests, timeout, count);
            results = results_opt;
        } else {
            results = storage.xread(stream_requests, count);
        }

        if (results.empty()) {
            co_return "*0\r\n";
        }

        // 构建 RESP 响应（保持原始 key 顺序）
        std::string response = "*" + std::to_string(num_streams) + "\r\n";

        for (size_t i = 0; i < num_streams; ++i) {
            const auto& key = keys[i];

            // 每个流是一个包含两个元素的数组：[key, entries_array]
            response += "*2\r\n";

            // key 作为 bulk string
            response += "$" + std::to_string(key.size()) + "\r\n";
            response += key + "\r\n";

            // 查找这个 key 的条目
            auto it = results.find(key);
            if (it == results.end() || it->second.empty()) {
                // 没有新条目，返回空数组
                response += "*0\r\n";
            } else {
                // 有条目，返回条目数组
                const auto& entries = it->second;
                response += "*" + std::to_string(entries.size()) + "\r\n";

                for (const auto& entry : entries) {
                    // 每个条目是 [id, fields_array]
                    response += "*2\r\n";

                    // id
                    response += "$" + std::to_string(entry.id.size()) + "\r\n";
                    response += entry.id + "\r\n";

                    // fields_array
                    response += "*" + std::to_string(entry.fields.size() * 2) + "\r\n";
                    for (const auto& field : entry.fields) {
                        // field name
                        response += "$" + std::to_string(field.first.size()) + "\r\n";
                        response += field.first + "\r\n";
                        // field value
                        response += "$" + std::to_string(field.second.size()) + "\r\n";
                        response += field.second + "\r\n";
                    }
                }
            }
        }

        co_return response;

    } catch (const std::runtime_error& e) {
        co_return RESPEncoder::encode_error(e.what());
    }
}

std::string CommandHandlers::type(const std::vector<std::string>& args)
{
    if (args.size() != 2) {
        return RESPEncoder::encode_error("wrong number of arguments for 'type' command");
    }

    const auto& key = args[1];
    auto& storage = Storage::instance();

    if (!storage.exists(key)) {
        return RESPEncoder::encode_simple_string("none");
    }

    auto type = storage.type(key);
    if (!type) {
        return RESPEncoder::encode_simple_string("none");
    }

    switch (*type) {
    case ValueType::STRING:
        return RESPEncoder::encode_simple_string("string");
    case ValueType::LIST:
        return RESPEncoder::encode_simple_string("list");
    case ValueType::STREAM:
        return RESPEncoder::encode_simple_string("stream");
    default:
        return RESPEncoder::encode_simple_string("unknown");
    }
}

std::string CommandHandlers::info(const std::vector<std::string>& args)
{
    if (args.size() > 2) {
        return RESPEncoder::encode_error("wrong number of arguments for 'info' command");
    }

    if (!_server) {
        return RESPEncoder::encode_error("server not initialized");
    }

    std::string section;
    if (args.size() == 2) {
        section = args[1];
        // 转为小写
        for (auto& c : section) {
            c = tolower(c);
        }
    }

    std::string info_str = _server->info(section);
    return RESPEncoder::encode_bulk_string(info_str);
}

std::string CommandHandlers::replconf(const std::vector<std::string>& args)
{
    if (args.size() < 2) {
        return RESPEncoder::encode_error("wrong number of arguments for 'replconf'");
    }

    // 对于这个阶段，我们忽略所有参数，直接返回 +OK
    // 后续阶段可以在这里记录 replica 的信息

    SPDLOG_INFO("Received REPLCONF :");
    for (const auto& arg : args) {
        SPDLOG_INFO("{}", arg);
    }

    return RESPEncoder::encode_simple_string("OK");
}
std::string CommandHandlers::psync(const std::vector<std::string>& args)
{
    if (args.size() != 3) {
        return RESPEncoder::encode_error("wrong number of arguments for 'psync'");
    }

    const std::string& replid = args[1];
    const std::string& offset = args[2];

    if (replid == "?" && offset == "-1") {
        std::string response = "+FULLRESYNC " + _server->replid() + " " + _server->repl_offset() + "\r\n";
        return response; // 直接返回字符串，不是 bulk string
    }
    return RESPEncoder::encode_error("unsupported PSYNC parameters");
}

}