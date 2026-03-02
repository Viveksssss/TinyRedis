#pragma once

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <variant>

namespace Redis {

enum class ValueType {
    STRING,
    LIST
};

using ListValue = std::vector<std::string>;
using ValueVariant = std::variant<std::string, ListValue>;

struct ValueWithExpiry {
    /* 值类型 */
    ValueType type;
    /* Value */
    ValueVariant value;
    /* 过期时间点 */
    std::chrono::time_point<std::chrono::steady_clock> expiry;
    /* 是否有过期时间 */
    bool has_expiry;

    ValueWithExpiry() = default;
    /* 字符串构造 */
    ValueWithExpiry(const std::string& v)
        : value(v)
        , type(ValueType::STRING)
        , has_expiry(false)
    {
    }

    ValueWithExpiry(const std::string& v, std::chrono::milliseconds ttl)
        : value(v)
        , type(ValueType::STRING)
        , expiry(std::chrono::steady_clock::now() + ttl)
        , has_expiry(true)
    {
    }

    /* 列表构造 */
    ValueWithExpiry(const ListValue& v)
        : value(v)
        , type(ValueType::LIST)
        , has_expiry(false)
    {
    }
    ValueWithExpiry(const ListValue& v, std::chrono::milliseconds ttl)
        : value(v)
        , type(ValueType::LIST)
        , expiry(std::chrono::steady_clock::now() + ttl)
        , has_expiry(true)
    {
    }

    bool is_expired() const
    {
        if (!has_expiry)
            return false;
        return std::chrono::steady_clock::now() >= expiry;
    }

    /* 获取字符串 */
    std::optional<std::string> get_string() const
    {
        if (type != ValueType::STRING) {
            return std::nullopt;
        }
        return std::get<std::string>(value);
    }
    /* 获取列表 */
    std::optional<ListValue> get_list() const
    {
        if (type != ValueType::LIST) {
            return std::nullopt;
        }
        return std::get<ListValue>(value);
    }
    ListValue* get_list_ptr()
    {
        if (type != ValueType::LIST) {
            return nullptr;
        }
        return &std::get<ListValue>(value);
    }
};

class Storage {
public:
    ~Storage();
    static Storage& instance();
    /*
        字符串操作****************************************************
    */
    /* 设置键值对 */
    void set_string(const std::string& key, const std::string& value);
    /* 设置键值对+时间 */
    void set_string_with_expiry_ms(const std::string& key, const std::string& value, std::chrono::milliseconds ttl);
    /* 获得键 */
    std::optional<std::string> get_string(const std::string& key);

    /**
        列表操作****************************************************
    */
    /* 获取列表左侧开始制定索引的元素 */
    std::optional<std::string> lindex(const std::string& key, int index);
    /* 移除列表中指定数量的匹配元素 */
    size_t lrem(const std::string& key, int count, const std::string& value);
    /* 在列表尾部追加元素，返回列表长度 */
    size_t rpush(const std::string& key, const std::string& value);
    /* 在列表尾部追加多个元素，返回列表长度 */
    size_t rpush_multi(const std::string& key, const std::vector<std::string>& values);
    /* 在列表头部插入元素，返回列表长度 */
    size_t lpush(const std::string& key, const std::string& value);
    /* 在列表头部插入多个元素，返回列表长度 */
    size_t lpush_multi(const std::string& key, const std::vector<std::string>& values);
    /* 获取列表所有元素 */
    std::optional<ListValue> get_list(const std::string& key);
    /* 获取列表指定范围的元素 */
    std::optional<ListValue> lrange(const std::string& key, int start, int stop);
    /* 设置列表指定索引的元素 */
    bool lset(const std::string& key, int index, const std::string& value);
    /* 获取列表长度 */
    std::optional<size_t> llen(const std::string& key);
    /* 检查键是否是列表 */
    bool is_list(const std::string& key);
    /* 从列表头部弹出元素 */
    std::optional<std::string> lpop(const std::string& key);
    /* 从列表尾部弹出元素 */
    std::optional<std::string> rpop(const std::string& key);
    /* 移除并返回列表的第一个元素，如果没有元素则阻塞 */
    std::optional<std::string> blpop(const std::string& key, std::chrono::seconds timeout);
    /* 移除并返回列表的最后一个元素，如果没有元素则阻塞 */
    std::optional<std::string> brpop(const std::string& key, std::chrono::seconds timeout);
    /* 修剪列表，只保留指定范围内的元素 */
    bool ltrim(const std::string& key, int start, int stop);

    /**
        通用操作****************************************************
    */
    /* 删除键 */
    bool del(const std::string& key);
    /* 删除键组 */
    std::size_t del(const std::vector<std::string>& keys);
    /* 检查键是否存在 */
    bool exists(const std::string& key);
    /* 随机抽样清除过期键 */
    void clean_expired_random();

private:
    Storage();
    /* 检查并移除过期的键 */
    void check_expiry(const std::string& key);
    /* 启动清理线程 */
    void start_cleaner();
    /* 关闭清理线程 */
    void stop_cleaner();
    std::unordered_map<std::string, ValueWithExpiry> _store;
    std::mutex _mutex;

    /* 清理线程 */
    std::thread _cleaner;
    /* 清理线程退出标志 */
    std::atomic<bool> _quit;
};

}