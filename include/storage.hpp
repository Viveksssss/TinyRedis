#pragma once

#include "task.hpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <condition_variable>
#include <future>
#include <list>
#include <memory>
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
    LIST,
    STREAM
};

struct StreamEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> fields;
    StreamEntry(const std::string& entry_id)
        : id(entry_id)
    {
    }
};

/* 列表类型 */
using ListValue = std::vector<std::string>;
/* Stream类型 */
using Stream = std::vector<StreamEntry>;
/* 值的变体 */
using ValueVariant = std::variant<std::string, ListValue, Stream>;
/* 等待的客户端信息 */
struct WaitingClient {
    std::string key;
    boost::asio::steady_timer timer; // 添加 ASIO 定时器
    std::chrono::steady_clock::time_point wakeup_time;
    std::weak_ptr<std::promise<std::optional<std::string>>> promise;
    /* true = BLPOP, false = BRPOP */
    bool is_left;

    WaitingClient(boost::asio::io_context& io_context)
        : timer(io_context)
    {
    }
};
/* XREAD等待结构 */
struct XReadWaiter {
    std::string key;
    std::string id;
    std::size_t count { 0 };
    std::shared_ptr<std::promise<std::optional<std::vector<StreamEntry>>>> promise;
    boost::asio::steady_timer timer;
    std::chrono::steady_clock::time_point wakeup_time;

    XReadWaiter(boost::asio::io_context& io_content)
        : timer(io_content)
    {
    }
};

/* 值 */
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

    // Stream 构造函数
    ValueWithExpiry(const Stream& v)
        : value(v)
        , type(ValueType::STREAM)
        , has_expiry(false)
    {
    }

    ValueWithExpiry(const Stream& v, std::chrono::milliseconds ttl)
        : value(v)
        , type(ValueType::STREAM)
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
    /* 获取流 */
    std::optional<Stream> get_stream() const
    {
        if (type != ValueType::STREAM)
            return std::nullopt;
        return std::get<Stream>(value);
    }

    Stream* get_stream_ptr()
    {
        if (type != ValueType::STREAM)
            return nullptr;
        return &std::get<Stream>(value);
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
    /* 移除并返回列表的第一个/最后一个元素，如果没有元素则阻塞 */
    Task<std::optional<std::string>> co_blpop(const std::string& key, std::chrono::seconds timeout);
    Task<std::optional<std::string>> co_brpop(const std::string& key, std::chrono::seconds timeout);
    // 尝试弹出（不阻塞，用于先检查）
    std::optional<std::string> try_lpop(const std::string& key);
    std::optional<std::string> try_rpop(const std::string& key);
    /* 修剪列表，只保留指定范围内的元素 */
    bool ltrim(const std::string& key, int start, int stop);

    /**
        Stream操作****************************************************
    */

    std::string xadd(const std::string& key, const std::string& id, const std::vector<std::pair<std::string, std::string>>& fields);
    std::optional<std::vector<StreamEntry>> xrange(const std::string& key, const std::string& start, const std::string& end);
    bool is_stream(const std::string& key);
    std::unordered_map<std::string, std::vector<StreamEntry>> xread(const std::vector<std::pair<std::string, std::string>>& stream_requets, std::size_t count);
    Task<std::unordered_map<std::string, std::vector<StreamEntry>>> xread_block(const std::vector<std::pair<std::string, std::string>>& stream_requets, std::chrono::milliseconds timeout, std::size_t count);

    /**
        通用操作****************************************************
    */
    /* 删除键 */
    bool del(const std::string& key);
    /* 删除键组 */
    std::size_t del(const std::vector<std::string>& keys);
    /* 检查键是否存在 */
    bool exists(const std::string& key);
    /* 获取键所对应的类型 */
    std::optional<ValueType> type(const std::string& key);

private:
    Storage();

    /**
        清理线程****************************************************
    */
    /* 检查并移除过期的键 */
    void check_expiry(const std::string& key);
    /* 启动清理线程 */
    void start_cleaner();
    /* 关闭清理线程 */
    void stop_cleaner();
    /* 随机抽样清除过期键 */
    void clean_expired_random();
    /* 清理线程 */
    std::thread _cleaner;
    /* 清理线程退出标志 */
    std::atomic<bool> _quit;

    /**
        阻塞支持****************************************************
    */
    /* 唤醒等待者 */
    void notify_waiters(const std::string& key);
    void notify_xread_waiters(const std::string& key, const StreamEntry& new_entry);
    /* 检查超时的等待者 */
    void check_expired_waiters();
    /* 启动等待者清理线程 */
    void start_waiter_cleaner();

    struct KeyWaiters {
        std::list<std::shared_ptr<WaitingClient>> left_waiters;
        std::list<std::shared_ptr<WaitingClient>> right_waiters;
    };

    std::unordered_map<std::string, KeyWaiters> _waiters;
    std::condition_variable _waiter_cv;
    std::thread _waiter_cleaner;
    std::thread _io_thread;
    std::atomic<bool> _waiter_cleaner_running;
    std::mutex _waiter_mutex;

    std::unordered_map<std::string, std::list<std::shared_ptr<XReadWaiter>>> _xread_waiters;
    std::mutex _xread_waiter_mutex;

    std::unordered_map<std::string, ValueWithExpiry> _store;
    std::mutex _mutex;
    boost::asio::io_context io_context;
};

}