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
#include <set>
#include <string>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <variant>

namespace Redis {

enum class ValueType {
    STRING,
    LIST,
    STREAM,
    SORTED_SET,
    HASH
};

struct StreamEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> fields;
    StreamEntry(const std::string& entry_id)
        : id(entry_id)
    {
    }
};

struct SortedSetMember {
    std::string member;
    double score;

    bool operator<(const SortedSetMember& other) const
    {
        if (score != other.score) {
            return score < other.score;
        }
        return member < other.member;
    }
};

class SortedSet {
private:
    std::multiset<SortedSetMember> _by_score;
    std::unordered_map<std::string, double> _by_member;

public:
    SortedSet() = default;

    SortedSet(const SortedSet& other) = default;

    SortedSet& operator=(const SortedSet& other) = default;

    SortedSet& operator=(SortedSet&& other) noexcept = default;
    SortedSet(SortedSet&& other) noexcept = default;

    bool add(const std::string& member, double score)
    {
        auto it = _by_member.find(member);
        if (it != _by_member.end()) {
            // 成员已存在，需要先删除旧的
            _by_score.erase({ it->first, it->second });
        }

        _by_member[member] = score;
        _by_score.insert({ member, score });
        return it == _by_member.end(); // 返回是否是新成员
    }
    std::optional<int> rank(const std::string& member) const
    {
        auto it = _by_member.find(member);
        if (it == _by_member.end()) {
            return std::nullopt;
        }
        // 使用 lower_bound 找到第一个可能的位置
        auto score_it = _by_score.lower_bound({ member, it->second });
        // 由于可能有相同分数，需要向前遍历找到正确的成员
        while (score_it != _by_score.end() && score_it->score == it->second) {
            if (score_it->member == member) {
                return std::distance(_by_score.begin(), score_it);
            }
            ++score_it;
        }
        return std::nullopt;
    }

    std::size_t size() const { return _by_member.size(); }

    bool contains(const std::string& member) const
    {
        return _by_member.find(member) != _by_member.end();
    }

    std::optional<double> score(const std::string& member) const
    {
        auto it = _by_member.find(member);
        if (it != _by_member.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    std::vector<std::string> range(int start, int stop) const
    {
        std::vector<std::string> result;

        if (_by_score.empty()) {
            return result;
        }

        if (stop == -1) {
            stop = _by_score.size() - 1;
        }

        if (start < 0 || start > stop) {
            return result;
        }

        // 调整 stop 索引
        size_t cardinality = _by_score.size();
        if (static_cast<size_t>(start) >= cardinality) {
            return result;
        }

        if (static_cast<size_t>(stop) >= cardinality) {
            stop = cardinality - 1;
        }

        if (start > stop) {
            return result;
        }

        // 遍历并收集结果
        int index = 0;
        for (const auto& item : _by_score) {
            if (index >= start && index <= stop) {
                result.push_back(item.member);
            }
            index++;
            if (index > stop)
                break;
        }

        return result;
    }

    std::vector<std::pair<std::string, double>> range_with_scores(int start, int stop) const
    {
        std::vector<std::pair<std::string, double>> result;

        if (_by_score.empty()) {
            return result;
        }

        if (stop == -1) {
            stop = _by_score.size() - 1;
        }

        if (start < 0 || start > stop) {
            return result;
        }

        size_t cardinality = _by_score.size();
        if (static_cast<size_t>(start) >= cardinality) {
            return result;
        }

        if (static_cast<size_t>(stop) >= cardinality) {
            stop = cardinality - 1;
        }

        if (start > stop) {
            return result;
        }

        int index = 0;
        for (const auto& item : _by_score) {
            if (index >= start && index <= stop) {
                result.emplace_back(item.member, item.score);
            }
            index++;
            if (index > stop)
                break;
        }

        return result;
    }

    bool remove(const std::string& member)
    {
        auto it = _by_member.find(member);
        if (it == _by_member.end()) {
            return false;
        }

        _by_score.erase({ it->first, it->second });
        _by_member.erase(it);
        return true;
    }

    size_t count(double min, double max) const
    {
        size_t cnt = 0;
        for (const auto& item : _by_score) {
            if (item.score >= min && item.score <= max) {
                cnt++;
            }
        }
        return cnt;
    }

    // 清空集合
    void clear()
    {
        _by_score.clear();
        _by_member.clear();
    }
};

// HASH类型
using HashValue = std::unordered_map<std::string, std::string>;
/* 列表类型 */
using ListValue = std::vector<std::string>;
/* Stream类型 */
using Stream = std::vector<StreamEntry>;
/* 值的变体 */
using ValueVariant = std::variant<std::string, ListValue, Stream, SortedSet, HashValue>;
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
    /* 版本号，每次修改递增 */
    uint64_t version { 1 };

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

    ValueWithExpiry(const SortedSet& v)
        : value(v)
        , type(ValueType::SORTED_SET)
        , has_expiry(false)
    {
    }

    ValueWithExpiry(const SortedSet& v, std::chrono::milliseconds ttl)
        : value(v)
        , type(ValueType::SORTED_SET)
        , expiry(std::chrono::steady_clock::now() + ttl)
        , has_expiry(true)
    {
    }

    ValueWithExpiry(const HashValue& v)
        : value(v)
        , type(ValueType::HASH)
        , has_expiry(false)
    {
    }

    ValueWithExpiry(HashValue&& v)
        : value(std::move(v))
        , type(ValueType::HASH)
        , has_expiry(false)
    {
    }

    ValueWithExpiry(const HashValue& v, std::chrono::milliseconds ttl)
        : value(v)
        , type(ValueType::HASH)
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

    std::optional<SortedSet> get_sorted_set() const
    {
        if (type != ValueType::SORTED_SET)
            return std::nullopt;
        return std::get<SortedSet>(value);
    }

    // 获取 SortedSet 指针
    SortedSet* get_sorted_set_ptr()
    {
        if (type != ValueType::SORTED_SET)
            return nullptr;
        return &std::get<SortedSet>(value);
    }

    std::optional<HashValue> get_hash() const
    {
        if (type != ValueType::HASH)
            return std::nullopt;
        return std::get<HashValue>(value);
    }

    // 获取 Hash 指针
    HashValue* get_hash_ptr()
    {
        if (type != ValueType::HASH)
            return nullptr;
        return &std::get<HashValue>(value);
    }
};

class Storage {
public:
    ~Storage();
    static Storage& instance();
    std::unordered_map<std::string, ValueWithExpiry>& get_store() { return _store; }

    /* 获取版本号 */
    std::optional<uint64_t> get_version(const std::string& key);
    /*
        字符串操作****************************************************
    */
    /* 设置键值对 */
    void set_string(const std::string& key, const std::string& value);
    /* 设置键值对+时间 */
    void set_string_with_expiry_ms(const std::string& key, const std::string& value, std::chrono::milliseconds ttl);
    /* 获得键 */
    std::optional<std::string> get_string(const std::string& key);
    /* 获得所有的键 */
    std::vector<std::string> keys();
    std::size_t size() const { return _store.size(); }
    int ttl(std::string& key);
    int pttl(std::string& key);

    /* 有序集合 */
    int zadd(const std::string& key, double score, const std::string& member);
    std::optional<int> zrank(const std::string& key, const std::string& member);
    // 检查是否是 sorted set
    bool is_sorted_set(const std::string& key) const;
    std::optional<std::vector<std::string>> zrange(
        const std::string& key, int start, int stop);
    std::optional<std::vector<std::pair<std::string, double>>> zrangewithscores(
        const std::string& key, int start, int stop);
    // ZCARD (集合大小)
    std::optional<size_t> zcard(const std::string& key);
    // ZREM (删除成员)
    bool zrem(const std::string& key, const std::string& member);
    // ZCOUNT (分数范围内的成员数)
    std::optional<size_t> zcount(const std::string& key, double min, double max);
    // ZINCRBY (增加分数)
    std::optional<double> zincrby(const std::string& key, double increment, const std::string& member);
    // ZREVRANK (倒序排名)
    std::optional<int> zrevrank(const std::string& key, const std::string& member);
    // ZREVRANGE (倒序范围)
    std::optional<std::vector<std::string>> zrevrange(
        const std::string& key, int start, int stop);
    // ZPOPMIN (弹出最小分数成员)
    std::optional<std::pair<std::string, double>> zpopmin(const std::string& key);
    // ZPOPMAX (弹出最大分数成员)
    std::optional<std::pair<std::string, double>> zpopmax(const std::string& key);
    // ZSCORE (获取分数)
    std::optional<double> zscore(const std::string& key, const std::string& member);

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
    /* 整形数值加1 */
    std::optional<std::size_t> incr(const std::string& key);

    /**
        HASH操作****************************************************
    */
    // HSET 操作
    int hset(const std::string& key, const std::string& field, const std::string& value);
    // HGET 操作
    std::optional<std::string> hget(const std::string& key, const std::string& field);
    // HGETALL 操作
    std::optional<HashValue> hgetall(const std::string& key);
    // HDEL 操作
    int hdel(const std::string& key, const std::vector<std::string>& fields);
    // HEXISTS 操作
    bool hexists(const std::string& key, const std::string& field);
    // HLEN 操作
    std::optional<size_t> hlen(const std::string& key);
    // HKEYS 操作
    std::optional<std::vector<std::string>> hkeys(const std::string& key);
    // HVALS 操作
    std::optional<std::vector<std::string>> hvals(const std::string& key);
    // HMSET 操作
    void hmset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& field_values);
    // HMGET 操作
    std::vector<std::optional<std::string>> hmget(const std::string& key, const std::vector<std::string>& fields);
    // HINCRBY 操作
    std::optional<int64_t> hincrby(const std::string& key, const std::string& field, int64_t increment);
    // HINCRBYFLOAT 操作
    std::optional<double> hincrbyfloat(const std::string& key, const std::string& field, double increment);
    // HSETNX 操作
    bool hsetnx(const std::string& key, const std::string& field, const std::string& value);
    // HSTRLEN 操作
    std::optional<size_t> hstrlen(const std::string& key, const std::string& field);
    // 检查是否是 hash
    bool is_hash(const std::string& key) const;

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
    /* 清理超时等待操作 */
    void check_expired_waiters();
    /* 检查超时的队列等待者 */
    void check_expired_blrpop_waiters(std::chrono::steady_clock::time_point& now);
    /* 检查超时的Stream等待者 */
    void check_expired_xread_waiters(std::chrono::steady_clock::time_point& now);
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
    mutable std::mutex _mutex;
    boost::asio::io_context io_context;
};
}