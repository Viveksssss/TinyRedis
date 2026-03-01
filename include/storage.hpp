#pragma once

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <sys/types.h>
#include <thread>
#include <unordered_map>

namespace Redis {

struct ValueWithExpiry {
    /* Value */
    std::string value;
    /* 过期时间点 */
    std::chrono::time_point<std::chrono::steady_clock> expiry;
    /* 是否有过期时间 */
    bool has_expiry;

    ValueWithExpiry() = default;
    ValueWithExpiry(const std::string& v)
        : value(v)
        , has_expiry(false)
    {
    }

    ValueWithExpiry(const std::string& v, std::chrono::milliseconds ttl)
        : value(v)
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
};

class Storage {
public:
    ~Storage();
    static Storage& instance();
    /* 设置键值对 */
    void set(const std::string& key, const std::string& value);
    /* 设置键值对+时间 */
    void set_with_expiry_ms(const std::string& key, const std::string& value, std::chrono::milliseconds ttl);
    /* 获得键 */
    std::optional<std::string> get(const std::string& key);
    /* 删除键 */
    bool del(const std::string& key);
    /* 删除键组 */
    std::size_t del(const std::vector<std::string>& keys);
    /* 检查键是否存在 */
    bool exists(const std::string& key);
    /* 清除过期键 */
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