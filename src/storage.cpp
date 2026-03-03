#include "storage.hpp"
#include "task.hpp"

#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/system/detail/error_code.hpp>
#include <chrono>
#include <cstddef>
#include <future>
#include <iterator>
// #include <mutex>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>

namespace Redis {

Storage::Storage()
    : _quit(false)
{
    start_cleaner();

    _io_thread = std::thread([this]() {
        io_context.run();
    });
}

Storage::~Storage()
{
    stop_cleaner();
    io_context.stop();
    _io_thread.join();
    _waiter_cleaner.join();
}

Storage& Storage::instance()
{
    static Storage storage;
    return storage;
}

void Storage::start_cleaner()
{
    _cleaner = std::thread([this]() {
        while (!_quit.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (_quit.load()) {
                break;
            }
            clean_expired_random();
        }
    });
}

void Storage::stop_cleaner()
{
    _quit.store(true);
}

/*
    字符串操作****************************************************
*/
void Storage::set_string(const std::string& key, const std::string& value)
{
    // std::lock_guard<std::mutex> lock(_mutex);
    _store[key] = ValueWithExpiry(value);
}

void Storage::set_string_with_expiry_ms(const std::string& key, const std::string& value, std::chrono::milliseconds ttl)
{
    // std::lock_guard<std::mutex> lock(_mutex);
    _store[key] = ValueWithExpiry(value, ttl);
}

std::optional<std::string> Storage::get_string(const std::string& key)
{
    // std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return std::nullopt;
    }

    // 检查是否过期
    if (it->second.is_expired()) {
        _store.erase(it);
        return std::nullopt;
    }

    return std::get<std::string>(it->second.value);
}

/**
    列表操作****************************************************
*/

std::optional<std::string> Storage::lindex(const std::string& key, int index)
{
    // std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::LIST) {
        return std::nullopt;
    }
    auto list = it->second.get_list();
    if (!list || list->empty()) {
        return std::nullopt;
    }
    int size = static_cast<int>(list->size());
    if (index < 0)
        index = size + index;
    if (index < 0 || index >= size) {
        return std::nullopt;
    }
    return (*list)[index];
}

size_t Storage::lrem(const std::string& key, int count, const std::string& value)
{
    // std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::LIST) {
        return 0;
    }

    auto* list = it->second.get_list_ptr();
    if (!list || list->empty()) {
        return 0;
    }

    std::size_t removed = 0;
    if (count > 0) {
        for (auto it = list->begin(); it != list->end() && removed < static_cast<size_t>(count);) {
            if (*it == value) {
                it = list->erase(it);
                removed++;
            } else {
                ++it;
            }
        }
    } else if (count < 0) {
        count = -count;
        for (auto it = list->rbegin(); it != list->rend() && removed < static_cast<size_t>(count);) {
            if (*it == value) {
                it = std::reverse_iterator<decltype(it.base())>(list->erase(std::prev(it.base())));
                removed++;
            } else {
                ++it;
            }
        }
    } else {
        for (auto it = list->begin(); it != list->end();) {
            if (*it == value) {
                it = list->erase(it);
                removed++;
            } else {
                ++it;
            }
        }
    }
    return removed;
}

size_t Storage::rpush(const std::string& key, const std::string& value)
{
    std::size_t result;
    {

        // std::lock_guard<std::mutex> lock(_mutex);
        auto it = _store.find(key);
        if (it == _store.end()) {
            ListValue new_list = { value };
            _store[key] = ValueWithExpiry(new_list);
            result = 1;
        } else {

            if (it->second.type != ValueType::LIST) {
                throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            auto* list = it->second.get_list_ptr();
            list->push_back(value);
            result = list->size();
        }
    }
    notify_waiters(key);
    return result;
}

size_t Storage::rpush_multi(const std::string& key, const std::vector<std::string>& values)
{
    if (values.empty()) {
        auto len = llen(key);
        return len.value_or(0);
    }

    std::size_t result;
    {
        // std::lock_guard<std::mutex> lock(_mutex);
        auto it = _store.find(key);
        if (it == _store.end()) {
            // 列表不存在，创建新列表
            _store[key] = ValueWithExpiry(values);
            result = 1;
        } else {

            // 检查类型
            if (it->second.type != ValueType::LIST) {
                throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            // 头部插入元素
            auto* list = it->second.get_list_ptr();
            list->insert(list->end(), values.begin(), values.end());

            result = list->size();
        }
    }
    notify_waiters(key);
    return result;
}

size_t Storage::lpush(const std::string& key, const std::string& value)
{
    std::size_t result;

    {
        // std::lock_guard<std::mutex> lock(_mutex);
        auto it = _store.find(key);
        if (it == _store.end()) {
            ListValue new_list = { value };
            _store[key] = new_list;
            result = 1;
        } else {
            if (it->second.type != ValueType::LIST) {
                throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            auto* list = it->second.get_list_ptr();
            list->insert(list->begin(), value);
            result = list->size();
        }
    }
    notify_waiters(key);
    return result;
}

size_t Storage::lpush_multi(const std::string& key, const std::vector<std::string>& values)
{
    if (values.empty()) {
        auto len = llen(key);
        return len.value_or(0);
    }

    std::size_t result;
    {
        // std::lock_guard<std::mutex> lock(_mutex);
        auto it = _store.find(key);
        if (it == _store.end()) {
            _store[key] = ValueWithExpiry(values);
            result = 1;
        } else {
            if (it->second.type != ValueType::LIST) {
                throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
            auto* list = it->second.get_list_ptr();
            list->insert(list->begin(), values.begin(), values.end());
            result = list->size();
        }
    }
    notify_waiters(key);
    return result;
}

std::optional<ListValue> Storage::get_list(const std::string& key)
{
    // std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return std::nullopt;
    }

    return it->second.get_list();
}

std::optional<ListValue> Storage::lrange(const std::string& key, int start, int stop)
{
    // std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end()) {
        return std::nullopt;
    }

    auto list = it->second.get_list();
    if (!list) {
        return std::nullopt;
    }

    int size = static_cast<int>(list->size());
    if (start < 0)
        start = size + start;

    if (stop < 0)
        stop = size + stop;

    if (start < 0)
        start = 0;
    if (stop >= size)
        stop = size - 1;

    if (start > stop || start >= size || stop < 0) {
        return ListValue {};
    }

    ListValue result;
    for (int i = start; i <= stop && i < size; ++i) {
        result.push_back((*list)[i]);
    }

    return result;
}

bool Storage::lset(const std::string& key, int index, const std::string& value)
{
    // std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::LIST) {
        // return false;
        throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    auto* list = it->second.get_list_ptr();
    if (!list) {
        return false;
    }

    int size = static_cast<int>(list->size());
    if (index < 0)
        index = size + index;
    if (index < 0 || index >= size) {
        // return false;
        throw std::runtime_error("ERR index out of range");
    }
    (*list)[index] = value;
    return true;
}

std::optional<size_t> Storage::llen(const std::string& key)
{
    // std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::LIST) {
        return std::nullopt;
    }
    auto list = it->second.get_list();
    if (!list) {
        return std::nullopt;
    }
    return list->size();
}

bool Storage::is_list(const std::string& key)
{
    // std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return false;
    }

    return it->second.type == ValueType::LIST;
}

std::optional<std::string> Storage::lpop(const std::string& key)
{
    // std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::LIST) {
        return std::nullopt;
    }
    auto* list = it->second.get_list_ptr();
    if (!list || list->empty()) {
        return std::nullopt;
    }

    std::string value = list->front();
    list->erase(list->begin());

    if (list->empty()) {
        _store.erase(key);
    }
    return value;
}

std::optional<std::string> Storage::rpop(const std::string& key)
{
    // std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::LIST) {
        return std::nullopt;
    }

    auto* list = it->second.get_list_ptr();
    if (!list || list->empty()) {
        return std::nullopt;
    }

    std::string value = list->back();
    list->pop_back();
    if (list->empty()) {
        _store.erase(key);
    }
    return value;
}

Task<std::optional<std::string>> Storage::co_blpop(const std::string& key, std::chrono::seconds timeout)
{
    auto value = try_lpop(key);
    if (value) {
        co_return value;
    }

    auto promise = std::make_shared<std::promise<std::optional<std::string>>>();
    auto future = promise->get_future().share();

    auto client = std::make_shared<WaitingClient>(io_context);
    client->key = key;
    client->promise = promise;
    client->is_left = true;
    {
        std::lock_guard<std::mutex> lock(_waiter_mutex);
        _waiters[key].left_waiters.push_back(client);
    }

    if (timeout != std::chrono::seconds::zero()) {
        client->timer.expires_after(timeout);
        client->timer.async_wait([this, client, key, promise](const boost::system::error_code& ec) {
            /* 主动取消 */

            if (ec == boost::asio::error::operation_aborted) {
                return;
            }

            /* 正常超时 */

            bool removed = false;
            {
                std::lock_guard<std::mutex> lock(_waiter_mutex);
                auto& waiters = _waiters[key].left_waiters;

                // 从等待队列中移除
                auto it = std::find_if(waiters.begin(), waiters.end(),
                    [client](const auto& c) { return c == client; });

                if (it != waiters.end()) {
                    waiters.erase(it);
                    removed = true;
                }
            }

            if (removed) {
                // 设置超时结果
                promise->set_value(std::nullopt);
            }
        });
    }

    AwaitableFuture<std::optional<std::string>> awaitable { future };
    auto result = co_await awaitable;
    co_return result;
}

Task<std::optional<std::string>> Storage::co_brpop(const std::string& key, std::chrono::seconds timeout)
{
    auto value = try_rpop(key);
    if (value) {
        co_return value;
    }

    auto promise = std::make_shared<std::promise<std::optional<std::string>>>();
    auto future = promise->get_future();

    auto client = std::make_shared<WaitingClient>(io_context);
    client->key = key;
    client->promise = promise;
    client->is_left = false;
    {
        std::lock_guard<std::mutex> lock(_waiter_mutex);
        _waiters[key].right_waiters.push_back(client);
    }

    if (timeout != std::chrono::seconds::zero()) {
        client->timer.expires_after(timeout);
        client->timer.async_wait([this, client, key, promise](const boost::system::error_code& ec) {
            /* 正常超时 */
            if (!ec) {
                bool removed = false;
                {
                    std::lock_guard<std::mutex> lock(_waiter_mutex);
                    auto& waiters = _waiters[key].right_waiters;

                    // 从等待队列中移除
                    auto it = std::find_if(waiters.begin(), waiters.end(),
                        [client](const auto& c) { return c == client; });

                    if (it != waiters.end()) {
                        waiters.erase(it);
                        removed = true;
                    }
                }

                if (removed) {
                    // 设置超时结果
                    promise->set_value(std::nullopt);
                }
            }
        });
    }

    AwaitableFuture<std::optional<std::string>> awaitable { std::move(future) };
    auto result = co_await awaitable;
    co_return result;
}

std::optional<std::string> Storage::try_lpop(const std::string& key)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::LIST) {
        // _store[key] = ValueWithExpiry { ListValue {} };
        return std::nullopt;
    }

    auto* list = it->second.get_list_ptr();
    if (!list || list->empty()) {
        return std::nullopt;
    }

    std::string value = list->front();
    list->erase(list->begin());

    if (list->empty()) {
        _store.erase(key);
    }
    return value;
}
std::optional<std::string> Storage::try_rpop(const std::string& key)
{
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::LIST) {
        // _store[key] = ValueWithExpiry { ListValue {} };
        return std::nullopt;
    }

    auto* list = it->second.get_list_ptr();
    if (!list || list->empty()) {
        return std::nullopt;
    }

    std::string value = list->back();
    list->pop_back();

    if (list->empty()) {
        _store.erase(key);
    }

    return value;
}

bool Storage::ltrim(const std::string& key, int start, int stop)
{
    // std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::LIST) {
        return false;
    }

    auto* list = it->second.get_list_ptr();
    if (!list) {
        return false;
    }

    int size = static_cast<int>(list->size());
    if (start < 0)
        start = size + start;
    if (stop < 0)
        stop = size + stop;

    // 边界检查
    if (start < 0)
        start = 0;
    if (stop >= size)
        stop = size - 1;
    if (start > stop || start >= size || stop < 0) {
        // 如果范围无效，清空列表
        // _store.erase(key);
        // return true;
        return false;
    }

    ListValue new_list;
    for (int i = start; i <= stop && i < size; ++i) {
        new_list.push_back((*list)[i]);
    }

    // 替换原列表
    if (new_list.empty()) {
        _store.erase(key);
    } else {
        it->second.value = new_list;
    }

    return true;
}

/**
    阻塞支持****************************************************
*/

void Storage::notify_waiters(const std::string& key)
{
    // std::lock_guard<std::mutex> lock(_waiter_mutex);
    auto it = _waiters.find(key);
    if (it == _waiters.end() || (it->second.left_waiters.empty() && it->second.right_waiters.empty())) {
        return;
    }

    auto& waiters = it->second;
    if (!waiters.left_waiters.empty()) {
        auto client = waiters.left_waiters.front();
        waiters.left_waiters.pop_front();

        client->timer.cancel();

        auto value = try_lpop(key);
        if (auto p = client->promise.lock()) {
            p->set_value(value);
        }

    } else if (!waiters.right_waiters.empty()) {
        auto client = waiters.right_waiters.front();
        waiters.right_waiters.pop_front();

        client->timer.cancel();

        auto value = try_rpop(key);
        if (auto p = client->promise.lock()) {
            p->set_value(value);
        }
    }
}

void Storage::check_expired_waiters()
{
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(_mutex);
    for (auto it = _waiters.begin(); it != _waiters.end();) {
        bool has_any = false;

        auto& left = it->second.left_waiters;
        for (auto w_it = left.begin(); w_it != left.end();) {
            if (now >= ((*w_it)->wakeup_time)) {
                /* 过期 */
                if (auto p = (*w_it)->promise.lock()) {
                    p->set_value(std::nullopt);
                }
                w_it = left.erase(w_it);
            } else {
                ++w_it;
                has_any = true;
            }
        }

        auto& right = it->second.right_waiters;
        for (auto w_it = right.begin(); w_it != right.end();) {
            if (now >= ((*w_it)->wakeup_time)) {
                if (auto p = (*w_it)->promise.lock()) {
                    p->set_value(std::nullopt);
                }
                w_it = right.erase(w_it);
            } else {
                ++w_it;
                has_any = true;
            }
        }
        if (!has_any) {
            it = _waiters.erase(it);
        } else {
            ++it;
        }
    }
}

void Storage::start_waiter_cleaner()
{
    _waiter_cleaner_running = true;
    _waiter_cleaner = std::thread([this]() {
        while (_waiter_cleaner_running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            check_expired_waiters();
        }
    });
}

/**
    通用操作****************************************************
*/
void Storage::check_expiry(const std::string& key)
{
    auto it = _store.find(key);
    if (it != _store.end() && it->second.is_expired()) {
        _store.erase(it);
    }
}

bool Storage::del(const std::string& key)
{
    // std::lock_guard<std::mutex> lock(_mutex);
    return _store.erase(key) > 0;
}

std::size_t Storage::del(const std::vector<std::string>& keys)
{
    auto& storage = Storage::instance();
    int deleted = 0;
    for (std::size_t i = 0; i < keys.size(); ++i) {
        if (storage.del(keys[i])) {
            deleted++;
        }
    }
    return deleted;
}

bool Storage::exists(const std::string& key)
{
    // std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return false;
    }

    if (it->second.is_expired()) {
        _store.erase(it);
        return false;
    }

    return true;
}

void Storage::clean_expired_random()
{
    // 1.循环检测
    // std::lock_guard<std::mutex> lock(_mutex);

    // for (auto it = _store.begin(); it != _store.end();) {
    //     if (it->second.is_expired()) {
    //         it = _store.erase(it);
    //     } else {
    //         ++it;
    //     }
    // }

    // 2.抽样检测
    constexpr int SAMPLES_PER_CIRCLE = 20; // 每轮抽取20个样本
    constexpr double EXPIRE_THRESHOLD = 0.25; // 过期比例 > 25% 继续
    constexpr int MAX_CIRCLE = 10; // 最大循环次数

    static thread_local std::mt19937 gen(std::random_device {}());

    for (std::size_t circle = 0; circle < MAX_CIRCLE; ++circle) {

        int expired = 0;
        int checked = 0;

        {
            // std::lock_guard<std::mutex> lock(_mutex);
            if (_store.empty()) {
                return;
            }

            std::uniform_int_distribution<std::size_t> start_dist(0, _store.size() - 1);

            std::size_t start_index = start_dist(gen);

            auto it = _store.begin();
            std::advance(it, start_index);
            for (int i = 0; i < SAMPLES_PER_CIRCLE && it != _store.end(); ++i) {
                checked++;
                if (it->second.is_expired()) {
                    it = _store.erase(it);
                    expired++;
                } else {
                    ++it;
                }
            }
        }
        if (checked == 0 || static_cast<double>(expired) / checked < EXPIRE_THRESHOLD) {
            break;
        }
    }
}
}