#include "storage.hpp"
#include "stream_utils.hpp"
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
#include <unordered_map>

namespace Redis {

Storage::Storage()
    : _quit(false)
{
    start_cleaner();
    start_waiter_cleaner();

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

std::optional<uint64_t> Storage::get_version(const std::string& key)
{
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return std::nullopt;
    }

    if (it->second.is_expired()) {
        _store.erase(it);
        return std::nullopt;
    }

    return it->second.version;
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
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end()) {
        _store[key] = ValueWithExpiry(value);
    } else {
        it->second.value = value;
        it->second.version++; // 版本号递增
    }
}

void Storage::set_string_with_expiry_ms(const std::string& key, const std::string& value, std::chrono::milliseconds ttl)
{
    // std::lock_guard<std::mutex> lock(_mutex);
    _store[key] = ValueWithExpiry(value, ttl);

    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end()) {
        _store[key] = ValueWithExpiry(value, ttl);
    } else {
        it->second.has_expiry = true;
        it->second.expiry = std::chrono::steady_clock::now() + ttl;
        it->second.value = value;
        it->second.version++; // 版本号递增
    }
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

std::vector<std::string> Storage::keys()
{
    std::vector<std::string> all_keys;
    for (const auto& pair : _store) {
        all_keys.push_back(pair.first);
    }
    return all_keys;
}

int Storage::ttl(std::string& key)
{
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return -2; // 键不存在
    }

    // 检查是否过期
    if (it->second.is_expired()) {
        _store.erase(it);
        return -2; // 键已过期，视为不存在
    }

    if (!it->second.has_expiry) {
        return -1; // 永不过期
    }

    // 计算剩余时间
    auto now = std::chrono::steady_clock::now();
    auto remaining = it->second.expiry - now;

    // 如果已经过期
    if (remaining <= std::chrono::seconds(0)) {
        _store.erase(it);
        return -2;
    }

    // 转换为秒
    auto remaining_sec = std::chrono::duration_cast<std::chrono::seconds>(remaining).count();
    return remaining_sec;
}
int Storage::pttl(std::string& key)
{
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return -2; // 键不存在
    }

    // 检查是否过期
    if (it->second.is_expired()) {
        _store.erase(it);
        return -2; // 键已过期，视为不存在
    }

    if (!it->second.has_expiry) {
        return -1; // 永不过期
    }

    // 计算剩余时间
    auto now = std::chrono::steady_clock::now();
    auto remaining = it->second.expiry - now;

    // 如果已经过期
    if (remaining <= std::chrono::milliseconds(0)) {
        _store.erase(it);
        return -2;
    }

    // 转换为毫秒
    auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
    return remaining_ms;
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
    if (removed != 0) {
        it->second.version++;
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
            it->second.version++;
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
            it->second.version++;
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
            it->second.version++;
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
            it->second.version++;
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
        throw std::runtime_error("index out of range");
    }
    (*list)[index] = value;
    it->second.version++;
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
    it->second.version++;

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
    it->second.version++;
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
    if (timeout == std::chrono::seconds::zero()) {
        timeout = std::chrono::seconds(99999);
    }
    client->wakeup_time = std::chrono::steady_clock::now() + timeout;
    {
        std::lock_guard<std::mutex> lock(_waiter_mutex);
        _waiters[key].left_waiters.push_back(client);
    }

    if (timeout != std::chrono::seconds::zero()) {
        client->timer.expires_after(timeout);
        client->timer.async_wait([this, client, key, promise](const boost::system::error_code& ec) {
            if (!ec) {
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
    if (timeout == std::chrono::seconds::zero()) {
        timeout = std::chrono::seconds(99999);
    }
    client->wakeup_time = std::chrono::steady_clock::now() + timeout;
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
    it->second.version++;

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
    it->second.version++;

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
    it->second.version++;

    return true;
}

std::string Storage::xadd(const std::string& key, const std::string& id, const std::vector<std::pair<std::string, std::string>>& fields)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    std::optional<std::string> last_id;
    if (it != _store.end()) {
        if (it->second.type != ValueType::STREAM) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        auto* stream = it->second.get_stream_ptr();
        if (!stream->empty()) {
            last_id = stream->back().id;
        }
    }

    std::string final_id;
    try {
        /* 如果是*或-*就生成，如果本身ok就直接检查后返回 */
        final_id = Utils::StreamUtils::make_id_from_spec(id, last_id);
    } catch (const std::runtime_error& e) {
        throw;
    }

    StreamEntry new_entry(final_id);
    new_entry.fields = fields; // 直接赋值整个 vector

    if (it == _store.end()) {
        Stream new_stream;
        new_stream.push_back(new_entry);
        _store[key] = ValueWithExpiry(new_stream);
    } else {
        auto* stream = it->second.get_stream_ptr();
        stream->push_back(new_entry);
    }
    it->second.version++;
    notify_xread_waiters(key, new_entry);
    return final_id;
}
std::optional<std::vector<StreamEntry>> Storage::xrange(const std::string& key, const std::string& start, const std::string& end)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end() || it->second.type != ValueType::STREAM) {
        return std::nullopt;
    }
    auto* stream = it->second.get_stream_ptr();
    if (stream->empty()) {
        return std::vector<StreamEntry>();
    }
    uint64_t start_ms, end_ms, start_seq, end_seq;

    if (!Utils::StreamUtils::parse_range_id(start, start_ms, start_seq, true)) {
        throw std::runtime_error("Invalid start ID");
    }
    if (!Utils::StreamUtils::parse_range_id(end, end_ms, end_seq, false)) {
        throw std::runtime_error("Invalid end ID");
    }

    std::vector<StreamEntry> result;
    for (const auto& entry : *stream) {
        uint64_t ms, seq;
        if (!Utils::StreamUtils::parse_range_id(entry.id, ms, seq, true)) {
            continue;
        }
        if (Utils::StreamUtils::is_id_in_range(ms, seq, start_ms, start_seq, end_ms, end_seq)) {
            result.push_back(entry);
        }
    }
    return result;
}

bool Storage::is_stream(const std::string& key)
{
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return false;
    }

    if (it->second.is_expired()) {
        _store.erase(it);
        return false;
    }

    return it->second.type == ValueType::STREAM;
}

std::unordered_map<std::string, std::vector<StreamEntry>> Storage::xread(const std::vector<std::pair<std::string, std::string>>& stream_requets, std::size_t count)
{
    std::lock_guard<std::mutex> lock(_mutex);
    std::unordered_map<std::string, std::vector<StreamEntry>> result;

    for (const auto& request : stream_requets) {
        const std::string& key = request.first;
        const std::string& id = request.second;
        auto it = _store.find(key);
        if (it == _store.end()) {
            continue;
        }

        if (it->second.type != ValueType::STREAM) {
            throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        auto* stream = it->second.get_stream_ptr();
        if (stream->empty()) {
            /* 空流跳过 */
            continue;
        }

        uint64_t req_ms, req_seq;
        if (!Utils::StreamUtils::parse_id(id, req_ms, req_seq)) {
            throw std::runtime_error("ERR Invalid stream ID format");
        }

        std::vector<StreamEntry> entries;
        for (const auto& entry : *stream) {
            uint64_t ms, seq;
            if (!Utils::StreamUtils::parse_id(entry.id, ms, seq)) {
                continue;
            }
            /* 只返回ID大于请求ID的条目 */
            if (ms > req_ms || (ms == req_ms && seq > req_seq)) {
                entries.push_back(entry);

                if (count > 0 && entries.size() >= count) {
                    break;
                }
            }
        }
        if (!entries.empty()) {
            result[key] = std::move(entries);
        }
    }
    return result;
}

Task<std::unordered_map<std::string, std::vector<StreamEntry>>> Storage::xread_block(const std::vector<std::pair<std::string, std::string>>& stream_requests, std::chrono::milliseconds timeout, std::size_t count)
{
    auto immediate_result = xread(stream_requests, count);

    if (!immediate_result.empty()) {
        co_return immediate_result;
    }

    if (timeout == std::chrono::milliseconds::zero()) {
        timeout = std::chrono::milliseconds::max();
    }

    auto promise = std::make_shared<std::promise<std::optional<std::vector<StreamEntry>>>>();
    auto future = promise->get_future();

    const auto& first_request = stream_requests[0];
    const std::string& key = first_request.first;
    const std::string& id = first_request.second;

    auto waiter = std::make_shared<XReadWaiter>(io_context);
    waiter->key = key;
    waiter->id = id;
    waiter->promise = promise;
    waiter->count = count;
    waiter->wakeup_time = std::chrono::steady_clock::now() + timeout;

    {
        std::lock_guard<std::mutex> lock(_xread_waiter_mutex);
        _xread_waiters[key].push_back(waiter);
    }

    if (timeout != std::chrono::milliseconds::max()) {
        waiter->timer.expires_after(timeout);
        waiter->timer.async_wait([this, waiter, key](const boost::system::error_code& ec) {
            if (!ec) {
                bool removed = false;
                {
                    std::lock_guard<std::mutex> lock(_xread_waiter_mutex);
                    auto& waiters = _xread_waiters[key];
                    auto it = std::find_if(waiters.begin(), waiters.end(),
                        [waiter](const auto& w) { return w == waiter; });

                    if (it != waiters.end()) {
                        waiters.erase(it);
                        removed = true;
                    }
                }

                if (removed) {
                    waiter->promise->set_value(std::nullopt);
                }
            }
        });
    }

    auto result_opt = co_await AwaitableFuture<std::optional<std::vector<StreamEntry>>>(std::move(future));

    if (!result_opt) {
        co_return std::unordered_map<std::string, std::vector<StreamEntry>> {};
    }

    std::unordered_map<std::string, std::vector<StreamEntry>> result;
    result[key] = *result_opt;
    co_return result;
}

/**
    阻塞支持****************************************************
*/

void Storage::notify_waiters(const std::string& key)
{
    std::lock_guard<std::mutex> lock(_waiter_mutex);
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

void Storage::notify_xread_waiters(const std::string& key, const StreamEntry& new_entry)
{
    std::lock_guard<std::mutex> lock(_xread_waiter_mutex);

    auto it = _xread_waiters.find(key);
    if (it == _xread_waiters.end() || it->second.empty()) {
        return;
    }

    auto it_stream = _store.find(key);
    bool stream_valid = (it_stream != _store.end() && it_stream->second.type == ValueType::STREAM);

    auto& waiters = it->second;

    if (!stream_valid) {
        for (const auto& waiter : waiters) {
            waiter->timer.cancel();
            waiter->promise->set_value(std::nullopt);
        }
        _xread_waiters.erase(it);
        return;
    }

    auto* stream = it_stream->second.get_stream_ptr();
    std::vector<std::shared_ptr<XReadWaiter>> to_notify;
    to_notify.reserve(waiters.size());

    /* 记录当前加入的entry */
    uint64_t new_ms, new_seq;
    Utils::StreamUtils::parse_id(new_entry.id, new_ms, new_seq);

    auto waiter_it = waiters.begin();
    while (waiter_it != waiters.end()) {
        auto waiter = *waiter_it;

        uint64_t req_ms, req_seq;
        Utils::StreamUtils::parse_id(waiter->id, req_ms, req_seq);

        if (new_ms > req_ms || (new_ms == req_ms && new_seq > req_seq)) {
            to_notify.push_back(waiter);
            waiter_it = waiters.erase(waiter_it);
            waiter->timer.cancel();
        } else {
            ++waiter_it;
        }
    }

    if (to_notify.empty()) {
        return;
    }

    std::sort(to_notify.begin(), to_notify.end(), [](const auto& a, const auto& b) {
        /* 按照id排序 */
        return a->id < b->id;
    });

    // 为每个等待者准备结果
    std::unordered_map<std::shared_ptr<XReadWaiter>, std::vector<StreamEntry>> results;

    for (const auto& entry : *stream) {
        uint64_t ms, seq;
        Utils::StreamUtils::parse_id(entry.id, ms, seq);

        for (const auto& waiter : to_notify) {
            uint64_t req_ms, req_seq;
            Utils::StreamUtils::parse_id(waiter->id, req_ms, req_seq);

            auto& entries = results[waiter];

            if (waiter->count > 0 && entries.size() >= waiter->count) {
                continue;
            }

            if (ms > req_ms || (ms == req_ms && seq > req_seq)) {
                entries.push_back(entry);
            }
        }
    }

    for (const auto& waiter : to_notify) {
        waiter->promise->set_value(std::move(results[waiter]));
    }

    if (waiters.empty()) {
        _xread_waiters.erase(it);
    }
}

void Storage::check_expired_waiters()
{
    auto now = std::chrono::steady_clock::now();
    check_expired_blrpop_waiters(now);
    check_expired_xread_waiters(now);
}

void Storage::check_expired_blrpop_waiters(std::chrono::steady_clock::time_point& now)
{

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

void Storage::check_expired_xread_waiters(std::chrono::steady_clock::time_point& now)
{
    std::lock_guard<std::mutex> lock(_xread_waiter_mutex);
    for (auto it = _xread_waiters.begin(); it != _xread_waiters.end();) {
        auto& waiters = it->second;
        auto waiter_it = waiters.begin();

        while (waiter_it != waiters.end()) {
            auto waiter = *waiter_it;

            if (now >= waiter->wakeup_time) {
                waiter->timer.cancel();
                waiter->promise->set_value(std::nullopt);
                waiter_it = waiters.erase(waiter_it);
            } else {
                ++waiter_it;
            }
        }

        if (waiters.empty()) {
            it = _xread_waiters.erase(it);
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
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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

std::optional<ValueType> Storage::type(const std::string& key)
{
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return std::nullopt;
    }

    // 检查是否过期
    if (it->second.is_expired()) {
        _store.erase(it);
        return std::nullopt;
    }

    return it->second.type;
}

std::optional<std::size_t> Storage::incr(const std::string& key)
{
    std::unordered_map<std::string, ValueWithExpiry>::iterator it;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        it = _store.find(key);
    }

    if (it == _store.end()) {
        set_string(key, std::to_string(1));
        return 1;
    }

    if (it->second.is_expired()) {
        _store.erase(it);
        set_string(key, std::to_string(1));
        return 1;
    }

    if (it->second.type != ValueType::STRING) {
        throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    auto str_val = it->second.get_string();
    if (!str_val) {
        throw std::runtime_error("value is not an integer or out of range");
    }
    try {
        int64_t val = std::stoll(*str_val);
        val++; // 加1

        // 存回字符串
        it->second.value = std::to_string(val);
        it->second.version++;

        return val;
    } catch (const std::exception& e) {
        throw std::runtime_error("value is not an integer or out of range");
    }
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