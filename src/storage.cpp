#include "storage.hpp"

#include <chrono>
#include <cstddef>
#include <iterator>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <thread>

namespace Redis {

Storage::Storage()
    : _quit(false)
{
    start_cleaner();
}

Storage::~Storage()
{
    stop_cleaner();
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
    std::lock_guard<std::mutex> lock(_mutex);
    _store[key] = ValueWithExpiry(value);
}

void Storage::set_string_with_expiry_ms(const std::string& key, const std::string& value, std::chrono::milliseconds ttl)
{
    std::lock_guard<std::mutex> lock(_mutex);
    _store[key] = ValueWithExpiry(value, ttl);
}

std::optional<std::string> Storage::get_string(const std::string& key)
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

    return std::get<std::string>(it->second.value);
}

/**
    列表操作****************************************************
*/

std::optional<std::string> Storage::lindex(const std::string& key, int index)
{
    std::lock_guard<std::mutex> lock(_mutex);
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
    std::lock_guard<std::mutex> lock(_mutex);
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
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _store.find(key);
    if (it == _store.end()) {
        ListValue new_list = { value };
        _store[key] = ValueWithExpiry(new_list);
        return 1;
    }

    if (it->second.type != ValueType::LIST) {
        throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    auto* list = it->second.get_list_ptr();
    list->push_back(value);
    return list->size();
}

size_t Storage::rpush_multi(const std::string& key, const std::vector<std::string>& values)
{
    if (values.empty()) {
        auto len = llen(key);
        return len.value_or(0);
    }

    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        // 列表不存在，创建新列表
        _store[key] = ValueWithExpiry(values);
        return values.size();
    }

    // 检查类型
    if (it->second.type != ValueType::LIST) {
        throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    // 头部插入元素
    auto* list = it->second.get_list_ptr();
    list->insert(list->end(), values.begin(), values.end());

    return list->size();
}

size_t Storage::lpush(const std::string& key, const std::string& value)
{
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        ListValue new_list = { value };
        _store[key] = new_list;
        return 1;
    }

    if (it->second.type != ValueType::LIST) {
        throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    auto* list = it->second.get_list_ptr();
    list->insert(list->begin(), value);
    return list->size();
}

size_t Storage::lpush_multi(const std::string& key, const std::vector<std::string>& values)
{
    if (values.empty()) {
        auto len = llen(key);
        return len.value_or(0);
    }

    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        _store[key] = ValueWithExpiry(values);
        return 1;
    }

    if (it->second.type != ValueType::LIST) {
        throw std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    auto* list = it->second.get_list_ptr();
    list->insert(list->begin(), values.begin(), values.end());
    return list->size();
}

std::optional<ListValue> Storage::get_list(const std::string& key)
{
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return std::nullopt;
    }

    return it->second.get_list();
}

std::optional<ListValue> Storage::lrange(const std::string& key, int start, int stop)
{
    std::lock_guard<std::mutex> lock(_mutex);
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
    std::lock_guard<std::mutex> lock(_mutex);

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
    std::lock_guard<std::mutex> lock(_mutex);

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
    std::lock_guard<std::mutex> lock(_mutex);

    auto it = _store.find(key);
    if (it == _store.end()) {
        return false;
    }

    return it->second.type == ValueType::LIST;
}

std::optional<std::string> Storage::lpop(const std::string& key)
{
    std::lock_guard<std::mutex> lock(_mutex);
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
    std::lock_guard<std::mutex> lock(_mutex);
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

std::optional<std::string> Storage::blpop(const std::string& key, std::chrono::seconds timeout)
{
    // 暂时不实现
    return std::nullopt;
}

std::optional<std::string> Storage::brpop(const std::string& key, std::chrono::seconds timeout)
{
    // 暂时不实现
    return std::nullopt;
}

bool Storage::ltrim(const std::string& key, int start, int stop)
{
    std::lock_guard<std::mutex> lock(_mutex);

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
    std::lock_guard<std::mutex> lock(_mutex);
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
    std::lock_guard<std::mutex> lock(_mutex);

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
            std::lock_guard<std::mutex> lock(_mutex);
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