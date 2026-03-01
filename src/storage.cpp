#include "storage.hpp"

#include <chrono>
#include <mutex>
#include <random>
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

void Storage::set(const std::string& key, const std::string& value)
{
    std::lock_guard<std::mutex> lock(_mutex);
    _store[key] = ValueWithExpiry(value);
}

void Storage::set_with_expiry_ms(const std::string& key, const std::string& value, std::chrono::milliseconds ttl)
{
    std::lock_guard<std::mutex> lock(_mutex);
    _store[key] = ValueWithExpiry(value, ttl);
}

void Storage::check_expiry(const std::string& key)
{
    auto it = _store.find(key);
    if (it != _store.end() && it->second.is_expired()) {
        _store.erase(it);
    }
}

std::optional<std::string> Storage::get(const std::string& key)
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

    return it->second.value;
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