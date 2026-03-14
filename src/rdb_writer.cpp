// rdb_writer.cpp
#include "rdb_writer.hpp"
#include "storage.hpp"
#include <chrono>
#include <cstring>
#include <filesystem>
#include <spdlog/spdlog.h>

namespace fs = std::filesystem;

namespace Redis {

RDBWriter::RDBWriter(const std::string& dir, const std::string& dbfilename)
    : _dir(dir)
    , _dbfilename(dbfilename)
{
    std::string fullpath = _dir + "/" + _dbfilename;
    if (!fs::exists(fullpath)) {
        fs::create_directories(_dir); // 递归创建
        {
            std::ofstream(fullpath, std::ios::out | std::ios::binary);
        }
    }
    _file.open(fullpath, std::ios::out | std::ios::binary);
    if (!_file.is_open()) {
        SPDLOG_ERROR("wrong!!!");
    }
}

void RDBWriter::write_byte(uint8_t byte)
{
    _file.write(reinterpret_cast<const char*>(&byte), 1);
}

void RDBWriter::write_bytes(const uint8_t* data, size_t len)
{
    _file.write(reinterpret_cast<const char*>(data), len);
}
void RDBWriter::write_bytes_be(uint32_t val)
{
    write_byte((val >> 24) & 0xFF);
    write_byte((val >> 16) & 0xFF);
    write_byte((val >> 8) & 0xFF);
    write_byte(val & 0xFF);
}

void RDBWriter::write_length(size_t len)
{
    if (len < 64) { // 6位能表示，格式：00xxxxxx
        write_byte(static_cast<uint8_t>(len));
    } else if (len < 16384) { // 14位能表示，格式：01xxxxxx xxxxxxxx
        uint16_t val = static_cast<uint16_t>(len);
        write_byte(static_cast<uint8_t>(((val >> 8) & 0x3F) | 0x40)); // 01xxxxxx
        write_byte(static_cast<uint8_t>(val & 0xFF)); // xxxxxxxx
    } else { // 32位，格式：10xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        write_byte(0x80); // 10 000000
        uint32_t val = static_cast<uint32_t>(len);
        // 转为大端序
        uint8_t bytes[4] = {
            static_cast<uint8_t>((val >> 24) & 0xFF),
            static_cast<uint8_t>((val >> 16) & 0xFF),
            static_cast<uint8_t>((val >> 8) & 0xFF),
            static_cast<uint8_t>(val & 0xFF)
        };
        write_bytes(bytes, 4);
    }
}

void RDBWriter::write_string(const std::string& str)
{
    // 普通字符串
    write_length(str.size());
    write_bytes(reinterpret_cast<const uint8_t*>(str.data()), str.size());
}

void RDBWriter::write_list(const ListValue& list)
{
    write_length(list.size());
    for (const auto& elem : list) {
        write_string(elem);
    }
}

void RDBWriter::write_sorted_set(const SortedSet& set)
{
    auto members = set.range_with_scores(0, -1); // 假设有这个方法来获取所有成员
    write_length(members.size());
    for (const auto& [member, score] : members) {
        write_string(member);
        write_string(std::to_string(score));
    }
}

void RDBWriter::write_hash(const HashValue& hash)
{
    write_length(hash.size());
    for (const auto& [field, value] : hash) {
        write_string(field);
        write_string(value);
    }
}

void RDBWriter::write_header()
{
    // "REDIS0011"
    const char* header = "REDIS0011";
    write_bytes(reinterpret_cast<const uint8_t*>(header), 9);
}

void RDBWriter::write_metadata()
{
    // 写入一些元数据
    // redis-ver
    write_byte(0xFA);
    write_string("redis-ver");
    write_string("6.0.16");

    // redis-bits
    write_byte(0xFA);
    write_string("redis-bits");
    write_string(sizeof(void*) == 8 ? "64" : "32");

    // ctime
    write_byte(0xFA);
    write_string("ctime");
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(
        now.time_since_epoch())
                         .count();
    write_string(std::to_string(timestamp));
}

void RDBWriter::write_database(const std::unordered_map<std::string, ValueWithExpiry>& data)
{
    if (data.empty())
        return;

    // 数据库选择器
    write_byte(0xFE);
    write_length(0); // 数据库索引 0
    // 哈希表大小信息
    write_byte(0xFB);

    size_t valid_keys = data.size();
    write_length(valid_keys);

    // 写入每个键值对
    for (const auto& [key, value] : data) {
        // 写入过期时间（毫秒），总是写入，即使是0
        write_byte(0xFC); // 毫秒级过期时间
        uint64_t expiry_ms = 0;

        if (value.has_expiry) {
            auto expiry_time = std::chrono::duration_cast<std::chrono::milliseconds>(value.expiry - std::chrono::steady_clock::now()).count();
            expiry_ms = expiry_time;
            SPDLOG_INFO("Time:{}ms", expiry_ms);
        }

        // 使用小端序写入8字节
        write_byte(static_cast<uint8_t>(expiry_ms & 0xFF));
        write_byte(static_cast<uint8_t>((expiry_ms >> 8) & 0xFF));
        write_byte(static_cast<uint8_t>((expiry_ms >> 16) & 0xFF));
        write_byte(static_cast<uint8_t>((expiry_ms >> 24) & 0xFF));
        write_byte(static_cast<uint8_t>((expiry_ms >> 32) & 0xFF));
        write_byte(static_cast<uint8_t>((expiry_ms >> 40) & 0xFF));
        write_byte(static_cast<uint8_t>((expiry_ms >> 48) & 0xFF));
        write_byte(static_cast<uint8_t>((expiry_ms >> 56) & 0xFF));

        // 值类型
        if (value.type == ValueType::STRING) {
            write_byte(0); // 字符串类型
            write_string(key);
            write_string(std::get<std::string>(value.value));
        } else if (value.type == ValueType::LIST) {
            write_byte(1); // 列表类型
            write_string(key);
            write_list(std::get<ListValue>(value.value));
        } else if (value.type == ValueType::SORTED_SET) {
            write_byte(2); // 有序集合类型
            write_string(key);
            write_sorted_set(std::get<SortedSet>(value.value));
        } else if (value.type == ValueType::HASH) {
            write_byte(3);
            write_string(key);
            write_hash(std::get<HashValue>(value.value));
        }
    }
}

void RDBWriter::write_checksum()
{
    // 简单实现：写入8字节的0作为占位
    // 实际应该计算CRC64校验和
    uint64_t dummy = 0;
    write_byte(0xFF);
    // 使用小端序写入8字节
    write_byte(static_cast<uint8_t>(dummy & 0xFF));
    write_byte(static_cast<uint8_t>((dummy >> 8) & 0xFF));
    write_byte(static_cast<uint8_t>((dummy >> 16) & 0xFF));
    write_byte(static_cast<uint8_t>((dummy >> 24) & 0xFF));
    write_byte(static_cast<uint8_t>((dummy >> 32) & 0xFF));
    write_byte(static_cast<uint8_t>((dummy >> 40) & 0xFF));
    write_byte(static_cast<uint8_t>((dummy >> 48) & 0xFF));
    write_byte(static_cast<uint8_t>((dummy >> 56) & 0xFF));
}

bool RDBWriter::save(const std::unordered_map<std::string, ValueWithExpiry>& data)
{
    if (!_file.is_open()) {
        SPDLOG_ERROR("no open!");
        return false;
    }

    try {
        write_header();
        write_metadata();
        write_database(data);
        write_checksum();

        _file.close();
        SPDLOG_INFO("RDB file saved successfully, {} keys", data.size());
        return true;
    } catch (const std::exception& e) {
        _error = "Failed to write RDB file: " + std::string(e.what());
        SPDLOG_ERROR(_error);
        return false;
    }
}

} // namespace Redis