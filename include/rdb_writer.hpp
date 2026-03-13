// rdb_writer.hpp
#pragma once

#include "storage.hpp"
#include <fstream>
#include <string>
#include <unordered_map>

namespace Redis {

class RDBWriter {
public:
    RDBWriter(const std::string& dir, const std::string& dbfilename);
    RDBWriter() = default;

    // 将 Storage 中的数据保存到 RDB 文件
    bool save(const std::unordered_map<std::string, ValueWithExpiry>& data);

    // 错误信息
    std::string error() const { return _error; }

private:
    // 写入工具函数
    void write_byte(uint8_t byte);
    void write_bytes(const uint8_t* data, size_t len);
    void write_bytes_be(uint32_t val);
    void write_length(size_t len);
    void write_string(const std::string& str);
    void write_list(const ListValue& list);

    // 写入各部分
    void write_header();
    void write_metadata();
    void write_database(const std::unordered_map<std::string, ValueWithExpiry>& data);
    void write_checksum();

    std::string _dir;
    std::string _dbfilename;
    std::fstream _file;
    std::string _error;
    uint64_t _checksum { 0 };
};

} // namespace Redis