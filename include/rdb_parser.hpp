#pragma once

#include "storage.hpp"
#include <cstdint>
#include <string>
#include <unordered_map>

namespace Redis {

class RDBParser {
public:
    RDBParser(const std::string& dir, const std::string& dbfilename);

    RDBParser(const std::string& rdb);

    RDBParser() = default;

    bool parse();

    const std::unordered_map<std::string, ValueWithExpiry>& get_keys() const { return _keys; }

    std::string error() const { return _error; }

private:
    /* 读取工具函数 */
    uint8_t read_byte();
    std::size_t read_bytes(uint8_t* buffer, std::size_t len);
    std::size_t read_length(uint32_t& len);
    std::string read_string();
    ListValue read_list();
    SortedSet read_sorted_set();
    HashValue read_hash();

    uint64_t read_uint64();
    uint32_t read_uint32();
    uint8_t peek_byte();

    /* 解析各部分 */
    bool parse_header(); // 解析头部
    bool parse_metadata(); // 解析元数据
    bool parse_database(); // 解析主体
    bool parse_key_value_pair(uint32_t db_number, uint32_t num_keys); // 解析一个键值对

    std::string _dir;
    std::string _dbfilename;
    std::unique_ptr<std::istream> _file;
    std::string _error;
    std::unordered_map<std::string, ValueWithExpiry> _keys;
    std::unordered_map<std::string, std::string> _metadatas;
    std::string _version;

    std::size_t _pos; // 当前解析位置
    std::string _rdb;
    bool _is_memory_mode = false;
};

} // namesapce