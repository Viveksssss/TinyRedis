#include "rdb_parser.hpp"
#include "storage.hpp"

#include <chrono>
#include <cstring>

#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>
#include <string>

namespace fs = std::filesystem;

namespace Redis {

RDBParser::RDBParser(const std::string& dir, const std::string& dbfilename)
    : _dir(dir)
    , _dbfilename(dbfilename)
{
    std::string fullpath = _dir + "/" + _dbfilename;

    if (!fs::exists(fullpath)) {
        _error = "Failed to open RDB file: " + fullpath;
        fs::create_directories(_dir);
        {
            std::ofstream(fullpath, std::ios::binary);
        }
    }

    _file = std::make_unique<std::ifstream>(fullpath, std::ios::binary);
}

RDBParser::RDBParser(const std::string& rdb)
    : _rdb(rdb)
    , _is_memory_mode(true)
{
    _file = std::make_unique<std::istringstream>(_rdb); // 将字符串转换为输入流
}

bool RDBParser::parse()
{
    if (!_file || !_file->good()) {
        _error = "input not ready";
        return false;
    }
    if (!parse_header()) {
        return false;
    }
    if (!parse_metadata()) {
        return false;
    }
    while (!_file->eof()) {
        uint8_t opcode = peek_byte();

        if (_file->eof()) {
            break;
        }

        if (opcode == 0xFE) { // database 开始
            if (!parse_database()) {
                return false;
            }
        } else if (opcode == 0xFF) { // EOF 标记
            read_byte(); // 消耗 0xFF

            // 读取校验和（8字节）
            uint64_t checksum = read_uint64();
            if (checksum != 0) {
                SPDLOG_WARN("RDB file has non-zero checksum: {:016x}, but ignoring", checksum);
            }

            break;
        } else {
            // 未知 opcode
            _error = "Unknown opcode: " + std::to_string(opcode);
            return false;
        }
    }

    return true;
}

/* 读取工具函数 */
uint8_t RDBParser::read_byte()
{
    char c;
    _file->read(&c, 1);
    _pos++;
    return static_cast<uint8_t>(c);

    // if (_is_memory_mode) {
    //     if (_pos >= _rdb.size()) {
    //         throw std::runtime_error("unexpected end of rdb string");
    //     }
    //     return static_cast<uint8_t>(_rdb[_pos++]);
    // } else {
    //     uint8_t byte;
    //     _file->read(reinterpret_cast<char*>(&byte), 1);
    //     return byte;
    // }
}
std::size_t RDBParser::read_bytes(uint8_t* buffer, std::size_t len)
{
    _file->read(reinterpret_cast<char*>(buffer), len);
    _pos += len;
    return len;
}
std::size_t RDBParser::read_length(uint32_t& len)
{
    uint8_t first_byte = read_byte();
    uint8_t type = (first_byte & 0xC0) >> 6; // 取前两位

    if (type == 0) { // 0b00: 6 位长度 1
        len = first_byte & 0x3F;
        return 1;
    } else if (type == 1) { // 0b01: 14 位长度 2
        uint8_t second_byte = read_byte();
        len = ((first_byte & 0x3F) << 8) | second_byte;
        return 2;
    } else if (type == 2) { // 0b10: 32 位长度 4
        uint8_t bytes[4];
        read_bytes(bytes, 4);
        len = (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
        return 5;
    } else { // 0b11: 特殊编码
        len = first_byte & 0x3F;
        return 1;
    }
}

std::string RDBParser::read_string()
{
    // 普通字符串
    uint32_t len;
    read_length(len);
    std::string str;
    str.resize(len);
    _file->read(&str[0], len);
    _pos += len;
    return str;
}

ListValue RDBParser::read_list()
{
    ListValue list;
    uint32_t list_len;
    read_length(list_len);

    for (uint32_t i = 0; i < list_len; i++) {
        std::string element = read_string();
        list.push_back(element);
    }
    return list;
}

SortedSet RDBParser::read_sorted_set()
{
    SortedSet sorted_set;
    uint32_t set_len;
    read_length(set_len);
    for (uint32_t i = 0; i < set_len; i++) {
        std::string element = read_string();
        double score = std::stof(read_string());
        sorted_set.add(element, score);
    }
    return sorted_set;
}

HashValue RDBParser::read_hash()
{
    HashValue hash;
    uint32_t field_count;
    read_length(field_count);
    for (uint32_t i = 0; i < field_count; i++) {
        std::string field = read_string();
        std::string value = read_string();
        hash[field] = value;
    }
    return hash;
}

uint64_t RDBParser::read_uint64()
{
    uint8_t bytes[8];
    read_bytes(bytes, 8);
    return static_cast<uint64_t>(bytes[0]) | (static_cast<uint64_t>(bytes[1]) << 8) | (static_cast<uint64_t>(bytes[2]) << 16) | (static_cast<uint64_t>(bytes[3]) << 24) | (static_cast<uint64_t>(bytes[4]) << 32) | (static_cast<uint64_t>(bytes[5]) << 40) | (static_cast<uint64_t>(bytes[6]) << 48) | (static_cast<uint64_t>(bytes[7]) << 56);

    // uint64_t val = 0;
    // if (_is_memory_mode) {
    //     if (_pos + 8 > _rdb.size()) {
    //         throw std::runtime_error("not enough data for uint64");
    //     }
    //     // RDB 是小端序
    //     for (int i = 0; i < 8; i++) {
    //         val |= static_cast<uint64_t>(static_cast<uint8_t>(_rdb[_pos++])) << (8 * i);
    //     }
    // } else {
    //     _file->read(reinterpret_cast<char*>(&val), 8);
    //     // 如果是大端机器需要字节序转换，但通常 x86 是小端
    // }
    // return val;
}
uint32_t RDBParser::read_uint32()
{
    uint8_t bytes[4];
    read_bytes(bytes, 4);
    // little-endian
    return bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);
}

uint8_t RDBParser::peek_byte()
{
    char c = _file->peek();
    if (c == EOF) {
        return 0xFF;
    }
    return static_cast<uint8_t>(c);

    // if (_is_memory_mode) {
    //     if (_pos >= _rdb.size()) {
    //         return 0; // 或抛出异常
    //     }
    //     return static_cast<uint8_t>(_rdb[_pos]);
    // } else {
    //     uint8_t byte = read_byte();
    //     _file->unget(); // 回退
    //     return byte;
    // }
}

/* 解析各部分 */
bool RDBParser::parse_header()
{
    char header[9];
    _file->read(header, 9);

    if (_file->gcount() != 9) {
        _error = "Failed to read RDB header: unexpected EOF";
        return false;
    }

    _pos += 9;

    if (std::string(header, 5) != "REDIS") {
        _error = "Invalid RDB header: missing REDIS magic string";
        return false;
    }

    std::string version(header + 5, 4);
    if (version != "0011") {
        _error = "Unsupported RDB version: " + version;
        return false;
    }

    _version = version;
    return true;
}
bool RDBParser::parse_metadata()
{
    while (true) {
        uint8_t opcode = peek_byte();
        if (opcode == 0xFA) {
            read_byte(); // 消耗0xFA
            std::string key = read_string();
            std::string value = read_string();
            _metadatas[key] = value;
        } else if (opcode == 0xFF || opcode == 0xFE) {
            break;
        } else {
            _error = "Unexcepted byte in database section: " + std::to_string(opcode);
            return false;
        }
    }
    return true;
}
bool RDBParser::parse_database()
{
    while (true) {
        uint8_t opcode = peek_byte();
        if (opcode == 0xFE) {
            read_byte(); // 消耗 0xFE

            uint8_t db_idx;
            db_idx = read_byte();

            uint8_t next = read_byte();
            if (next != 0xFB) {
                _error = "Expected 0xFB for hash table sizes";
                return false;
            }

            uint32_t key_count;
            read_length(key_count);

            if (!parse_key_value_pair(db_idx, key_count)) {
                return false;
            }
        } else if (opcode == 0xFB) {
            read_byte(); // 消耗 0xFB

            break;
        } else if (opcode == 0xFF) {
            break;
        } else {
            _error = "Unexcepted byte in database section: " + std::to_string(opcode);
            return false;
        }
    }
    return true;
}
bool RDBParser::parse_key_value_pair(uint32_t db_number, uint32_t num_keys)
{

    for (uint32_t i = 0; i < num_keys; ++i) {
        uint64_t expiry_ms = 0;

        // 总是读取过期时间（毫秒级）
        uint8_t opcode = read_byte();
        if (opcode == 0xFD) { // 秒级过期
            uint32_t seconds = read_uint32(); // 读取4字节秒数
            expiry_ms = static_cast<uint64_t>(seconds) * 1000; // 转为毫秒
        } else if (opcode == 0xFC) { // 毫秒级过期
            expiry_ms = read_uint64(); // 读取8字节毫秒数
        } else {
            // 没有预期到其他opcode，应该总是有过期时间字段
            _error = "Expected expiry time opcode (0xFD or 0xFC), got: " + std::to_string(opcode);
            return false;
        }

        uint8_t value_type = read_byte();
        std::string key = read_string();

        // 先读取值
        ValueWithExpiry value;
        switch (value_type) {
        case 0: // String
            value = read_string();
            break;
        case 1: // List
            value = read_list();
            break;
        case 2: // Set
            value = read_sorted_set();
            break;
        case 3:
            value = read_hash();
            break;
        default:
            return false;
        }

        // 然后设置过期时间信息
        if (expiry_ms != 0) {
            value.has_expiry = true;
            value.expiry = std::chrono::steady_clock::now() + std::chrono::milliseconds(expiry_ms);
            SPDLOG_INFO("Key: {}, Expiry: {}", key, expiry_ms);
        } else {
            value.has_expiry = false;
        }

        _keys[key] = value;
    }
    return true;
}

} // namesapce