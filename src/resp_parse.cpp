#include "resp_parse.hpp"

#include <memory>
#include <string>

namespace Redis {

RESPParser::RESPParser(const std::string& data)
    : _data(data)
    , _pos(0)
{
}

RESPType RESPParser::peek_type() const
{
    if (_pos >= _data.size()) {
        return RESPType::Unknown;
    }

    char c = _data[_pos];
    switch (c) {
    case '+':
        return RESPType::SimpleString;
    case '-':
        return RESPType::Error;
    case ':':
        return RESPType::Integer;
    case '$':
        return RESPType::BulkString;
    case '*':
        return RESPType::Array;
    default:
        return RESPType::Unknown;
    }
}

std::string RESPParser::read_line()
{
    std::size_t end = _data.find("\r\n", _pos);
    if (end == std::string::npos) {
        return "";
    }
    std::string line = _data.substr(_pos, end - _pos);
    _pos = end + 2;
    return line;
}

bool RESPParser::expect(const std::string& str)
{
    if (_pos + str.size() > _data.size()) {
        return false;
    }

    if (_data.substr(_pos, str.size()) == str) {
        _pos += str.size();
        return true;
    }
    return false;
}

bool RESPParser::has_more() const
{
    return _pos < _data.size();
}

std::shared_ptr<RESPValue> RESPParser::next()
{
    if (!has_more()) {
        return nullptr;
    }

    RESPType type = peek_type();
    switch (type) {
    case RESPType::SimpleString:
        return parse_simple_string();
    case RESPType::Error:
        return parse_error();
    case RESPType::Integer:
        return parse_integer();
    case RESPType::BulkString:
        return parse_bulk_string();
    case RESPType::Array:
        return parse_array();
    case RESPType::Null:
        return parse_null();
    default:
        return nullptr;
    };
}

std::shared_ptr<RESPValue> RESPParser::parse_simple_string()
{
    if (!expect("+")) {
        return nullptr;
    }

    std::string line = read_line();
    if (line.empty()) {
        return nullptr;
    }
    return std::make_shared<RESPValue>(line);
}

std::shared_ptr<RESPValue> RESPParser::parse_error()
{
    if (!expect("-")) {
        return nullptr;
    }
    std::string line = read_line();
    if (line.empty()) {
        return nullptr;
    }
    return std::make_shared<RESPValue>(line);
}

std::shared_ptr<RESPValue> RESPParser::parse_integer()
{
    if (!expect(":")) {
        return nullptr;
    }
    std::string line = read_line();
    if (line.empty()) {
        return nullptr;
    }
    try {
        int64_t num = std::stoll(line);
        return std::make_shared<RESPValue>(num);
    } catch (...) {
        return nullptr;
    }
}

std::shared_ptr<RESPValue> RESPParser::parse_bulk_string()
{
    if (!expect("$")) {
        return nullptr;
    }
    std::string line = read_line();
    if (line.empty()) {
        return nullptr;
    }
    int length = std::stoll(line);
    if (length == -1) {
        return std::make_shared<RESPValue>(nullptr);
    }

    if (_pos + length + 2 > _data.size()) {
        return nullptr;
    }

    std::string str = _data.substr(_pos, length);
    _pos += length;
    if (!expect("\r\n")) {
        return nullptr;
    }
    return std::make_shared<RESPValue>(str);
}

std::shared_ptr<RESPValue> RESPParser::parse_array()
{
    if (!expect("*")) {
        return nullptr;
    }

    std::string line = read_line();
    if (line.empty()) {
        return nullptr;
    }
    int length = std::stoi(line);
    if (length == -1) {
        return std::make_shared<RESPValue>(nullptr);
    }

    std::vector<std::shared_ptr<RESPValue>> items;
    items.reserve(length);
    for (std::size_t i = 0; i < length; ++i) {
        auto item = next();
        if (!item) {
            return nullptr;
        }
        items.push_back(item);
    }
    return std::make_shared<RESPValue>(items);
}

std::shared_ptr<RESPValue> RESPParser::parse_null()
{
    // 检查 Null Bulk String ($-1\r\n)
    if (_pos + 4 <= _data.size() && _data.substr(_pos, 4) == "$-1\r\n") {
        _pos += 4;
        return std::make_shared<RESPValue>(nullptr);
    }

    // 检查 Null Array (*-1\r\n)
    if (_pos + 4 <= _data.size() && _data.substr(_pos, 4) == "*-1\r\n") {
        _pos += 4;
        return std::make_shared<RESPValue>(nullptr);
    }

    return nullptr;
}

std::string RESPEncoder::encode_simple_string(const std::string& str)
{
    return "+" + str + "\r\n";
}

std::string RESPEncoder::encode_error(const std::string& str)
{
    return "-ERR " + str + "\r\n";
}

std::string RESPEncoder::encode_error_with_prefix(const std::string& prefix, const std::string& err)
{
    return "-" + prefix + " " + err + "\r\n";
}

std::string RESPEncoder::encode_raw_error(const std::string& err)
{
    return "-" + err + "\r\n";
}

std::string RESPEncoder::encode_integer(int64_t num)
{
    return ":" + std::to_string(num) + "\r\n";
}
std::string RESPEncoder::encode_bulk_string(const std::string& str)
{
    return "$" + std::to_string(str.size()) + "\r\n" + str + "\r\n";
}

std::string RESPEncoder::encode_null_bulk_string()
{
    return "$-1\r\n";
}

std::string RESPEncoder::encode_array(const std::vector<std::string>& items)
{
    std::string result = "*" + std::to_string(items.size()) + "\r\n";
    for (const auto& item : items) {
        result += encode_bulk_string(item);
    }
    return result;
}

std::string RESPEncoder::encode_null_array()
{
    return "*-1\r\n";
}

}