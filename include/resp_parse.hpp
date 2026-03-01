#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace Redis {

enum class RESPType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Null,
    Unknown
};

class RESPValue;

using RESPValueVariant = std::variant<
    std::string,
    int64_t,
    std::vector<std::shared_ptr<RESPValue>>,
    std::nullptr_t>;

class RESPValue {

public:
    RESPValue() = default;
    RESPValue(const std::string& str)
        : _value(str)
        , _type(RESPType::SimpleString)
    {
    }
    RESPValue(int64_t num)
        : _value(num)
        , _type(RESPType::Integer)
    {
    }
    RESPValue(const std::vector<std::shared_ptr<RESPValue>>& arr)
        : _value(arr)
        , _type(RESPType::Array)
    {
    }
    RESPValue(std::nullptr_t)
        : _value(nullptr)
        , _type(RESPType::Null)
    {
    }

    bool is_string() const { return std::holds_alternative<std::string>(_value); }
    bool is_integer() const { return std::holds_alternative<int64_t>(_value); }
    bool is_array() const { return std::holds_alternative<std::vector<std::shared_ptr<RESPValue>>>(_value); }
    bool is_null() const { return std::holds_alternative<std::nullptr_t>(_value); }

    std::optional<std::string> as_string() const
    {
        if (is_string())
            return std::get<std::string>(_value);
        return std::nullopt;
    }

    std::optional<int64_t> as_integer() const
    {
        if (is_integer())
            return std::get<int64_t>(_value);
        return std::nullopt;
    }

    std::optional<std::vector<std::shared_ptr<RESPValue>>> as_array() const
    {
        if (is_array())
            return std::get<std::vector<std::shared_ptr<RESPValue>>>(_value);
        return std::nullopt;
    }

    // 辅助函数：转换为字符串数组（用于命令解析）
    std::optional<std::vector<std::string>> as_string_array() const
    {
        auto arr = as_array();
        if (!arr)
            return std::nullopt;

        std::vector<std::string> result;
        for (const auto& item : *arr) {
            auto str = item->as_string();
            if (!str)
                return std::nullopt;
            result.push_back(*str);
        }
        return result;
    }

private:
    RESPValueVariant _value;
    RESPType _type;
};

class RESPParser {
public:
    RESPParser(const std::string& data);

    std::shared_ptr<RESPValue> next();

    bool has_more() const;

    std::size_t position() const { return _pos; }

private:
    RESPType peek_type() const;
    std::shared_ptr<RESPValue> parse_simple_string();
    std::shared_ptr<RESPValue> parse_error();
    std::shared_ptr<RESPValue> parse_integer();
    std::shared_ptr<RESPValue> parse_bulk_string();
    std::shared_ptr<RESPValue> parse_array();
    std::shared_ptr<RESPValue> parse_null();

    bool expect(const std::string& str);
    std::string read_line();

    const std::string& _data;
    size_t _pos;
};

class RESPEncoder {
public:
    // 编码函数
    static std::string encode_simple_string(const std::string& str);
    static std::string encode_error(const std::string& err);
    static std::string encode_raw_error(const std::string& err);
    static std::string encode_error_with_prefix(const std::string& prefix, const std::string& err);
    static std::string encode_integer(int64_t num);
    static std::string encode_bulk_string(const std::string& str);
    static std::string encode_null_bulk_string();
    static std::string encode_array(const std::vector<std::string>& items);
    static std::string encode_null_array();

    // 辅助函数：从RESPValue中提取数据
    static std::optional<std::string> as_string(const RESPValue& val);
    static std::optional<int64_t> as_integer(const RESPValue& val);
    static std::optional<std::vector<std::string>> as_string_array(const RESPValue& val);
};

}