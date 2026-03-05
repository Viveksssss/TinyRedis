#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace Redis::Utils::StreamUtils {

inline constexpr char SEPARATOR = '-';
inline constexpr const char* AUTO_SUFFIX = "-*";

// ID验证
bool is_valid_id(const std::string& id);
// ID比较
bool is_id_greater(const std::string& id1, const std::string& id2);
// 解析ID成毫秒和序列号,但是不能解析部分id,->parse_range_id
bool parse_id(const std::string& id, uint64_t& ms, uint64_t& seq);
// 生成ID
std::string make_id(uint64_t ms, uint64_t seq);
// 从规范生成 ID（处理 * 和 <ms>-*）
std::string make_id_from_spec(const std::string& spec,
    const std::optional<std::string>& last_id = std::nullopt);
// 获取当前毫秒时间戳
uint64_t now_ms();
// 判断是否是自动生成规范（* 或 <ms>-*）
bool is_auto_spec(const std::string& spec);
// 从规范中提取毫秒部分
std::optional<uint64_t> extract_ms(const std::string& spec);
// 解析带默认值的 ID（处理序列号可选的情况）
bool parse_range_id(const std::string& spec, uint64_t& ms, uint64_t& seq, bool is_start);
// 比较两个 ID（考虑默认值）
bool is_id_in_range(uint64_t ms, uint64_t seq,
    uint64_t start_ms, uint64_t start_seq,
    uint64_t end_ms, uint64_t end_seq);

}