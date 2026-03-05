#include "stream_utils.hpp"
#include "command_handlers.hpp"

#include <cctype>
#include <chrono>
#include <cstdint>
#include <exception>
#include <stdexcept>

namespace Redis::Utils::StreamUtils {

bool is_valid_id(const std::string& id)
{
    if (id.empty()) {
        return false;
    }
    std::size_t dash_pos = id.find(SEPARATOR);

    if (dash_pos == std::string::npos || dash_pos == 0)
        return false;

    for (size_t i = 0; i < dash_pos; ++i) {
        if (!isdigit(id[i]))
            return false;
    }

    for (size_t i = dash_pos + 1; i < id.size(); ++i) {
        if (!isdigit(id[i]))
            return false;
    }

    return true;
}

bool parse_id(const std::string& id, uint64_t& ms, uint64_t& seq)
{
    if (!is_valid_id(id))
        return false;

    size_t dash_pos = id.find(SEPARATOR);
    try {
        ms = std::stoull(id.substr(0, dash_pos));
        seq = std::stoull(id.substr(dash_pos + 1));
    } catch (std::exception& e) {
        return false;
    }
    return true;
}

std::string make_id(uint64_t ms, uint64_t seq)
{
    return std::to_string(ms) + SEPARATOR + std::to_string(seq);
}

uint64_t now_ms()
{
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

bool is_id_greater(const std::string& id1, const std::string& id2)
{
    uint64_t ms1, seq1, ms2, seq2;
    if (!parse_id(id1, ms1, seq1) || !parse_id(id2, ms2, seq2)) {
        throw std::invalid_argument("Invalid stream ID format");
    }

    if (ms1 != ms2)
        return ms1 > ms2;
    return seq1 > seq2;
}

bool is_auto_spec(const std::string& spec)
{
    return spec == "*" || (spec.size() > 2 && spec.substr(spec.size() - 2) == AUTO_SUFFIX);
}

std::optional<uint64_t> extract_ms(const std::string& spec)
{
    if (spec == "*") {
        return std::nullopt; // 完全自动生成，使用当前时间
    }

    if (spec.size() > 2 && spec.substr(spec.size() - 2) == AUTO_SUFFIX) {
        std::string ms_part = spec.substr(0, spec.size() - 2);

        // 验证毫秒部分
        for (char c : ms_part) {
            if (!isdigit(c))
                return std::nullopt;
        }

        return std::stoull(ms_part);
    }

    return std::nullopt;
}

std::string make_id_from_spec(const std::string& spec,
    const std::optional<std::string>& last_id)
{
    uint64_t last_ms = 0, last_seq = 0;
    if (last_id) {
        parse_id(*last_id, last_ms, last_seq);
    }

    // 情况 1: "*" - 完全自动生成
    if (spec == "*") {
        uint64_t ms = now_ms();
        uint64_t seq = (ms == last_ms) ? last_seq + 1 : 0;
        return make_id(ms, seq);
    }

    // 情况 2: "<ms>-*" - 指定毫秒部分
    if (spec.size() > 2 && spec.substr(spec.size() - 2) == AUTO_SUFFIX) {
        std::string ms_part = spec.substr(0, spec.size() - 2);
        uint64_t ms = std::stoull(ms_part);

        if (last_id && ms < last_ms) {
            throw std::runtime_error("The ID specified is smaller than the target stream top item");
        }

        uint64_t seq = (ms == last_ms) ? last_seq + 1 : 0;
        return make_id(ms, seq);
    }

    // 情况 3: 完整 ID - 验证并返回
    if (!is_valid_id(spec)) {
        throw std::runtime_error("ERR Invalid stream ID format");
    }

    if (last_id && !is_id_greater(spec, *last_id)) {
        throw std::runtime_error("The ID specified is smaller than the target stream top item");
    }

    return spec;
}

bool parse_range_id(const std::string& spec, uint64_t& ms, uint64_t& seq, bool is_start)
{
    /* 特殊符解析 */
    if (spec == "-") {
        if (!is_start) {
            return false;
        }
        ms = 0;
        seq = 0;
        return true;
    }

    if (spec == "+") {
        if (is_start) {
            return false;
        }
        ms = UINT64_MAX;
        seq = UINT64_MAX;
        return true;
    }

    std::size_t dash_pos = spec.find(SEPARATOR);
    if (dash_pos == std::string::npos) {
        try {
            ms = std::stoll(spec);
            seq = is_start ? 0 : UINT16_MAX;
            return true;
        } catch (...) {
            return false;
        }
    } else {
        return parse_id(spec, ms, seq);
    }
}
bool is_id_in_range(uint64_t ms, uint64_t seq,
    uint64_t start_ms, uint64_t start_seq,
    uint64_t end_ms, uint64_t end_seq)
{
    if (ms < start_ms || ms > end_ms) {
        return false;
    }
    if (ms == start_ms && seq < start_seq) {
        return false;
    }
    if (ms == end_ms && seq > end_seq) {
        return false;
    }
    return true;
}

}