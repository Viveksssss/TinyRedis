#include "match_utils.hpp"

namespace Redis::Utils::MatchUtils {

bool match_pattern(const std::string& str, const std::string& pattern)
{
    std::size_t p_idx = 0, s_idx = 0;
    std::size_t last_str = std::string::npos;
    std::size_t last_s = 0;

    while (s_idx < str.size()) {
        if (p_idx < pattern.size() && pattern[p_idx] == '*') {
            last_str = p_idx;
            last_s = s_idx;
            p_idx++;
        } else if (p_idx < pattern.size() && pattern[p_idx] == '?') {
            p_idx++;
            s_idx++;
        } else if (p_idx < pattern.length() && pattern[p_idx] == '[') {
            size_t back_end = pattern.find(']', p_idx + 1);
            if (back_end == std::string::npos) {
                if (pattern[p_idx] == str[s_idx]) {
                    p_idx++;
                    s_idx++;
                } else {
                    return false;
                }
            } else {
                bool negate = (pattern[p_idx + 1] == '^');
                std::size_t charset_start = p_idx + 1 + (negate ? 1 : 0);
                std::string charset = pattern.substr(charset_start, back_end - charset_start);

                bool matched = false;
                for (char c : charset) {
                    if (c == str[s_idx]) {
                        matched = true;
                        break;
                    }
                }
                if ((matched && !negate) || (!matched && negate)) {
                    p_idx = back_end + 1;
                    s_idx++;
                } else {
                    if (last_str != std::string::npos) {
                        p_idx = last_str + 1;
                        s_idx = ++last_s;
                    } else {
                        return false;
                    }
                }
            }
        } else if (p_idx < pattern.size() && pattern[p_idx] == '\\') {
            if (p_idx + 1 < pattern.size() && pattern[p_idx + 1] == str[s_idx]) {
                p_idx += 2;
                s_idx++;
            } else {
                if (last_str != std::string::npos) {
                    p_idx = last_str + 1;
                    s_idx = ++last_s;
                } else {
                    return false;
                }
            }
        } else {
            if (p_idx < pattern.size() && pattern[p_idx] == str[s_idx]) {
                p_idx++;
                s_idx++;
            } else {
                if (last_str != std::string::npos) {
                    p_idx = last_str + 1;
                    s_idx = ++last_s;
                } else {
                    return false;
                }
            }
        }
    }
    while (p_idx < pattern.size() && pattern[p_idx] == '*') {
        p_idx++;
    }
    return p_idx == pattern.size();
}

} // namespace