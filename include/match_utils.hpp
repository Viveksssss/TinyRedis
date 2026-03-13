#pragma once

#include <string>

namespace Redis::Utils::MatchUtils {

bool match_pattern(const std::string& str, const std::string& pattern);

}