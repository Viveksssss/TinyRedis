#pragma once

#include <cstdint>
#include <string>

namespace Redis::Config {

struct Config {
    uint16_t port = 6379;
    std::string bind = "0.0.0.0";

    bool is_replication = false;
    std::string master_host;
    std::uint16_t master_port;

    std::string dir = ".";
    std::string db_filename = "dump.rdb";
    Config();
};

bool is_valid_address(const std::string& address);
Config from_args(int argc, char** argv);
void print_usage();

}