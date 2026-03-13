#include "config_utils.hpp"
#include <boost/asio/ip/address.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace Redis::Config {

Config::Config()
{
    const char* home = std::getenv("HOME");
    if (!home) {
        home = std::getenv("USERPROFILE");
    }
    if (!home) {
        const char* drive = std::getenv("HOMEDRIVE");
        const char* path = std::getenv("HOMEPATH");
        if (drive && path) {
            static std::string combined = std::string(drive) + path;
            home = combined.data();
        }
    }
    if (!home) {
        home = ".";
    }

    this->dir = home;
}

Config from_args(int argc, char** argv)
{
    Config config;

    for (std::size_t i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage();
            std::exit(0);
        } else if (arg == "--port" || arg == "-p") {
            if (++i >= argc) {
                throw std::runtime_error("--port requires an argument");
            }
            int port = std::stoi(argv[i]);
            if (port <= 0 || port > 65535) {
                throw std::runtime_error("Invalid port number: " + std::to_string(port));
            }
            config.port = static_cast<uint16_t>(port);
        } else if (arg == "--bind" || arg == "-b") {
            if (++i >= argc) {
                throw std::runtime_error("--bind requires an argument");
            }
            config.bind = argv[i];
        } else if (arg == "--replicaof") {
            if (++i >= argc) {
                throw std::runtime_error("--replicaof requires an argument");
            }
            std::istringstream ss(argv[i]);
            ss >> config.master_host;
            std::string port;
            ss >> port;
            config.master_port = static_cast<uint16_t>(std::stoi(port));

            if (is_valid_address(config.master_host)) {
                config.is_replication = true;
            }
        } else if (arg == "--dir") {
            if (++i >= argc) {
                throw std::runtime_error("--dir requires an argument");
            }
            config.dir = argv[i];
        } else if (arg == "--dbfilename") {
            if (++i >= argc) {
                throw std::runtime_error("--dbfilename requires an argument");
            }
            config.db_filename = argv[i];
        } else {
            throw std::runtime_error(std::string("Unknown option: ") + std::string(arg));
        }
    }

    return config;
}

bool is_valid_address(const std::string& address)
{

    if (address == "localhost") {
        // 明确指定为 127.0.0.1，避免 IPv6 歧义
        return true;
    }
    if (address == "*" || address == "any") {
        return true;
    }

    try {
        // 尝试解析为 IP 地址
        auto addr = boost::asio::ip::make_address(address);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

void print_usage()
{
    std::cerr << "Options:\n"
              << "  --port <port>     -p   Server port (default: 6379)\n"
              << "  --bind <address>  -b   Bind address (default: 0.0.0.0)\n"
              << "  --help            -h   Show this help message\n";
}

}