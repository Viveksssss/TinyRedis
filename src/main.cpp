#include <iostream>

#include <exception>
#include <memory>

#include <boost/asio.hpp>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>

#include "config_utils.hpp"
#include "server.hpp"

extern void set_global_server(std::shared_ptr<Redis::Server> server);

std::shared_ptr<Redis::Server> make_server(boost::asio::io_context& io, Redis::Config::Config config);

int main(int argc, char** argv)
{

    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%s:%#] %v");
    spdlog::set_level(spdlog::level::debug);

    try {
        auto config = Redis::Config::from_args(argc, argv);
        boost::asio::io_context io_context;
        auto server = make_server(io_context, config);
        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

std::shared_ptr<Redis::Server> make_server(boost::asio::io_context& io, Redis::Config::Config config)
{
    std::shared_ptr<Redis::Server> server(nullptr);
    if (config.is_replication) {
        server = std::make_shared<Redis::Server>(io, config.bind, config.port, config.master_host, config.master_port);
    } else {
        server = std::make_shared<Redis::Server>(io, config.bind, config.port);
    }
    set_global_server(server);
    return server;
}