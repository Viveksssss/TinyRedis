#include <iostream>

#include <exception>
#include <memory>

#include <boost/asio.hpp>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>

#include "config_utils.hpp"
#include "server.hpp"

std::shared_ptr<Redis::Server> server(nullptr);
boost::asio::io_context io_context;

extern void set_global_server(std::shared_ptr<Redis::Server> server);

std::shared_ptr<Redis::Server> make_server(boost::asio::io_context& io, Redis::Config::Config config);

void signal_handler(int signum)
{
    std::cout << "Interrupt signal (" << signum << ") received." << std::endl;
    server->stop();
    SPDLOG_INFO("Server stopped~~~~~~~");
    io_context.stop();

    exit(signum);
}

int main(int argc, char** argv)
{

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%s:%#] %v");
    spdlog::set_level(spdlog::level::debug);

    try {
        auto config = Redis::Config::from_args(argc, argv);

        ::server = make_server(io_context, config);

        boost::asio::signal_set signals(io_context, SIGINT, SIGTERM);

        signals.async_wait([&](auto, auto) {
            std::cout << "Shutdown signal received" << std::endl;
            if (server) {
                server->stop();
                io_context.stop();
            }
        });

        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

std::shared_ptr<Redis::Server> make_server(boost::asio::io_context& io, Redis::Config::Config config)
{
    std::shared_ptr<Redis::Server> srv; // 用局部变量
    if (config.is_replication) {
        srv = std::make_shared<Redis::Server>(io, config.bind, config.port, config.master_host, config.master_port);
    } else {
        srv = std::make_shared<Redis::Server>(io, config.bind, config.port, config.dir, config.db_filename);
    }
    set_global_server(srv);
    if (config.is_replication) {
        srv->start_replication();
    }
    return srv; // 返回给 main
}