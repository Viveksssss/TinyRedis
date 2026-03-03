#include "server.hpp"

#include <exception>
#include <iostream>
#include <memory>

#include <boost/asio/io_context.hpp>

#include "command_handlers.hpp"
#include "command_registry.hpp"
#include "session.hpp"

namespace Redis {

using boost::asio::ip::tcp;

Server::Server(boost::asio::io_context& io_context, short port)
    : _acceptor(io_context, tcp::endpoint(tcp::v4(), port))
    , io_context(io_context)
{
    try {
        do_registry();
    } catch (std::exception& e) {
        throw e;
    }
    do_accept();
}

void Server::do_registry()
{
    try {

        auto& registry = Config::CommandRegistry::instance();
        if (!registry.load_from_file("config/commands.json")) {
            std::cerr << "Warning: Failed to load command config, using defaults" << std::endl;
        }
        Config::CommandHandlers::register_all();
    } catch (std::exception& e) {
        throw e;
    }
}

void Server::do_accept()
{
    _acceptor.async_accept([this](boost::system::error_code ec, tcp::socket socket) {
        if (!ec) {
            std::make_shared<Redis::Session>(io_context, std::move(socket))->start();
        }
        do_accept();
    });
}

}