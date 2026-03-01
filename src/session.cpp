#include "session.hpp"
#include "command_registry.hpp"
#include "resp_parse.hpp"

#include <cstddef>
#include <iostream>
#include <string>

#include <boost/system/detail/error_code.hpp>
#include <thread>

namespace Redis {

Session::Session(boost::asio::ip::tcp::socket socket)
    : _socket(std::move(socket))
{
}

Session::~Session()
{
    std::cout << "Client disconnected" << std::endl;
}

void Session::start()
{
    std::cout << "Client connected" << std::endl;
    do_read();
}

void Session::do_read()
{
    auto self = shared_from_this();
    auto handler = [this, self](boost::system::error_code ec, std::size_t bytes_transferred) {
        if (!ec) {
            _read_buffer.append(_data.data(), bytes_transferred);

            RESPParser parser(_read_buffer);
            auto value = parser.next();
            if (value) {
                if (value->is_array()) {
                    auto cmd_array = value->as_string_array();
                    if (cmd_array.has_value()) {
                        process_command(*cmd_array);
                    } else {
                        do_write(RESPEncoder::encode_error("invalid command format"));
                    }
                } else {
                    do_write(RESPEncoder::encode_error("expected array"));
                }
                _read_buffer.erase(0, parser.position());

                if (!_read_buffer.empty()) {
                    do_read();
                } else {
                    do_read();
                }
            } else {
                do_read();
            }

        } else if (ec == boost::asio::error::eof) {
            std::cout << "Client closed connection" << std::endl;
        } else {
            handle_error(ec, "read");
        }
    };
    _socket.async_read_some(boost::asio::buffer(_data), handler);
}
void Session::do_write(const std::string& response)
{
    auto self = shared_from_this();

    auto handler = [self, this](boost::system::error_code ec, std::size_t bytes_transferred) {
        if (!ec) {
            do_read();
        } else {
            handle_error(ec, "write");
        }
    };
    boost::asio::async_write(_socket, boost::asio::buffer(response), handler);
}

void Session::process_command(const std::vector<std::string>& command)
{
    if (command.empty()) {
        do_write(RESPEncoder::encode_error("empty command"));
        return;
    }

    std::string cmd = command[0];

    auto& registry = Config::CommandRegistry::instance();
    std::string response = registry.execute(cmd, command);

    do_write(response);
}
void Session::handle_error(const boost::system::error_code& ec, const std::string& operation)
{
    std::cerr << "Error during " << operation << " (thread: " << std::this_thread::get_id() << "): " << ec.message() << std::endl;
    if (_socket.is_open()) {
        boost::system::error_code ignored_ec;
        [[maybe_unused]] auto ec = _socket.close(ignored_ec);
    }
}
}