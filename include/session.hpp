#pragma once

#include "task.hpp"

#include <array>
#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/system/detail/error_code.hpp>
#include <memory>

namespace Redis {

class Session : public std::enable_shared_from_this<Session> {
public:
    explicit Session(boost::asio::io_context& context, boost::asio::ip::tcp::socket socket);
    ~Session();

    void start();

private:
    void do_read();
    void do_write(const std::string& response);
    // std::string process_command(const std::vector<std::string>& command);
    Task<std::monostate> process_command_co(const std::vector<std::string>& command);
    void handle_error(const boost::system::error_code& ec, const std::string& operation);

    boost::asio::ip::tcp::socket _socket;
    std::array<char, 1024> _data;
    std::string _read_buffer;
    boost::asio::io_context& io_context;
};

}
