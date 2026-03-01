#pragma once

#include <array>
#include <boost/asio.hpp>
#include <boost/system/detail/error_code.hpp>
#include <memory>

namespace Redis {

class Session : public std::enable_shared_from_this<Session> {
public:
    explicit Session(boost::asio::ip::tcp::socket socket);
    ~Session();

    void start();

private:
    void do_read();
    void do_write(const std::string& response);
    void process_command(const std::vector<std::string>& command);
    void handle_error(const boost::system::error_code& ec, const std::string& operation);

    boost::asio::ip::tcp::socket _socket;
    std::array<char, 1024> _data;
    std::string _read_buffer;
};

}
