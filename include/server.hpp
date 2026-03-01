#pragma once

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>

namespace Redis {

class Server {
public:
    Server(boost::asio::io_context& io_context, short port);

private:
    void do_registry();
    void do_accept();

    boost::asio::ip::tcp::acceptor _acceptor;
};

}