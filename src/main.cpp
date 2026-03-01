#include <iostream>

#include <boost/asio.hpp>
#include <exception>

#include "server.hpp"

int main(int argc, char** argv)
{
    try {
        boost::asio::io_context io_context;

        Redis::Server server(io_context, 6379);

        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
