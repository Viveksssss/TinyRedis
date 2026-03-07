#pragma once

#include "task.hpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/system/detail/error_code.hpp>

#include <array>
#include <memory>
#include <unordered_set>

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
    Task<void> process_command_co(const std::vector<std::string>& command);
    Task<std::string> queued_commands(const std::vector<std::vector<std::string>>& commands);
    void handle_error(const boost::system::error_code& ec, const std::string& operation);

    boost::asio::ip::tcp::socket _socket;
    std::array<char, 1024> _data;
    std::string _read_buffer;
    boost::asio::io_context& io_context;

    /* 支持事物 */
    bool _in_transaction { false };
    std::vector<std::vector<std::string>> _transaction_queue;
    // WATCH 相关
    std::unordered_set<std::string> _watched_keys; // 当前会话监视的键
    std::unordered_map<std::string, uint64_t> _watched_versions; // 监视时的版本号
};

}
