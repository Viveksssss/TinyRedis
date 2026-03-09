#include "session.hpp"
#include "command_registry.hpp"
#include "resp_parse.hpp"
#include "server.hpp"
#include "storage.hpp"

#include <boost/asio/io_context.hpp>
#include <cctype>
#include <cstddef>
#include <iostream>
#include <string>

#include <boost/system/detail/error_code.hpp>
#include <spdlog/spdlog.h>

namespace Redis {

Session::Session(boost::asio::io_context& context, boost::asio::ip::tcp::socket socket, std::shared_ptr<Server> server)
    : _socket(std::move(socket))
    , io_context(context)
    , _server(server)
{
    _transaction_queue.reserve(5);
}

Session::~Session()
{
    if (_is_replica && _server) {
        _server->remove_replica(shared_from_this());
    } else {
        _server->decrement_client();
    }
    // std::cout << "Client disconnected" << std::endl;
}

void Session::start()
{
    // std::cout << "Client connected" << std::endl;
    do_read();
}

void Session::do_write_raw(const std::string& content)
{
    do_write(content);
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
                        process_command_co(*cmd_array);
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
            // std::cout << "Client closed connection" << std::endl;
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

bool Session::replica_process(std::string_view cmd, const std::vector<std::string>& command)
{

    if (_handshake_state == HandshakeState::None && cmd == "PING") {
        _handshake_state = HandshakeState::PingReceived;
        do_write(RESPEncoder::encode_simple_string("PONG"));
        return true;
    } else if (_handshake_state == HandshakeState::PingReceived && cmd == "REPLCONF") {
        if (command.size() >= 3 && command[1] == "listening-port") {
            _handshake_state = HandshakeState::RepliconfPortReceived;
            SPDLOG_INFO("Received REPLCONF listening-port from replica {}", get_peer_info());

            do_write(RESPEncoder::encode_simple_string("OK"));
            return true;
        }
    } else if (_handshake_state == HandshakeState::RepliconfPortReceived && cmd == "REPLCONF") {
        if (command.size() >= 3 && command[1] == "capa") {
            _handshake_state = HandshakeState::RepliconfCapaReceived;
            SPDLOG_INFO("Received REPLCONF capa from replica {}", get_peer_info());

            do_write(RESPEncoder::encode_simple_string("OK"));
            return true;
        }
    } else if (_handshake_state == HandshakeState::RepliconfCapaReceived && cmd == "PSYNC") {
        if (command.size() != 3) {
            do_write(RESPEncoder::encode_error("wrong number of arguments for 'psync' command"));
            return true;
        }

        if (command[1] == "?" && command[2] == "-1") {
            // 获取 master 的 replid 和 offset
            auto server = _server; // 需要在 Session 中保存 Server 指针

            // 发送 FULLRESYNC 响应
            std::string fullresync = "+FULLRESYNC " + server->replid() + " " + server->repl_offset() + "\r\n";
            do_write(fullresync);

            // 发送 RDB 文件
            server->send_rdb_file(shared_from_this());

            _is_replica = true;
            _handshake_state = HandshakeState::Complete;

            // 注册到 server
            _server->add_replica(shared_from_this());
            _server->decrement_client(); // 因为这个连接不算普通客户端连接数

            return true;
        }
    }
    return false;
}

// 获取连接信息
std::string Session::remote_endpoint() const
{
    try {
        return _socket.remote_endpoint().address().to_string() + ":" + std::to_string(_socket.remote_endpoint().port());
    } catch (const std::exception& e) {
        return "unknown";
    }
}
// 判断是否是 replica 连接
// 发送命令到 replica（用于 master 向 replica 传播）
void Session::send_to_replica(const std::string& command)
{
    bool write_in_progress = !_write_queue.empty();
    _write_queue.push(command);
    if (!write_in_progress) {
        do_write_replica();
    }
}
// 获取连接信息用于日志
std::string Session::get_peer_info() const
{
    return remote_endpoint() + (is_replica() ? " (replica)" : " (client)");
}

void Session::do_write_replica()
{
    if (_write_queue.empty()) {
        _writing = false;
        return;
    }

    _writing = true;
    auto self = shared_from_this();
    const auto& command = _write_queue.front();

    boost::asio::async_write(_socket, boost::asio::buffer(command), [this, self, command](boost::system::error_code ec, std::size_t bytes_transferred) {
        if (ec) {
            handle_error(ec, "write to replica");
            return;
        }

        _write_queue.pop();
        SPDLOG_INFO("Sent command to replica {}: {}", get_peer_info(), command);

        do_write_replica();
    });
}

Task<void> Session::process_command_co(const std::vector<std::string>& command)
{
    if (command.empty()) {
        do_write(RESPEncoder::encode_error("empty command"));
        co_return;
    }

    std::string cmd = command[0];
    for (auto& c : cmd) {
        c = std::toupper(c);
    }

    if (!_is_replica && _server && !_server->is_replication()) {
        bool handled = replica_process(cmd, command);
        if (handled) {
            co_return;
        }
    }

    if (_is_replica) {
        co_await execute_command_no_response(command);
        co_return;
    }

    if (auto it = Config::CommandRegistry::pre_check(command)) {
        do_write(*it);
        co_return;
    }
    /* 增加服务器收到的命令数 */
    _server->incrment_command();

    if (cmd == "MULTI") {
        _in_transaction = true;
        _transaction_queue.clear();
        do_write(RESPEncoder::encode_simple_string("OK"));
        co_return;
    } else if (cmd == "EXEC") {
        if (!_in_transaction) {
            do_write(RESPEncoder::encode_error("EXEC without MULTI"));
            co_return;
        }

        std::string response = co_await queued_commands(_transaction_queue);

        _in_transaction = false;
        _transaction_queue.clear();

        do_write(response);
        co_return;
    } else if (cmd == "DISCARD") {
        if (!_in_transaction) {
            do_write(RESPEncoder::encode_error("DISCARD without MULTI"));
            co_return;
        }

        _in_transaction = false;
        _transaction_queue.clear();
        // DISCARD 也会清除 WATCH
        _watched_keys.clear();
        _watched_versions.clear();
        do_write(RESPEncoder::encode_simple_string("OK"));
        co_return;
    } else if (cmd == "WATCH") {
        if (command.size() < 2) {
            do_write(RESPEncoder::encode_error("wrong number of arguments for 'watch' command"));
            co_return;
        }

        if (_in_transaction) {
            do_write(RESPEncoder::encode_error("WATCH inside MULTI is not allowed"));
            co_return;
        }

        auto& storage = Storage::instance();

        _watched_keys.clear();
        _watched_versions.clear();

        // 监视所有指定的键
        for (size_t i = 1; i < command.size(); ++i) {
            const auto& key = command[i];
            _watched_keys.insert(key);

            auto version = storage.get_version(key);
            if (version) {
                _watched_versions[key] = *version;
            } else {
                // 键不存在，版本号视为 0
                _watched_versions[key] = 0;
            }
        }
        do_write(RESPEncoder::encode_simple_string("OK"));
        co_return;
    }
    if (_in_transaction) {
        _transaction_queue.push_back(command);
        do_write(RESPEncoder::encode_simple_string("QUEUED"));
        co_return;
    }

    auto& registry = Config::CommandRegistry::instance();
    /* 结果 */
    std::string response;
    if (registry.is_async(cmd)) {
        /* 异步命令 */
        response = co_await registry.execute_async(cmd, command);
    } else {
        /* 同步命令 */
        response = registry.execute(cmd, command);
    }

    // 如果是写命令且当前是 master，传播给所有 replica
    if (_server && !_server->is_replication() && _server->is_write_command(cmd)) {
        // 构建 RESP 命令
        std::string resp_command = "*" + std::to_string(command.size()) + "\r\n";
        for (const auto& arg : command) {
            resp_command += "$" + std::to_string(arg.size()) + "\r\n";
            resp_command += arg + "\r\n";
        }

        _server->propagate_command(command, resp_command);
    }

    /* 写回 */
    do_write(response);
    co_return;
}

Task<void> Session::execute_command_no_response(const std::vector<std::string>& command)
{
    std::string cmd = command[0];
    for (auto& c : cmd)
        c = toupper(c);

    auto& registry = Config::CommandRegistry::instance();

    try {
        if (registry.is_async(cmd)) {
            // 异步命令需要等待，但不返回响应
            co_await registry.execute_async(cmd, command);
        } else {
            registry.execute(cmd, command); // 执行但不使用返回值
        }
    } catch (const std::exception& e) {
        std::cerr << "Error executing command on replica: " << e.what() << std::endl;
    }
}

Task<std::string> Session::queued_commands(const std::vector<std::vector<std::string>>& commands)
{

    auto& storage = Storage::instance();

    bool watched_keys_modified = false;
    for (const auto& key : _watched_keys) {
        auto current_version = storage.get_version(key);
        auto watched_version = _watched_versions[key];

        if (current_version.value_or(0) != watched_version) {
            watched_keys_modified = true;
            break;
        }
    }

    if (watched_keys_modified) {
        _in_transaction = false;
        _transaction_queue.clear();
        _watched_keys.clear();
        _watched_versions.clear();

        co_return "*0\r\n"; // 返回空数组表示事务失败
    }

    if (_transaction_queue.empty()) {
        _in_transaction = false;
        _watched_keys.clear();
        _watched_versions.clear();
        co_return "*0\r\n";
    }

    std::string response;
    response.reserve(64);
    response = "*" + std::to_string(_transaction_queue.size()) + "\r\n";
    auto& registry = Config::CommandRegistry::instance();
    for (const auto& queued_cmd : _transaction_queue) {
        std::string cmd_name = queued_cmd[0];
        for (auto& c : cmd_name) {
            c = toupper(c);
        }

        if (registry.is_async(cmd_name)) {
            // 事务中不应该有阻塞命令，但为了安全，我们等待结果
            response += co_await registry.execute_async(cmd_name, queued_cmd);
        } else {
            response += registry.execute(cmd_name, queued_cmd);
        }
    }
    co_return response;
}

void Session::handle_error(const boost::system::error_code& ec, const std::string& operation)
{
    SPDLOG_ERROR("Error during {} : {}", operation, ec.message());
    if (_socket.is_open()) {
        boost::system::error_code ignored_ec;
        [[maybe_unused]] auto ec = _socket.close(ignored_ec);
    }
}
}