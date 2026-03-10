#include "session.hpp"
#include "command_registry.hpp"
#include "resp_parse.hpp"
#include "server.hpp"
#include "storage.hpp"

#include <boost/asio/io_context.hpp>
#include <cctype>
#include <chrono>
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
    , _replication_timer(context)
{
    _transaction_queue.reserve(5);
}

Session::~Session()
{
    if (_is_replica) {
        _server->remove_replica(shared_from_this());
        _server->decrement_replica_count();
    } else if (!_is_replica || !_is_replication) {
        _server->decrement_client();
    }
}

void Session::start()
{
    // std::cout << "Client connected" << std::endl;
    do_read();
}

void Session::start_replication(const std::string& master_host, std::uint16_t master_port, std::uint16_t listening_port)
{
    _is_replication = true;
    _master_host = master_host;
    _master_port = master_port;
    _listening_port = listening_port;

    SPDLOG_INFO("Replication session starting. Connecting to master {}:{}", _master_host, _master_port);

    // Delay a bit to make sure server is fully up.
    auto self = shared_from_this();
    _replication_timer.expires_after(std::chrono::milliseconds(1000));
    _replication_timer.async_wait([this, self](boost::system::error_code ec) {
        if (!ec) {
            replication_connect();
        }
    });
}

void Session::do_write_raw(const std::string& content)
{
    do_write(content);
}

void Session::do_read()
{
    auto self = shared_from_this();
    auto handler = [this, self](boost::system::error_code ec, std::size_t bytes_transferred) {
        // 先处理错误，再处理正常路径
        if (ec) {
            if (ec == boost::asio::error::eof) {
                // 对端正常关闭连接，静默返回
                return;
            }
            handle_error(ec, "read");
            return;
        }

        // 正常读取数据
        _read_buffer.append(_data.data(), bytes_transferred);

        RESPParser parser(_read_buffer);
        auto value = parser.next();
        auto consumed = parser.position();
        if (!value) {
            // 数据尚不完整，继续读
            do_read();
            return;
        }

        // RDB文件
        if (!value->is_array()) {
            // 只有普通客户端需要错误提示，复制流静默丢弃非数组
            if (!_is_replication) {
                do_write(RESPEncoder::encode_error("expected array"));
            }
            if (consumed > 0) {
                _read_buffer.erase(0, consumed);
            }
            do_read();
            return;
        }

        auto cmd_array = value->as_string_array();
        if (!cmd_array.has_value()) {
            if (!_is_replication) {
                do_write(RESPEncoder::encode_error("invalid command format"));
            }
            if (consumed > 0) {
                _read_buffer.erase(0, consumed);
            }
            do_read();
            return;
        }

        // 复制会话和普通客户端统一走 process_command_co，
        // 在其中根据 _is_replication / _is_replica 决定具体行为
        process_command_co(*cmd_array);

        if (consumed > 0) {
            _read_buffer.erase(0, consumed);
        }
        do_read();
    };
    _socket.async_read_some(boost::asio::buffer(_data), handler);
}

void Session::replication_connect()
{
    using boost::asio::ip::tcp;
    tcp::resolver resolver(io_context);
    auto endpoints = resolver.resolve(_master_host, std::to_string(_master_port));

    auto self = shared_from_this();
    boost::asio::async_connect(_socket, endpoints, [this, self](boost::system::error_code ec, tcp::endpoint) {
        if (!ec) {
            SPDLOG_INFO("Replication session connected to master, starting handshake...");
            _replication_step = ReplicationStep::Ping;
            replication_send_ping();
        } else {
            SPDLOG_ERROR("Replication session failed to connect to master: {}", ec.message());
            _replication_timer.expires_after(std::chrono::seconds(5));
            _replication_timer.async_wait([this, self](boost::system::error_code ec) {
                if (!ec) {
                    replication_connect();
                }
            });
        }
    });
}

std::string Session::replication_step_name() const
{
    switch (_replication_step) {
    case ReplicationStep::None:
        return "None";
    case ReplicationStep::Ping:
        return "PING";
    case ReplicationStep::ReplconfPort:
        return "REPLCONF listening-port";
    case ReplicationStep::ReplconfCapa:
        return "REPLCONF capa";
    case ReplicationStep::Psync:
        return "PSYNC";
    case ReplicationStep::Complete:
        return "Complete";
    default:
        return "Unknown";
    }
}

void Session::replication_send_ping()
{
    SPDLOG_INFO("Replication step [{}]: Sending PING...", replication_step_name());
    std::string ping_cmd = "*1\r\n$4\r\nPING\r\n";
    auto self = shared_from_this();
    boost::asio::async_write(_socket, boost::asio::buffer(ping_cmd), [this, self](boost::system::error_code ec, std::size_t) {
        if (ec) {
            SPDLOG_ERROR("Replication failed to send PING: {}", ec.message());
            return;
        }

        _socket.async_read_some(boost::asio::buffer(_repl_read_buffer), [this, self](boost::system::error_code ec, std::size_t length) {
            if (ec) {
                SPDLOG_ERROR("Replication error reading PONG: {}", ec.message());
                return;
            }

            std::string response(_repl_read_buffer.data(), length);
            if (response.find("+PONG") != std::string::npos) {
                SPDLOG_INFO("Replication PING successful, moving to next step");
                _replication_step = ReplicationStep::ReplconfPort;
                replication_send_replconf_listening_port();
            } else {
                SPDLOG_ERROR("Replication unexpected response to PING, retrying. Received: {}", response);
                _replication_timer.expires_after(std::chrono::seconds(5));
                _replication_timer.async_wait([this, self](boost::system::error_code ec) {
                    if (!ec) {
                        replication_send_ping();
                    }
                });
            }
        });
    });
}

void Session::replication_send_replconf_listening_port()
{
    std::vector<std::string> replconf_vec;
    replconf_vec.reserve(3);
    replconf_vec.emplace_back("REPLCONF");
    replconf_vec.emplace_back("listening-port");
    replconf_vec.push_back(std::to_string(_listening_port));
    std::string replconf_cmd = RESPEncoder::encode_array(replconf_vec);

    SPDLOG_INFO("Replication step [{}]: Sending REPLCONF listening-port {}", replication_step_name(), _listening_port);
    auto self = shared_from_this();
    boost::asio::async_write(_socket, boost::asio::buffer(replconf_cmd), [this, self](boost::system::error_code ec, std::size_t) {
        if (ec) {
            SPDLOG_ERROR("Replication failed to send REPLCONF listening-port: {}", ec.message());
            return;
        }

        _socket.async_read_some(boost::asio::buffer(_repl_read_buffer), [this, self](boost::system::error_code ec, std::size_t length) {
            if (ec) {
                SPDLOG_ERROR("Replication error reading REPLCONF response: {}", ec.message());
                return;
            }

            std::string response(_repl_read_buffer.data(), length);
            if (response.find("+OK") != std::string::npos) {
                SPDLOG_INFO("Replication REPLCONF listening-port OK, moving to next step");
                _replication_step = ReplicationStep::ReplconfCapa;
                replication_send_replconf_capa();
            } else {
                SPDLOG_ERROR("Replication unexpected REPLCONF listening-port response, retrying. Received: {}", response);
                _replication_timer.expires_after(std::chrono::seconds(5));
                _replication_timer.async_wait([this, self](boost::system::error_code ec) {
                    if (!ec) {
                        replication_send_replconf_listening_port();
                    }
                });
            }
        });
    });
}

void Session::replication_send_replconf_capa()
{
    std::string replconf_cmd1 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n";
    std::string replconf_cmd2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";

    SPDLOG_INFO("Replication step [{}]: Sending REPLCONF capa eof & psync2", replication_step_name());
    auto self = shared_from_this();
    boost::asio::async_write(_socket, boost::asio::buffer(replconf_cmd1), [this, self, replconf_cmd2](boost::system::error_code ec, std::size_t) {
        if (ec) {
            SPDLOG_ERROR("Replication failed to send REPLCONF capa eof: {}", ec.message());
            return;
        }

        boost::asio::async_write(_socket, boost::asio::buffer(replconf_cmd2), [this, self](boost::system::error_code ec, std::size_t) {
            if (ec) {
                SPDLOG_ERROR("Replication failed to send REPLCONF capa psync2: {}", ec.message());
                return;
            }
            replication_read_replconf_responses(2);
        });
    });
}

void Session::replication_read_replconf_responses(int remaining)
{
    if (remaining <= 0) {
        SPDLOG_INFO("Replication step [{}]: REPLCONF capa OK, moving to PSYNC", replication_step_name());
        _replication_step = ReplicationStep::Psync;
        replication_send_psync();
        return;
    }

    SPDLOG_INFO("Replication step [{}]: Waiting for REPLCONF response, {} remaining...", replication_step_name(), remaining);
    auto self = shared_from_this();
    boost::asio::async_read(_socket, boost::asio::buffer(_repl_read_buffer, 5),
        [this, self, remaining](boost::system::error_code ec, std::size_t length) {
            if (ec) {
                SPDLOG_ERROR("Replication error reading REPLCONF response: {}", ec.message());
                return;
            }

            std::string response(_repl_read_buffer.data(), length);
            if (response.find("+OK") != std::string::npos) {
                replication_read_replconf_responses(remaining - 1);
            } else {
                SPDLOG_ERROR("Replication unexpected REPLCONF response, retrying. Received: {}", response);
                _replication_timer.expires_after(std::chrono::seconds(1));
                _replication_timer.async_wait([this, self, remaining](boost::system::error_code ec) {
                    if (!ec) {
                        replication_read_replconf_responses(remaining - 1);
                    }
                });
            }
        });
}

void Session::replication_send_psync()
{
    std::string psync_cmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    SPDLOG_INFO("Replication step [{}]: Sending PSYNC ? -1...", replication_step_name());
    auto self = shared_from_this();
    boost::asio::async_write(_socket, boost::asio::buffer(psync_cmd), [this, self](boost::system::error_code ec, std::size_t) {
        if (ec) {
            SPDLOG_ERROR("Replication failed to send PSYNC: {}", ec.message());
            return;
        }

        boost::asio::async_read_until(_socket, _repl_stream, "\r\n", [this, self](boost::system::error_code ec, std::size_t length) {
            if (ec) {
                SPDLOG_ERROR("Replication error reading PSYNC response: {}", ec.message());
                return;
            }

            std::string response(boost::asio::buffers_begin(_repl_stream.data()),
                boost::asio::buffers_begin(_repl_stream.data()) + length);
            _repl_stream.consume(length);

            if (response.find("+FULLRESYNC") != std::string::npos) {
                SPDLOG_INFO("Replication PSYNC FULLRESYNC received, handshake complete");
                _replication_step = ReplicationStep::Complete;
                replication_start_command_stream();
            } else if (response.find("+CONTINUE") != std::string::npos) {
                SPDLOG_INFO("Replication PSYNC CONTINUE received, handshake complete");
                _replication_step = ReplicationStep::Complete;
                replication_start_command_stream();
            } else {
                SPDLOG_ERROR("Replication unexpected PSYNC response. Received: {}", response);
            }
        });
    });
}

void Session::replication_start_command_stream()
{
    // After handshake, master will stream RDB + commands.
    // Minimal implementation: start reading RESP arrays and apply commands.
    SPDLOG_INFO("Replication session starting command stream from master");
    do_read();
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
            std::string fullresync = "+FULLRESYNC " + server->replid() + " " + std::to_string(server->repl_offset()) + "\r\n";
            do_write(fullresync);

            // 发送 RDB 文件
            server->send_rdb_file(shared_from_this());

            _is_replica = true;
            _handshake_state = HandshakeState::Complete;

            // 注册到 server
            _server->add_replica(shared_from_this());
            _server->increment_replica_count();
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
        do_write_replica();
    });
}

Task<void> Session::process_command_co(const std::vector<std::string>& command)
{
    // 1. 基本校验
    if (command.empty()) {
        do_write(RESPEncoder::encode_error("empty command"));
        co_return;
    }

    // 2. 规范化命令名（大写）
    std::string cmd = command[0];
    for (auto& c : cmd) {
        c = std::toupper(c);
    }

    // 从端
    if (_server->is_replication()) {
        size_t cmd_length = calculate_command_length(command);
        // 和主端的复制连接会话：
        if (_is_replication) {
            // 只对 REPLCONF GETACK * 发送响应，其它命令本地执行但不回写
            if (cmd == "REPLCONF" && command.size() >= 3) {
                std::string sub = command[1];
                for (auto& c : sub) {
                    c = std::toupper(c);
                }
                if (sub == "GETACK") {
                    // 符合题目要求：硬编码 offset 为 0
                    std::string offset = "0";

                    offset = std::to_string(_server->repl_offset());
                    std::string ack = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + std::to_string(offset.length()) + "\r\n" + offset + "\r\n";
                    do_write_raw(ack);
                    co_return;
                }
            } else if (cmd == "PING") {
                SPDLOG_INFO("Received PING from master, replying with PONG");

                _server->add_to_offset(cmd_length);
                co_return;
            }

            // 普通传播命令：只执行，不写回 master
            co_await execute_command_no_response(command);
            _server->add_to_offset(cmd_length);
            co_return;
        }
        // 从端，但是普通客户：禁止写命令
        else if (!_is_replication) {
            if (_server->is_write_command(cmd)) {
                do_write(RESPEncoder::encode_error_with_prefix(
                    "READONLY", "You can't write against a read only replica."));
                co_return;
            } else {
                goto normal_process;
            }
        }
    }
    // 主库逻辑
    else if (!_server->is_replication()) {
        // 作为主库时，处理 replica 握手流程（PING/REPLCONF/PSYNC）
        if (_is_replica) {
            if (replica_process(cmd, command)) {
                co_return;
            }

            if (cmd == "REPLCONF" && command.size() >= 3) {
                std::string sub = command[1];
                for (auto& c : sub) {
                    c = toupper(c);
                }

                if (sub == "ACK") {
                    uint64_t ack_offset = std::stoll(command[2]);
                    SPDLOG_INFO("Received ACK from replica {}, offset: {}", get_peer_info(), ack_offset);

                    _server->update_replica_offset(get_peer_info(), ack_offset);
                    co_return;
                }
            }

            SPDLOG_WARN("Replica sent unexpected command: {}", cmd);
            co_return;
        }
        // 主库接入的 replica：只执行命令，不返回响
        else if (!_is_replica) {
            bool handled = replica_process(cmd, command);
            if (handled) {
                co_return;
            } else {
                goto normal_process;
            }
        }
    }

normal_process:
    // 6. 通用预检查（比如命令存在性等）
    if (auto it = Config::CommandRegistry::pre_check(command)) {
        do_write(*it);
        co_return;
    }

    /* 增加服务器收到的命令数 */
    _server->incrment_command();

    // 7. 事务命令（MULTI / EXEC / DISCARD / WATCH）
    bool is_transaction_cmd = (cmd == "MULTI" || cmd == "EXEC" || cmd == "DISCARD" || cmd == "WATCH");
    if (is_transaction_cmd) {
        co_await transaction_process(command);
        co_return;
    }

    // 8. MULTI 之后的命令：排队
    if (_in_transaction) {
        _transaction_queue.push_back(command);
        do_write(RESPEncoder::encode_simple_string("QUEUED"));
        co_return;
    }

    // 9. 普通命令执行
    auto& registry = Config::CommandRegistry::instance();
    std::string response;
    if (registry.is_async(cmd)) {
        response = co_await registry.execute_async(cmd, command);
    } else {
        response = registry.execute(cmd, command);
    }

    // 10. 如果是写命令且当前是 master，传播给所有 replica
    if (_server && !_server->is_replication() && _server->is_write_command(cmd)) {
        // 构建 RESP 命令
        std::string resp_command = "*" + std::to_string(command.size()) + "\r\n";
        for (const auto& arg : command) {
            resp_command += "$" + std::to_string(arg.size()) + "\r\n";
            resp_command += arg + "\r\n";
        }

        std::size_t cmd_length = calculate_command_length(command);
        _server->add_to_offset(cmd_length);
        _server->propagate_command(command, resp_command);
    }

    // 11. 写回给客户端
    do_write(response);
    co_return;
}

Task<void> Session::transaction_process(const std::vector<std::string>& command)
{
    auto& cmd = command[0];
    if (cmd == "MULTI") {
        _in_transaction = true;
        _transaction_queue.clear();
        do_write(RESPEncoder::encode_simple_string("OK"));
    } else if (cmd == "EXEC") {
        if (!_in_transaction) {
            do_write(RESPEncoder::encode_error("EXEC without MULTI"));
            co_return;
        }

        std::string response = co_await queued_commands(_transaction_queue);

        _in_transaction = false;
        _transaction_queue.clear();

        do_write(response);
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
            // 键不存在，版本号视为 0
            _watched_versions[key] = version.value_or(0);
        }
        do_write(RESPEncoder::encode_simple_string("OK"));
    }
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

size_t Session::calculate_command_length(const std::vector<std::string>& command) const
{
    size_t length = 0;

    // 数组头: *<count>\r\n
    length += std::to_string(command.size()).size() + 3; // * + 数字 + \r\n

    for (const auto& arg : command) {
        // 每个参数: $<len>\r\n<arg>\r\n
        length += std::to_string(arg.size()).size() + 3; // $ + 数字 + \r\n
        length += arg.size(); // 参数内容
        length += 2; // 结尾的 \r\n
    }

    return length;
}
size_t Session::calculate_resp_length(const std::string& resp) const
{
    return resp.size();
}
}