#pragma once

#include "task.hpp"

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/system/detail/error_code.hpp>

#include <array>
#include <cstdint>
#include <memory>
#include <queue>
#include <unordered_set>

namespace Redis {

enum class ReplicationStep {
    None,
    Ping,
    ReplconfPort,
    ReplconfCapa,
    Psync,
    Complete
};

enum class HandshakeState {
    None,
    PingReceived,
    RepliconfPortReceived,
    RepliconfCapaReceived,
    PsyncReceived,
    Complete
};

class Server;
class Session : public std::enable_shared_from_this<Session> {
public:
    explicit Session(boost::asio::io_context& context, boost::asio::ip::tcp::socket socket, std::shared_ptr<Server> server);
    ~Session();

    void start();
    void do_write_raw(const std::string& content);
    // Outbound replication session (replica -> master)
    void set_replication_mode(bool enabled) { _is_replication = enabled; }
    bool is_replication() const { return _is_replication; }
    void start_replication(const std::string& master_host, std::uint16_t master_port, std::uint16_t listening_port);

public:
    // 获取连接信息
    std::string remote_endpoint() const;
    // 判断是否是 replica
    bool is_replica() const { return _is_replica; }
    // 发送命令到 replica（用于 master 向 replica 传播）
    void send_to_replica(const std::string& command);
    // 获取连接信息用于日志
    std::string get_peer_info() const;
    void do_write_replica(); // 专门用于向 replica 发送命令
    void set_replica_mode(bool enabled) { this->_is_replica = enabled; }

private:
    void do_read();
    void do_write(const std::string& response);
    void replication_connect();
    void replication_send_ping();
    void replication_send_replconf_listening_port();
    void replication_send_replconf_capa();
    void replication_read_replconf_responses(int remaining);
    void replication_send_psync();
    void replication_start_command_stream();
    std::string replication_step_name() const;
    // std::string process_command(const std::vector<std::string>& command);
    bool replica_process(std::string_view, const std::vector<std::string>& command);
    Task<void> transaction_process(const std::vector<std::string>& command);
    Task<void> process_command_co(const std::vector<std::string>& command);
    Task<void> execute_command_no_response(const std::vector<std::string>& command);
    Task<std::string> queued_commands(const std::vector<std::vector<std::string>>& commands);
    void handle_error(const boost::system::error_code& ec, const std::string& operation);

    size_t calculate_command_length(const std::vector<std::string>& command) const;
    size_t calculate_resp_length(const std::string& resp) const;

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

    // Server
    std::shared_ptr<Server> _server;

    // Replicaof 相关
    // _is_replica      : master 侧接入的 replica 连接（从库连上来，主库视角）
    // _is_replication  : 从库侧主动连主库的复制连接（slave -> master）
    bool _is_replica { false };
    bool _is_replication { false };
    HandshakeState _handshake_state { HandshakeState::None };
    std::queue<std::string> _write_queue;
    bool _writing { false };

    // Replication client state (outbound to master)
    std::string _master_host;
    std::uint16_t _master_port { 0 };
    std::uint16_t _listening_port { 0 };
    boost::asio::steady_timer _replication_timer;
    boost::asio::streambuf _repl_stream;
    std::array<char, 1024> _repl_read_buffer;
    ReplicationStep _replication_step { ReplicationStep::None };
};

}
