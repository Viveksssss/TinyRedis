#pragma once

#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <chrono>
#include <cstdint>
#include <memory>

namespace Redis {

enum class ReplicationStep {
    None, // 未开始
    Ping, // 发送 PING
    ReplconfPort, // 发送 REPLCONF listening-port
    ReplconfCapa, // 发送 REPLCONF capa
    Psync, // 发送 PSYNC
    Complete // 握手完成
};

class Session;
class Server : public std::enable_shared_from_this<Server> {
public:
    Server(boost::asio::io_context& io_context, const std::string& address, short port);
    Server(boost::asio::io_context& io_context, const std::string& address, short port, const std::string& master_host, short master_port);

public:
    // 客户端减少
    void decrement_client();
    // 命令数
    void incrment_command();
    // 获取服务器角色
    std::string role() const;
    // 生成 INFO 响应
    std::string info(const std::string& section = "") const;
    std::string replid() const { return _repl_id; }
    std::string repl_offset() const { return _repl_offset; }
    void send_rdb_file(std::shared_ptr<Session> session);

    // 注册 replica 连接
    void add_replica(std::shared_ptr<Session> replica_session);
    void remove_replica(std::shared_ptr<Session> replica_session);
    // 传播命令到所有 replica
    void propagate_command(const std::vector<std::string>& command, const std::string& resp_command);
    // 判断是否是写命令
    bool is_write_command(const std::string& cmd) const;
    bool is_replication() { return _is_replication; };

private:
    /* RDB */
    std::string get_empty_rdb_content() const;
    std::string get_empty_rdb_hex() const;
    std::string hex_to_binary(const std::string& hex) const;

    /* Replicaof */
    void start_replication();
    void connect_to_master();
    void send_ping();
    void send_replconf_listening_port();
    void send_replconf_capa();
    void send_psync();
    void handler_master_response(const boost::system::error_code& ec, std::size_t bytes_transferred);
    void read_replconf_responses(int remaining);
    std::string step_name() const;

    // replica 连接管理
    std::vector<std::weak_ptr<Session>> _replicas;
    std::mutex _replicas_mutex;

    boost::asio::streambuf _stream;
    std::string _master_replid;
    std::string _offset_str;
    boost::asio::ip::tcp::socket _master_socket;
    boost::asio::steady_timer _replication_timer;
    std::array<char, 1024> _read_buffer;
    std::string _response_buffer;
    ReplicationStep _handshake_step { ReplicationStep::None }; // 使用 enum class

private:
    void
    collection();
    void collection_server();
    void collection_system();

    /* 服务器 */
    std::string _role { "master" };
    std::string _repl_id; // 后续使用
    std::string _repl_offset; // 后续使用

    /* 系统 */
    std::string _os_info; // 操作系统信息
    std::string _arch_bits; // 架构位数
    std::string _multiplexing_api; // 事件机制 (epoll/select/kqueue)
    std::string _gcc_version; // GCC版本
    pid_t _process_id; // 进程ID
    std::chrono::system_clock::time_point _start_time; // 启动时间
    std::string _start_time_str; // 日常格式

private:
    void do_registry();
    void do_accept();

    boost::asio::ip::tcp::acceptor _acceptor;
    boost::asio::io_context& _io_context;
    bool _is_replication = false;

    std::size_t _total_clients = 0;
    std::size_t _total_commands = 0;
    std::string _master_host;
    std::uint16_t _master_port;
    std::string _host;
    std::uint16_t _port;
};

}