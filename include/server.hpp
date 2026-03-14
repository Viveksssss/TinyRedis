#pragma once

#include "rdb_parser.hpp"
#include "rdb_writer.hpp"
#include "storage.hpp"
#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <unordered_map>

namespace Redis {

class Session;
class Server : public std::enable_shared_from_this<Server> {
public:
    Server(boost::asio::io_context& io_context, const std::string& address, short port, const std::string& requirepass);
    Server(boost::asio::io_context& io_context, const std::string& address, short port, const std::string& master_host, short master_port, const std::string& requirepass);
    Server(boost::asio::io_context& io_context, const std::string& address, short port,
        const std::string& dir, const std::string& dbfilename, const std::string& requirepass);
    Server(boost::asio::io_context& io_context, const std::string& address, short port,
        const std::string& master_host, short master_port, const std::string& dir, const std::string& dbfilename, const std::string& requirepass);

    void stop();

    // Must be called after Server is owned by a shared_ptr (i.e. after construction).
    void start_replication();
    boost::asio::io_context& io_context() { return _io_context; }

    std::atomic<bool> _stopped { false };

public:
    struct WaitContext {
        std::size_t target_offset;
        std::size_t required_replicas;
        std::chrono::steady_clock::time_point deadline;
        std::shared_ptr<boost::asio::steady_timer> timer;
        std::weak_ptr<std::promise<std::size_t>> promise;
        bool completed { false };
    };

    // 注册 WAIT 等待
    void register_wait(std::shared_ptr<WaitContext> ctx);
    // 检查是否有等待条件满足
    void check_wait_conditions();

    // 客户端减少
    void decrement_client();
    // 命令数
    void incrment_command();
    // 获取服务器角色
    std::string role() const;
    // 生成 INFO 响应
    std::string info(const std::string& section = "") const;
    std::string replid() const { return _repl_id; }
    void send_rdb_file(std::shared_ptr<Session> session);

    // 注册 replica 连接
    void add_replica(std::shared_ptr<Session> replica_session);
    void remove_replica(std::shared_ptr<Session> replica_session);
    // 传播命令到所有 replica
    void propagate_command(const std::vector<std::string>& command, const std::string& resp_command);
    // 判断是否是写命令
    bool is_write_command(const std::string& cmd) const;
    bool is_replication() { return _is_replication; };
    void update_repl_offset(std::size_t new_offset) { _repl_offset = new_offset; }
    std::size_t repl_offset() const { return _repl_offset; }
    void add_to_offset(std::size_t increment) { _repl_offset += increment; }
    size_t count_replicas_with_offset_ge(uint64_t offset) const;
    void send_getack_to_all();
    void update_replica_offset(const std::string& replica_id, uint64_t offset);

    // 从库数量
    std::size_t replica_count();
    void increment_replica_count() { _replica_count++; }
    void decrement_replica_count() { _replica_count--; }

private:
    // replica 连接管理
    std::vector<std::weak_ptr<Session>> _replicas;
    std::mutex _replicas_mutex;
    // Outbound session (replica -> master). Must be strongly held.
    std::shared_ptr<Session> _master_session;

private:
    void collection();
    void collection_server();
    void collection_system();

    /* 服务器 */
    std::string _role { "master" };
    std::string _repl_id; // 后续使用
    std::atomic<std::size_t> _repl_offset; // 后续使用
    // replica 偏移量映射
    std::unordered_map<std::string, uint64_t> _replica_offsets;
    mutable std::mutex _replica_offsets_mutex;
    std::vector<std::weak_ptr<WaitContext>> _waiting_waits;
    std::mutex _wait_mutex;

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

    std::size_t _replica_count = 0;
    std::size_t _total_clients = 0;
    std::size_t _total_commands = 0;
    std::string _master_host;
    std::uint16_t _master_port;
    std::string _host;
    std::uint16_t _port;
    /* RDB */
private:
    std::string get_empty_rdb_content() const;
    std::string get_empty_rdb_hex() const;
    std::string get_rdb_content() const;
    std::string hex_to_binary(const std::string& hex) const;
    bool load_rdb_file();

    std::string _dir;
    std::string _db_filename;
    RDBParser parser;
    RDBWriter writer;
    std::unordered_map<std::string, ValueWithExpiry> _rdb_data;
    std::atomic<bool> _bgsave_running { false };

public:
    bool load_from_rdb(const std::string& rdb);
    std::string get_dir() const { return _dir; }
    std::string get_dbfilename() const { return _db_filename; }
    void set_dir(const std::string& dir) { _dir = dir; }
    void set_dbfilename(const std::string& dbfilename) { _db_filename = dbfilename; }
    bool save_rdb_file();
    bool is_bgsave_running() const { return _bgsave_running.load(); }

public:
    struct SaveCondition {
        std::size_t seconds; // 时间窗口（秒）
        std::size_t changes; // 变化的key数量
    };
    void set_save_conditions(const std::vector<SaveCondition>& conditions);
    void note_key_change();
    void check_auto_save();
    void add_condition(std::size_t, std::size_t);

private:
    // 自动保存相关
    std::vector<SaveCondition> _save_conditions;
    std::atomic<size_t> _key_changes { 0 };
    std::chrono::steady_clock::time_point _last_save_time;
    std::mutex _save_mutex;
    std::unique_ptr<std::thread> _save_monitor_thread;
    std::atomic<bool> _save_monitor_running { false };

    /* auth/config */
public:
    void set_password(const std::string& password) { _requirepass = password; }
    bool check_password(const std::string& password) const { return !_requirepass.empty() && _requirepass == password; }
    bool requires_auth() const { return !_requirepass.empty(); }

    // 配置文件
    bool load_config(const std::string& config_file);

private:
    std::string _requirepass; // requirepass 配置
};

}