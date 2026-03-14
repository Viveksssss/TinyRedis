#include "server.hpp"

#include <boost/asio/ip/address.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/system/detail/error_code.hpp>
#include <chrono>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <boost/asio/io_context.hpp>
#include <spdlog/spdlog.h>

#include "command_handlers.hpp"
#include "command_registry.hpp"
#include "rdb_parser.hpp"
#include "session.hpp"
#include "storage.hpp"

namespace Redis {

using boost::asio::ip::tcp;

static std::string generate_id()
{
    std::string _repl_id;
    _repl_id.reserve(40);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);
    const char* hex = "0123456789abcdef";
    for (int i = 0; i < 40; i++) {
        _repl_id += hex[dis(gen)];
    }
    return _repl_id;
}

Server::Server(boost::asio::io_context& io_context, const std::string& address, short port, const std::string& requirepass)
    : _io_context(io_context)
    , _acceptor(io_context)
    , _host(address)
    , _port(port)
    , _requirepass(requirepass)
{

    auto addr = boost::asio::ip::make_address(address);
    tcp::endpoint endpoint(addr, port);

    _acceptor.open(endpoint.protocol());
    _acceptor.set_option(tcp::acceptor::reuse_address(true));
    _acceptor.bind(endpoint);
    _acceptor.listen();

    collection();

    try {
        do_registry();
    } catch (std::exception& e) {
        throw e;
    }
    do_accept();
}

Server::Server(boost::asio::io_context& io_context, const std::string& address, short port, const std::string& master_host, short master_port, const std::string& requirepass)
    : Server(io_context, address, port, requirepass)
{
    _master_host = master_host;
    _master_port = master_port;
    _role = "replication";
    _is_replication = true;

    _dir = '.';
    _db_filename = "dump.rdb";
}

Server::Server(boost::asio::io_context& io_context, const std::string& address, short port,
    const std::string& dir, const std::string& dbfilename, const std::string& requirepass)
    : Server(io_context, address, port, requirepass) // 委托给基础构造函数
{
    _dir = dir;
    _db_filename = dbfilename;
    load_rdb_file();

    boost::asio::steady_timer timer(_io_context, std::chrono::seconds(1));
    timer.async_wait([this](const boost::system::error_code& error) {
        set_save_conditions(_save_conditions);
    });
}

Server::Server(boost::asio::io_context& io_context, const std::string& address, short port, const std::string& master_host, short master_port, const std::string& dir, const std::string& dbfilename, const std::string& requirepass)
    : Server(io_context, address, port, master_host, master_port, requirepass)
{
    _dir = dir;
    _db_filename = dbfilename;
    load_rdb_file();
    boost::asio::steady_timer timer(_io_context, std::chrono::seconds(1));
    timer.async_wait([this](const boost::system::error_code& error) {
        set_save_conditions(_save_conditions);
    });
}

void Server::stop()
{
    if (_stopped.exchange(true)) {
        return;
    }

    SPDLOG_INFO("Stopping server...");

    _save_monitor_running = false;
    _acceptor.close();
    _io_context.stop();

    save_rdb_file();

    SPDLOG_INFO("Server stopped");
}

void Server::register_wait(std::shared_ptr<WaitContext> ctx)
{
    std::lock_guard<std::mutex> lock(_wait_mutex);
    _waiting_waits.push_back(ctx);

    check_wait_conditions();
}

void Server::check_wait_conditions()
{
    auto now = std::chrono::steady_clock::now();
    auto it = _waiting_waits.begin();
    while (it != _waiting_waits.end()) {
        if (auto ctx = it->lock()) {
            SPDLOG_INFO("Checking wait: target_offset={}, required={}, current_acks={}, deadline={}",
                ctx->target_offset, ctx->required_replicas,
                count_replicas_with_offset_ge(ctx->target_offset),
                std::chrono::duration_cast<std::chrono::milliseconds>(ctx->deadline - now).count());
            // 检查是否超时
            if (now >= ctx->deadline) {
                // 超时，返回当前确认数量
                size_t acked = count_replicas_with_offset_ge(ctx->target_offset);
                if (!ctx->completed) {
                    ctx->completed = true;
                    if (auto sp = ctx->promise.lock()) {
                        sp->set_value(acked);
                    }
                }
                it = _waiting_waits.erase(it);
                continue;
            }

            // 检查是否已有足够 replica 确认
            size_t acked = count_replicas_with_offset_ge(ctx->target_offset);
            if (acked >= ctx->required_replicas && !ctx->completed) {
                ctx->completed = true;
                if (auto sp = ctx->promise.lock()) {
                    sp->set_value(acked);
                }
                it = _waiting_waits.erase(it);
                continue;
            }

            ++it;
        } else {
            // 清理失效的等待
            it = _waiting_waits.erase(it);
        }
    }
}

void Server::decrement_client()
{
    this->_total_clients--;
}

void Server::incrment_command()
{
    this->_total_commands++;
}

std::string Server::role() const
{
    return "master";
}

std::string Server::get_empty_rdb_content() const
{
    std::string hex = get_empty_rdb_hex();
    return hex_to_binary(hex);
}

std::string Server::get_rdb_content() const
{
    std::string binary;
    if (_db_filename.empty()) {

        binary = get_empty_rdb_content();
    } else {
        std::ifstream file(_dir + "/" + _db_filename, std::ios::binary);
        if (file.is_open()) {
            file.seekg(0, std::ios::end);
            std::streampos size = file.tellg();
            file.seekg(0, std::ios::beg);
            binary.resize(size);
            file.read(reinterpret_cast<char*>(binary.data()), size); // ← 加上这行！
            file.close();
        }
    }
    return binary;
}

std::string Server::get_empty_rdb_hex() const
{
    return "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
}
std::string Server::hex_to_binary(const std::string& hex) const
{
    std::string binary;
    binary.reserve(hex.length() / 2);

    for (size_t i = 0; i < hex.length(); i += 2) {
        std::string byte_str = hex.substr(i, 2);
        char byte = static_cast<char>(std::stoi(byte_str, nullptr, 16));
        binary.push_back(byte);
    }

    return binary;
}

bool Server::save_rdb_file()
{
    if (!_bgsave_running.exchange(true)) {

        auto& storage = Storage::instance();
        auto& data = storage.get_store(); // 需要这个方法

        RDBWriter writer(_dir, _db_filename);

        bool ok = writer.save(data);
        _bgsave_running = false;
        return ok; // 返回保存是否成功
    } else {
        return false;
    }
}

void Server::add_condition(std::size_t timeout, std::size_t frequency)
{
    _save_conditions.push_back({ timeout, frequency });
}

bool Server::load_config(const std::string& config_file)
{
    std::ifstream file(config_file);
    if (!file.is_open()) {
        SPDLOG_WARN("Could not open config file: {}", config_file);
        return false;
    }
    std::string line;
    while (std::getline(file, line)) {
        // 跳过注释和空行
        if (line.empty() || line[0] == '#')
            continue;

        std::istringstream iss(line);
        std::string directive;
        iss >> directive;

        if (directive == "requirepass") {
            std::string password;
            iss >> password;
            _requirepass = password;
            SPDLOG_INFO("Config: requirepass set");
        }
        // 可以添加其他配置指令
        else if (directive == "port") {
            int port;
            iss >> port;
            // 注意：端口通常在命令行指定，这里可以覆盖
            SPDLOG_INFO("Config: port set to {}", port);
        } else if (directive == "bind") {
            std::string bind;
            iss >> bind;
            SPDLOG_INFO("Config: bind set to {}", bind);
        } else if (directive == "dir") {
            std::string dir;
            iss >> dir;
            _dir = dir;
            SPDLOG_INFO("Config: dir set to {}", dir);
        } else if (directive == "dbfilename") {
            std::string dbfilename;
            iss >> dbfilename;
            _db_filename = dbfilename;
            SPDLOG_INFO("Config: dbfilename set to {}", dbfilename);
        }
    }

    return true;
}

void Server::set_save_conditions(const std::vector<SaveCondition>& conditions)
{
    _save_conditions = conditions;
    _last_save_time = std::chrono::steady_clock::now();

    if (!_save_monitor_running.exchange(true)) {
        _save_monitor_thread = std::make_unique<std::thread>([this]() {
            while (_save_monitor_running) {
                check_auto_save();
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        });
        _save_monitor_thread->detach();
    }
}
void Server::note_key_change()
{
    _key_changes++;
}
void Server::check_auto_save()
{
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
        now - _last_save_time)
                       .count();
    std::size_t changes = _key_changes.load();

    for (const auto& cond : _save_conditions) {
        if (elapsed >= cond.seconds && changes >= cond.changes) {
            // 条件满足，触发 BGSAVE
            SPDLOG_INFO("Auto-save triggered: {} seconds, {} changes",
                cond.seconds, cond.changes);

            // 重置计数和时间
            _key_changes = 0;
            _last_save_time = now;

            // 执行 BGSAVE
            save_rdb_file();
            break;
        }
    }
}

bool Server::load_rdb_file()
{
    parser = RDBParser(_dir, _db_filename);
    if (!parser.parse()) {
        SPDLOG_INFO("{}: No RDB file found or file is corrupted,starting with an empty database", parser.error());
        save_rdb_file();
        return false;
    }
    _rdb_data = parser.get_keys();
    auto& storage = Storage::instance();
    for (const auto& [key, value] : _rdb_data) {
        if (value.type == ValueType::STRING) {
            // 字符串类型
            std::string str_value = std::get<std::string>(value.value);
            if (!value.has_expiry) {
                storage.set_string(key, str_value);
            } else {
                auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(value.expiry - std::chrono::steady_clock::now());
                storage.set_string_with_expiry_ms(key, str_value, timeout);
            }
            SPDLOG_DEBUG("Loaded string: {} = {}", key, str_value);
        } else if (value.type == ValueType::LIST) {
            // 列表类型
            ListValue list_value = std::get<ListValue>(value.value);

            // 创建流或直接存储为列表
            // 这里我们直接用列表存储
            storage.get_store()[key] = ValueWithExpiry(list_value);

            SPDLOG_DEBUG("Loaded list: {} with {} elements", key, list_value.size());
        } else if (value.type == ValueType::SORTED_SET) {
            SortedSet sorted_set_value = std::get<SortedSet>(value.value);
            storage.get_store()[key] = ValueWithExpiry(sorted_set_value);
        } else if (value.type == ValueType::HASH) {
            HashValue hash = std::get<HashValue>(value.value);
            storage.get_store()[key] = ValueWithExpiry(hash);
        } else {
            SPDLOG_WARN("Unknown type for key: {}", key);
        }
    }
    return true;
}

bool Server::load_from_rdb(const std::string& rdb)
{
    RDBParser parser(rdb);
    if (!parser.parse()) {
        SPDLOG_INFO("{}: No RDB file found or file is corrupted,starting with an empty database", parser.error());
        save_rdb_file();
        return false;
    }
    _rdb_data = parser.get_keys();
    auto& storage = Storage::instance();
    for (const auto& [key, value] : _rdb_data) {
        if (value.type == ValueType::STRING) {
            // 字符串类型
            std::string str_value = std::get<std::string>(value.value);
            if (!value.has_expiry) {
                storage.set_string(key, str_value);
            } else {
                auto timeout = std::chrono::duration_cast<std::chrono::milliseconds>(value.expiry - std::chrono::steady_clock::now());
                storage.set_string_with_expiry_ms(key, str_value, timeout);
            }
            SPDLOG_DEBUG("Loaded string: {} = {}", key, str_value);
        } else if (value.type == ValueType::LIST) {
            // 列表类型
            ListValue list_value = std::get<ListValue>(value.value);

            // 创建流或直接存储为列表
            // 这里我们直接用列表存储
            storage.get_store()[key] = ValueWithExpiry(list_value);

            SPDLOG_DEBUG("Loaded list: {} with {} elements", key, list_value.size());
        } else {
            SPDLOG_WARN("Unknown type for key: {}", key);
        }
    }
    return true;
}

void Server::send_rdb_file(std::shared_ptr<Session> session)
{
    std::string rdb_content = get_rdb_content();
    size_t rdb_length = rdb_content.length();

    // 格式: $<length>\r\n<binary_contents>
    // RESP Bulk String requires trailing \r\n after the payload.
    std::string rdb_response = "$" + std::to_string(rdb_length) + "\r\n" + rdb_content + "\r\n";

    SPDLOG_INFO("Sending RDB file, length: {} bytes", rdb_length);

    // 直接发送给 replica
    session->do_write_raw(rdb_response);
}

void Server::add_replica(std::shared_ptr<Session> replica_session)
{
    std::lock_guard<std::mutex> lock(_replicas_mutex);
    _replicas.push_back(replica_session);
}
void Server::remove_replica(std::shared_ptr<Session> replica_session)
{
    std::lock_guard<std::mutex> lock(_replicas_mutex);
    auto it = std::find_if(_replicas.begin(), _replicas.end(),
        [replica_session](const auto& weak) {
            if (auto sp = weak.lock()) {
                return sp == replica_session;
            }
            return false;
        });

    if (it != _replicas.end()) {
        _replicas.erase(it);
        SPDLOG_INFO("Replica removed, remaining replicas: {}", _replicas.size());
    }
}

// 传播命令到所有 replica
void Server::propagate_command(const std::vector<std::string>& command, const std::string& resp_command)
{
    std::lock_guard<std::mutex> lock(_replicas_mutex);
    auto it = _replicas.begin();
    while (it != _replicas.end()) {
        if (auto sp = it->lock()) {
            sp->send_to_replica(resp_command);
            ++it;
        } else {
            it = _replicas.erase(it);
        }
    }
}
// 判断是否是写命令
bool Server::is_write_command(const std::string& cmd) const
{
    static const std::unordered_set<std::string> write_commands = {
        "SET", "DEL", "INCR", "DECR", "INCRBY", "DECRBY",
        "LPUSH", "RPUSH", "LPOP", "RPOP", "LREM", "LSET",
        "XADD", "XDEL", "XTRIM"
    };

    std::string upper_cmd = cmd;
    for (auto& c : upper_cmd) {
        c = toupper(c);
    }

    return write_commands.find(upper_cmd) != write_commands.end();
}

size_t Server::count_replicas_with_offset_ge(uint64_t offset) const
{
    size_t count = 0;
    for (const auto& [_, replica_offset] : _replica_offsets) {
        if (replica_offset >= offset) {
            count++;
        }
    }
    return count;
}
void Server::send_getack_to_all()
{
    std::lock_guard<std::mutex> lock(_replicas_mutex);
    std::string getack_cmd = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";

    for (const auto& weak_replica : _replicas) {
        if (auto sp = weak_replica.lock()) {
            // sp->do_write_raw(getack_cmd);
            sp->send_to_replica(getack_cmd);
        } else {
            // 清理失效的 replica
            _replicas.erase(std::remove_if(_replicas.begin(), _replicas.end(), [&weak_replica](const std::weak_ptr<Redis::Session>& elem) {
                return elem.lock() == weak_replica.lock();
            }),
                _replicas.end());
        }
    }
}

void Server::update_replica_offset(const std::string& replica_id, uint64_t offset)
{
    std::lock_guard<std::mutex> lock(_replica_offsets_mutex);
    _replica_offsets[replica_id] = offset;

    check_wait_conditions();
}

std::size_t Server::replica_count()
{
    std::lock_guard<std::mutex> lock(_replicas_mutex);
    std::size_t count = 0;
    auto it = _replicas.begin();
    while (it != _replicas.end()) {
        if (auto sp = it->lock() && it->expired()) {
            it = _replicas.erase(it);
        } else {
            ++it;
            count++;
        }
    }

    return count;
}

void Server::start_replication()
{
    SPDLOG_INFO("Starting replication (replica connects to master)...");
    auto self = shared_from_this();
    boost::asio::post(_io_context, [this, self]() {
        boost::asio::ip::tcp::socket socket(_io_context);
        _master_session = std::make_shared<Redis::Session>(_io_context, std::move(socket), self);
        _master_session->set_replication_mode(true);
        _master_session->start_replication(_master_host, _master_port, _port);
    });
}

std::string Server::info(const std::string& section) const
{
    std::ostringstream response;

    // 运行时间
    auto now = std::chrono::system_clock::now();
    auto uptime_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - _start_time).count();
    auto uptime_days = uptime_seconds / (24 * 3600);

    // 内存使用情况
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);

    // Server 部分
    if (section.empty() || section == "server" || section == "default" || section == "all") {
        response << "# Server\r\n";
        response << "redis_version:1.0.0\r\n";
        response << "redis_git_sha1:00000000\r\n";
        response << "redis_git_dirty:0\r\n";
        response << "redis_build_id:" << reinterpret_cast<uintptr_t>(this) << "\r\n";
        response << "redis_mode:standalone\r\n";
        response << "os:" << _os_info << "\r\n";
        response << "arch_bits:" << _arch_bits << "\r\n";
        response << "multiplexing_api:" << _multiplexing_api << "\r\n";
        response << "gcc_version:" << _gcc_version << "\r\n";
        response << "process_id:" << _process_id << "\r\n";
        response << "process_supervised:no\r\n";
        response << "run_id:" << _repl_id << "\r\n";
        response << "tcp_port:" << _acceptor.local_endpoint().port() << "\r\n";

        // ⭐ 时间戳和运行时间
        auto system_now = std::chrono::system_clock::now();
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(
            system_now.time_since_epoch())
                      .count();
        response << "server_time_usec:" << us << "\r\n";
        response << "uptime_in_seconds:" << uptime_seconds << "\r\n";
        response << "uptime_in_days:" << uptime_days << "\r\n";

        response << "start_time:" << _start_time_str << "\r\n";

        response << "hz:10\r\n";
        response << "configured_hz:10\r\n";
        response << "lru_clock:" << (time(nullptr) / 60) << "\r\n";
        response << "executable:" << "/proc/self/exe" << "\r\n";
        response << "config_file:" << "\r\n";
        response << "io_threads_active:0\r\n";
        response << "name:tcp,bind=" << _host << ",port=" << _port << "\r\n";
        response << "\r\n";
    }

    // Clients 部分
    if (section.empty() || section == "clients" || section == "default" || section == "all") {
        response << "# Clients\r\n";
        // TODO: 从 Session 管理器获取实际连接数
        response << "connected_clients:" << _total_clients << "\r\n";
        response << "cluster_connections:0\r\n";
        response << "maxclients:10000\r\n";
        response << "client_recent_max_input_buffer:0\r\n";
        response << "client_recent_max_output_buffer:0\r\n";
        response << "blocked_clients:0\r\n";
        response << "tracking_clients:0\r\n";
        response << "watching_clients:0\r\n";
        response << "\r\n";
    }

    // Memory 部分
    if (section.empty() || section == "memory" || section == "default" || section == "all") {
        response << "# Memory\r\n";

        // 获取进程内存使用
        long rss = 0;
        FILE* statm = fopen("/proc/self/statm", "r");
        if (statm) {
            long size, resident, share, text, lib, data, dt;
            if (fscanf(statm, "%ld %ld %ld %ld %ld %ld %ld",
                    &size, &resident, &share, &text, &lib, &data, &dt)
                == 7) {
                rss = resident * sysconf(_SC_PAGESIZE);
            }
            fclose(statm);
        }

        response << "used_memory:0\r\n";
        response << "used_memory_human:0B\r\n";
        response << "used_memory_rss:" << rss << "\r\n";
        response << "used_memory_rss_human:" << (rss / 1024 / 1024) << "M\r\n";
        response << "used_memory_peak:0\r\n";
        response << "used_memory_peak_human:0B\r\n";
        response << "used_memory_peak_perc:0%\r\n";
        response << "used_memory_overhead:0\r\n";
        response << "used_memory_startup:0\r\n";
        response << "used_memory_dataset:0\r\n";
        response << "used_memory_dataset_perc:0%\r\n";

        // 系统内存
        long pages = sysconf(_SC_PHYS_PAGES);
        long page_size = sysconf(_SC_PAGESIZE);
        long total_mem = pages * page_size;
        response << "total_system_memory:" << total_mem << "\r\n";
        response << "total_system_memory_human:" << (total_mem / 1024 / 1024) << "M\r\n";

        response << "maxmemory:0\r\n";
        response << "maxmemory_human:0B\r\n";
        response << "maxmemory_policy:noeviction\r\n";
        response << "mem_fragmentation_ratio:1.00\r\n";
        response << "mem_fragmentation_bytes:0\r\n";
        response << "mem_allocator:jemalloc-5.2.1\r\n";
        response << "active_defrag_running:0\r\n";
        response << "lazyfree_pending_objects:0\r\n";
        response << "lazyfreed_objects:0\r\n";
        response << "\r\n";
    }

    // Persistence 部分
    if (section.empty() || section == "persistence" || section == "default" || section == "all") {
        response << "# Persistence\r\n";
        response << "loading:0\r\n";
        response << "rdb_changes_since_last_save:0\r\n";
        response << "rdb_bgsave_in_progress:0\r\n";
        response << "rdb_last_save_time:" << time(nullptr) << "\r\n";
        response << "rdb_last_bgsave_status:ok\r\n";
        response << "rdb_last_bgsave_time_sec:-1\r\n";
        response << "rdb_current_bgsave_time_sec:-1\r\n";
        response << "aof_enabled:0\r\n";
        response << "\r\n";
    }

    // Stats 部分
    if (section.empty() || section == "stats" || section == "default" || section == "all") {
        response << "# Stats\r\n";
        response << "total_connections_received:" << _total_clients << "\r\n";
        response << "total_commands_processed:" << _total_commands << "\r\n";

        // ⭐ 计算每秒操作数
        auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - _start_time).count();
        if (uptime > 0) {
            response << "instantaneous_ops_per_sec:" << (_total_commands / uptime) << "\r\n";
        } else {
            response << "instantaneous_ops_per_sec:0\r\n";
        }

        response << "total_net_input_bytes:0\r\n";
        response << "total_net_output_bytes:0\r\n";
        response << "instantaneous_input_kbps:0.00\r\n";
        response << "instantaneous_output_kbps:0.00\r\n";
        response << "rejected_connections:0\r\n";
        response << "evicted_keys:0\r\n";
        response << "keyspace_hits:0\r\n";
        response << "keyspace_misses:0\r\n";
        response << "pubsub_channels:0\r\n";
        response << "pubsub_patterns:0\r\n";
        response << "latest_fork_usec:0\r\n";
        response << "\r\n";
    }

    if (section.empty() || section == "replication" || section == "default" || section == "all") {
        response << "# Replication\r\n";
        // Replication
        if (_is_replication) {
            response << "role:slave\r\n";
            response << "master_host:" << _master_host << "\r\n";
            response << "master_port:" << _master_port << "\r\n";
            response << "master_link_status:down\r\n"; // 后续阶段会实现
            response << "master_last_io_seconds_ago:-1\r\n";
            response << "master_sync_in_progress:0\r\n";
            response << "slave_read_repl_offset:" << _repl_offset << "\r\n";
            response << "slave_repl_offset:" << _repl_offset << "\r\n";
        } else {
            response << "role:master\r\n";
            response << "connected_slaves:" << _replicas.size() << "\r\n";
            response << "master_replid:" << _repl_id << "\r\n";
            response << "master_repl_offset:" << _repl_offset << "\r\n";
            response << "second_repl_offset:-1\r\n";
            response << "repl_backlog_active:0\r\n";
            response << "repl_backlog_size:1048576\r\n";
            response << "repl_backlog_first_byte_offset:0\r\n";
            response << "repl_backlog_histlen:0\r\n";
        }
    }

    // CPU 部分
    if (section.empty() || section == "cpu" || section == "default" || section == "all") {
        response << "# CPU\r\n";
        response << "used_cpu_sys:" << usage.ru_stime.tv_sec << "."
                 << usage.ru_stime.tv_usec << "\r\n";
        response << "used_cpu_user:" << usage.ru_utime.tv_sec << "."
                 << usage.ru_utime.tv_usec << "\r\n";
        response << "used_cpu_sys_children:0.00\r\n";
        response << "used_cpu_user_children:0.00\r\n";

        // ⭐ CPU使用率（粗略计算）
        double total_cpu = usage.ru_stime.tv_sec + usage.ru_utime.tv_sec + (usage.ru_stime.tv_usec + usage.ru_utime.tv_usec) / 1000000.0;
        if (uptime_seconds > 0) {
            response << "used_cpu_percent:" << std::fixed << std::setprecision(2)
                     << (total_cpu / uptime_seconds * 100) << "\r\n";
        }

        response << "\r\n";
    }

    return response.str();
}

void Server::collection()
{

    _save_conditions.push_back({ 900, 1 }); // 900秒 1个变化
    _save_conditions.push_back({ 300, 10 }); // 300秒 10个变化
    _save_conditions.push_back({ 60, 10000 }); // 60秒 10000个变化

    collection_server();
    collection_system();
}

void Server::collection_server()
{
    _repl_id.reserve(40);
    _repl_id = generate_id();
    _repl_offset = 0;
}
void Server::collection_system()
{
    // 启动时间
    _start_time = std::chrono::system_clock::now();
    auto start_time_t = std::chrono::system_clock::to_time_t(_start_time);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&start_time_t), "%Y-%m-%d %H:%M:%S");
    _start_time_str = ss.str();

    // 获取系统信息
    struct utsname sys_info;
    if (uname(&sys_info) == 0) {
        _os_info = std::string(sys_info.sysname) + " " + std::string(sys_info.release) + " " + std::string(sys_info.machine);
    } else {
        _os_info = "Unknown";
    }

    // 进程ID
    _process_id = getpid();

    // 获取架构位数
    _arch_bits = (sizeof(void*) == 8) ? "64" : "32";

    // 获取事件机制（根据编译平台）
#ifdef __linux__
    _multiplexing_api = "epoll";
#elif defined(__APPLE__) || defined(__FreeBSD__)
    _multiplexing_api = "kqueue";
#elif defined(_WIN32)
    _multiplexing_api = "IOCP";
#else
    _multiplexing_api = "select";
#endif

    // 获取GCC版本
#ifdef __GNUC__
    _gcc_version = std::to_string(__GNUC__) + "." + std::to_string(__GNUC_MINOR__) + "." + std::to_string(__GNUC_PATCHLEVEL__);
#else
    _gcc_version = "unknown";
#endif
}

void Server::do_registry()
{
    try {
        auto& registry = Config::CommandRegistry::instance();
        if (!registry.load_from_file("config/commands.json")) {
            std::cerr << "Warning: Failed to load command config, using defaults" << std::endl;
        }
        Config::CommandHandlers::register_all();
    } catch (std::exception& e) {
        throw e;
    }
}

void Server::do_accept()
{
    _acceptor.async_accept([this](boost::system::error_code ec, tcp::socket socket) {
        if (!ec) {
            std::make_shared<Redis::Session>(_io_context, std::move(socket), shared_from_this())->start();
            _total_clients++;
        }
        do_accept();
    });
}
}