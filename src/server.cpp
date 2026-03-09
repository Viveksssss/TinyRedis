#include "server.hpp"

#include <boost/asio/ip/address.hpp>
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
#include "resp_parse.hpp"
#include "session.hpp"

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

Server::Server(boost::asio::io_context& io_context, const std::string& address, short port)
    : _io_context(io_context)
    , _acceptor(io_context)
    , _host(address)
    , _port(port)
    , _master_socket(io_context)
    , _replication_timer(io_context)
    , _handshake_step(ReplicationStep::None)
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

Server::Server(boost::asio::io_context& io_context, const std::string& address, short port, const std::string& master_host, short master_port)
    : Server(io_context, address, port)
{
    _master_host = master_host;
    _master_port = master_port;
    _role = "replication";
    _is_replication = true;

    start_replication();
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

void Server::send_rdb_file(std::shared_ptr<Session> session)
{
    std::string rdb_content = get_empty_rdb_content();
    size_t rdb_length = rdb_content.length();

    // 格式: $<length>\r\n<binary_contents>
    std::string rdb_response = "$" + std::to_string(rdb_length) + "\r\n" + rdb_content;

    SPDLOG_INFO("Sending empty RDB file, length: {} bytes", rdb_length);

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

std::string Server::step_name() const
{
    switch (_handshake_step) {
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

void Server::start_replication()
{
    SPDLOG_INFO("Starting replication handshake...");
    // 延迟一下再连接，确保服务器已经启动
    // _replication_timer.expires_after(std::chrono::milliseconds(1000));
    // _replication_timer.async_wait([this](boost::system::error_code ec) {
    //     if (!ec) {
    //         connect_to_master();
    //     }
    // });

    boost::asio::post(_io_context, [this]() {
        // 但这里也不能直接用 shared_from_this，因为 post 的回调可能
        // 在构造函数还没返回时就执行（如果 io_context 已经在运行）

        // 更安全：再用一次定时器，延迟 0 秒
        _replication_timer.expires_after(std::chrono::milliseconds(1000));
        _replication_timer.async_wait([this](boost::system::error_code ec) {
            if (!ec) {
                connect_to_master();
            }
        });
    });
}

void Server::connect_to_master()
{
    SPDLOG_INFO("Connecting to master {}:{}", _master_host, _master_port);

    tcp::resolver resolver(_io_context);
    auto endpoints = resolver.resolve(_master_host, std::to_string(_master_port));

    auto self = shared_from_this();
    boost::asio::async_connect(_master_socket, endpoints, [this, self](boost::system::error_code ec, tcp::endpoint) {
        if (!ec) {
            SPDLOG_INFO("Connected to master, starting handshake...");

            _handshake_step = ReplicationStep::Ping;
            send_ping();
        } else {
            SPDLOG_ERROR("Failed to connect to master: {}", ec.message());
            _replication_timer.expires_after(std::chrono::seconds(5));
            _replication_timer.async_wait([this](boost::system::error_code ec) {
                if (!ec) {
                    connect_to_master();
                }
            });
        }
    });
}
void Server::send_ping()
{
    SPDLOG_INFO("Step [{}]: Sending PING...", step_name());

    std::string ping_cmd = "*1\r\n$4\r\nPING\r\n";

    auto self = shared_from_this();
    boost::asio::async_write(_master_socket, boost::asio::buffer(ping_cmd), [this, self](boost::system::error_code ec, std::size_t) {
        if (!ec) {
            _master_socket.async_read_some(boost::asio::buffer(_read_buffer), [this, self](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    std::string response(_read_buffer.data(), length);

                    if (response.find("+PONG") != std::string::npos) {
                        SPDLOG_INFO("PING successful, moving to next step");
                        _handshake_step = ReplicationStep::ReplconfPort;
                        send_replconf_listening_port();
                    } else {
                        SPDLOG_ERROR("Unexpected response, retrying PING");
                        _replication_timer.expires_after(std::chrono::seconds(5));
                        _replication_timer.async_wait([this](boost::system::error_code ec) {
                            if (!ec) {
                                send_ping(); // 保持在 PING 步骤重试
                            }
                        });
                    }
                } else {
                    SPDLOG_ERROR("Error reading PONG: {}", ec.message());
                }
            });
        } else {
            SPDLOG_ERROR("Failed to send PING: {}", ec.message());
        }
    });
}
void Server::send_replconf_listening_port()
{
    // std::string replconf_cmd = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$"
    //     + std::to_string(std::to_string(_port).length()) + "\r\n"
    //     + std::to_string(_port) + "\r\n";

    // 端口信息
    std::vector<std::string> replconf_vec;
    replconf_vec.reserve(3);
    replconf_vec.emplace_back("REPLCONF");
    replconf_vec.emplace_back("listening-port");
    replconf_vec.push_back(std::to_string(std::to_string(_port).length()));
    std::string replconf_cmd = RESPEncoder::encode_array(replconf_vec);

    SPDLOG_INFO("Step [{}]: Sending REPLICONF listening-port {}", step_name(), _port);

    auto self = shared_from_this();

    boost::asio::async_write(_master_socket, boost::asio::buffer(replconf_cmd), [this, self](boost::system::error_code ec, std::size_t) {
        if (!ec) {
            _master_socket.async_read_some(boost::asio::buffer(_read_buffer), [this, self](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    std::string response(_read_buffer.data(), length);

                    if (response.find("+OK") != std::string::npos) {
                        SPDLOG_INFO("REPLCONF listening-port successful, moving to next step");
                        _handshake_step = ReplicationStep::ReplconfCapa;
                        send_replconf_capa();
                    } else {
                        SPDLOG_ERROR("Unexpected response, retrying REPLCONF listening-port");
                        _replication_timer.expires_after(std::chrono::seconds(5));
                        _replication_timer.async_wait([this](boost::system::error_code ec) {
                            if (!ec) {
                                send_replconf_listening_port();
                            }
                        });
                    }
                } else {

                    SPDLOG_ERROR("Error reading REPLCONF response: {}", ec.message());
                }
            });
        } else {
            SPDLOG_ERROR("Failed to send REPLCONF listening-port: {}", ec.message());
        }
    });
}
void Server::send_replconf_capa()
{
    std::string replconf_cmd1 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n";
    std::string replconf_cmd2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";

    SPDLOG_INFO("Step [{}]: Sending REPLCONF capa eof & psync2", step_name());

    auto self = shared_from_this();
    boost::asio::async_write(_master_socket, boost::asio::buffer(replconf_cmd1), [this, self, replconf_cmd2](boost::system::error_code ec, std::size_t) {
        if (ec) {
            SPDLOG_ERROR("Failed to send REPLCONF capa: {}", ec.message());
        }

        boost::asio::async_write(_master_socket, boost::asio::buffer(replconf_cmd2), [this, self](boost::system::error_code ec, std::size_t) {
            if (ec) {
                SPDLOG_ERROR("Failed to send REPLCONF capa: {}", ec.message());
            }
            read_replconf_responses(2);
        });
    });
}
void Server::send_psync()
{

    // PSYNC ? -1
    std::string psync_cmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";

    SPDLOG_INFO("Step [{}]: Sending PSYNC ? -1...", step_name());

    auto self = shared_from_this();

    boost::asio::async_write(_master_socket, boost::asio::buffer(psync_cmd), [this, self](boost::system::error_code ec, std::size_t) {
        if (ec) { // 先检查错误
            SPDLOG_ERROR("Failed to send PSYNC: {}", ec.message());
            return;
        }
        // 等待 FULLRESYNC 响应

        boost::asio::async_read_until(_master_socket, _stream, "\r\n", [this, self](boost::system::error_code ec, std::size_t length) {
            if (ec) { // 先检查读取错误
                SPDLOG_ERROR("Error reading PSYNC response: {}", ec.message());
                return;
            }

            std::string response(boost::asio::buffers_begin(_stream.data()), boost::asio::buffers_begin(_stream.data()) + length);
            if (response.find("+FULLRESYNC") != std::string::npos) {
                SPDLOG_INFO("PSYNC successful, replication handshake complete!");
                _handshake_step = ReplicationStep::Complete;

                size_t space1 = response.find(' ');
                size_t space2 = response.find(' ', space1 + 1);

                if (space1 != std::string::npos && space2 != std::string::npos) {
                    std::string master_replid = response.substr(space1 + 1, space2 - space1 - 1);
                    std::string offset_str = response.substr(space2 + 1);

                    if (!offset_str.empty() && offset_str.back() == '\n') {
                        offset_str.pop_back();
                    }
                    if (!offset_str.empty() && offset_str.back() == '\r') {
                        offset_str.pop_back();
                    }

                    this->_master_replid = master_replid;
                    this->_offset_str = offset_str;

                    SPDLOG_INFO("Replication handshake complete! Master replid: {}, offset: {}", master_replid, offset_str);
                }
            } else if (response.find("+CONTINUE") != std::string::npos) {

            } else {
                SPDLOG_ERROR("Unexpected PSYNC response, retrying PSYNC");
            }
        });
    });
}
void Server::handler_master_response(const boost::system::error_code& ec, std::size_t bytes_transferred)
{
}
void Server::read_replconf_responses(int remaining)
{
    if (remaining <= 0) {
        SPDLOG_INFO("Step [{}]: REPLCONF capa successful, moving to next step", step_name());
        _handshake_step = ReplicationStep::Psync;
        send_psync();
        return;
    }

    SPDLOG_INFO("Step [{}]: Waiting for REPLCONF response, {} remaining...", step_name(), remaining);
    auto self = shared_from_this();
    boost::asio::async_read(_master_socket, boost::asio::buffer(_read_buffer, 5),
        [this, self, remaining](boost::system::error_code ec, std::size_t length) {
            std::cout << "read" << std::endl;
            if (!ec) {
                std::string response(_read_buffer.data(), length);

                if (response.find("+OK") != std::string::npos) {
                    read_replconf_responses(remaining - 1);
                } else {
                    SPDLOG_ERROR("Unexpected REPLCONF response, retrying. Received: {}", response);
                    _replication_timer.expires_after(std::chrono::seconds(1));
                    _replication_timer.async_wait([this, remaining, self](boost::system::error_code ec) {
                        if (!ec) {
                            read_replconf_responses(remaining - 1);
                        }
                    });
                }
            } else {
                SPDLOG_ERROR("Error reading REPLCONF response: {}", ec.message());
            }
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
            response << "connected_slaves:0\r\n";
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
    collection_server();
    collection_system();
}

void Server::collection_server()
{
    _repl_id.reserve(40);
    _repl_id = generate_id();
    _repl_offset = "0";
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