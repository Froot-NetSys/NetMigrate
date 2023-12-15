#pragma once
#include <sw/redis++/redis++.h>
#include <hiredis/hiredis.h>

#include <memory>
#include <thread>
#include <pthread.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <chrono>


#include "pkt_headers.h"
#include "concurrentqueue.h"
#include "socket.h"
#include "redis_agent.h"
#include "rpc.h"
#include "crc64.h"

using namespace sw::redis;

#define MILLION 1000000

// modify for better performance
#define SCAN_BATCH 400 // can be tuned for higher migration speed
#define MSET_BATCH 100
#define PIPELINE_COUNT 200
#define REQ_PIPELINE 100

#define MAX_THREAD_NUM 32  

#define PRIORITY_PULL_BATCH_SIZE 16 // Same as RockSteady
#define PRIORITY_PULL_TIMEOUT 100 // milliseconds
#define PRIORITY_PULL_BUFFER_CLEAR 10000 // milliseconds

#define CLIENT_PORT_START 48704 // Server request agent start port actually
// #define CLIENT_PORT_START 5000 // for debug use => same as source baseline


// Control client op and migration op CPU occupation 
#define SRC_THRESHOLD_MIGRATION 30000 // per thread
#define SRC_THRESHOLD_CLIENT 9000 // 27000 // 9000 // 30000 // 13000 // 14000 // 28000 // 30000 // per thread
#define DST_THRESHOLD_MIGRATION 30000 // per thread
#define DST_THRESHOLD_CLIENT 70000 // per thread, not used
#define CHECK_PERIOD 1000 // milliseconds

#define MAX_SIZE_EXP 17



struct StorageConfig {
  StorageConfig(std::string src_ip, std::vector<uint16_t> &src_port_list, std::string dst_ip, std::vector<uint16_t> &dst_port_list): 
    src_port_list(std::move(src_port_list)), src_ip(std::move(src_ip)), dst_port_list(std::move(dst_port_list)), dst_ip(std::move(dst_ip)) { }

  std::string src_ip;
  std::vector<uint16_t> src_port_list;
  std::string dst_ip;
  std::vector<uint16_t> dst_port_list;
};

struct AgentConfig {
  AgentConfig(std::string src_ip, std::string dst_ip,
              uint16_t src_start_port, uint16_t dst_start_port, uint16_t client_start_port,
              uint32_t migr_thread_num, uint32_t migr_pkt_thread_num, uint32_t req_thread_num, uint32_t grpc_thread_num = 1, uint32_t redis_scale_num = 2):
    src_ip(std::move(src_ip)), dst_ip(std::move(dst_ip)),
    src_start_port(src_start_port), dst_start_port(dst_start_port), client_start_port(client_start_port),
    migr_thread_num(migr_thread_num), migr_pkt_thread_num(migr_pkt_thread_num), req_thread_num(req_thread_num), grpc_thread_num(grpc_thread_num), redis_scale_num(redis_scale_num) { }
  
  std::string src_ip;
  std::string dst_ip;
  uint16_t src_start_port;
  uint16_t dst_start_port;
  uint16_t client_start_port;
  uint32_t migr_thread_num;
  uint32_t migr_pkt_thread_num;
  uint32_t req_thread_num;
  uint32_t grpc_thread_num; // TODO: tunable
  uint32_t redis_scale_num;
};

typedef std::pair<std::string, std::string> kv_pair_string;
typedef std::vector<std::vector<std::string> > scan_reply;

struct cmd_q {
    std::string op;
    std::string key;
    std::string val;
    uint32_t socket_id;
    uint64_t seq;
    uint8_t ver;
};

struct log_cmd_q {
    std::string dbPort;
    std::string key;
    std::string val;
};


class MigrationManager {

public:
  MigrationManager(StorageConfig storage_config, AgentConfig agent_config, std::string server_name)
      : storage_config(storage_config), agent_config(agent_config), server_name(server_name) {
    
    assert(agent_config.migr_thread_num > 0 && (agent_config.migr_thread_num & (agent_config.migr_thread_num - 1)) == 0); 
    // migr agent thread_num must be power of 2

    crc64_init();

    if (server_name == "destination") { 
      // destination migration agent 
      server_type = 1;

      dst_migr_agent = std::make_shared<ShardAgent>(storage_config.dst_ip, storage_config.dst_port_list, agent_config.migr_thread_num);
      int i = 0;
      for (auto & port : storage_config.dst_port_list) {
        dst_port_idx[port] = i++;
      }

      std::cout << "Migration threads listen to UDP ports: ";
      for (int i = agent_config.dst_start_port; i < agent_config.dst_start_port + agent_config.migr_pkt_thread_num; i ++) {
        std::cout << i << " ";
        server_sockets.emplace_back(std::make_shared<ServerSocket>(i));
      }
      std::cout << std::endl;

      // destination request agent
      dst_req_agent = std::make_shared<ShardAgent>(storage_config.dst_ip, storage_config.dst_port_list, agent_config.redis_scale_num); 
      for (uint32_t i = 0; i < agent_config.redis_scale_num; i++) {
        std::unordered_set<std::string> s;
        priority_pull_buffer.emplace_back(s);
        keys.emplace_back();
        m_StartTime_Buffer.emplace_back(std::chrono::system_clock::now());
        m_StartTime_Pull.emplace_back(std::chrono::system_clock::now());
      }

      std::cout << "Destination Request Agent listens to UDP ports: ";
      // Request agents continue to use migration agent start port + thread_num
      for (int i = agent_config.client_start_port; i < agent_config.client_start_port + agent_config.req_thread_num; i++) { 
        std::cout << i << " ";
        server_sockets.emplace_back(std::make_shared<ServerSocket>(i));
      }
      std::cout << std::endl;

      for (uint32_t i = 0; i < dst_port_idx.size(); i++) {
        op_queue.emplace(storage_config.dst_port_list[i], moodycamel::ConcurrentQueue<cmd_q>());
      }
    }
    else if (server_name == "source_push") { // source background migration push
      server_type = 0;
      src_migr_agent = std::make_shared<ShardAgent>(storage_config.src_ip, storage_config.src_port_list, agent_config.migr_thread_num);
      
      int i = 0;
      for (auto & port : storage_config.src_port_list) {
        src_port_idx[port] = i++;
      }

      std::cout << "Migration threads connect to UDP ports: ";
      for (int i = 0; i < agent_config.migr_pkt_thread_num; i ++) {
        std::cout << agent_config.dst_start_port + i << " ";
        client_sockets.emplace_back(std::make_shared<ClientSocket>(agent_config.dst_ip.c_str(), agent_config.dst_start_port + i, agent_config.src_start_port + i));
      }
      std::cout << std::endl;

      ipc_client_socket = std::make_shared<BlockingClientSocket>("127.0.0.1", 12345, 12346);

      std::cout << "Source Request Agent listens to UDP ports: ";
      // Request agents continue to use migration agent start port + thread_num
      for (int i = agent_config.client_start_port; i < agent_config.client_start_port + agent_config.req_thread_num; i++) { 
        std::cout << i << " ";
        server_sockets.emplace_back(std::make_shared<ServerSocket>(i));
      }
      std::cout << std::endl;

      for (uint32_t i = 0; i < src_port_idx.size(); i++) {
        op_queue.emplace(storage_config.src_port_list[i], moodycamel::ConcurrentQueue<cmd_q>());
      }

      src_req_agent = std::make_shared<ShardAgent>(storage_config.src_ip, storage_config.src_port_list, agent_config.redis_scale_num); // For PriorityPull and source request handler


      for (uint32_t i = 0; i < storage_config.src_port_list.size(); i++) {
        group_start_pkt_sent.push_back(std::vector<uint32_t>());
        group_start_pkt_thread.push_back(std::vector<bool>());
        group_complete_pkt_sent.push_back(std::vector<uint32_t>());
        group_complete_pkt_thread.push_back(std::vector<bool>());
        for (uint32_t partition = 0; partition < agent_config.migr_thread_num; partition++) {
          group_start_pkt_sent.at(i).push_back(0);
          group_start_pkt_thread.at(i).push_back(false);
          group_complete_pkt_sent.at(i).push_back(0);
          group_complete_pkt_thread.at(i).push_back(false);
        }
        migration_step.push_back(0);
        migration_size_exp_vec.push_back(0);
      }
      
    } 
    else if (server_name == "source_pull") { // gRPC 
      server_type = 0;
      src_migr_agent = std::make_shared<ShardAgent>(storage_config.src_ip, storage_config.src_port_list, agent_config.migr_thread_num);
      src_req_agent = std::make_shared<ShardAgent>(storage_config.src_ip, storage_config.src_port_list, agent_config.redis_scale_num); // For PriorityPull and source request handler

      int i = 0;
      for (auto & port : storage_config.src_port_list) {
        src_port_idx[port] = i++;
      }

      ipc_server_socket = std::make_shared<ServerSocket>(12345);
    
    }

    for (int i = 0; i < VAL_SIZE; i++)
        val_not_found[i] = 0xFF;

    std::cout << "MigrationManager Initilization Succeed" << std::endl;
  }

  ~MigrationManager() { 
    // According to socket.h, client_socket needs to manually close
    for (int i = 0; i < client_sockets.size(); i ++) {
      client_sockets.at(i)->ClientSocketClose();
    }
  }

  void SourceMigrationManager();
  void DestinationMigrationManager();
  void StartSourceRPCServer();
  void FinishSourceRPCServer();

private:
  void ScanKVPairsThread(unsigned long size_exp, uint16_t dbPort, uint32_t thread_id);
  int EnqueueDataPkt(scan_reply res, uint16_t dbPort);
  void EnqueueGroupCtrlPkt(bool start, uint32_t group_id, uint16_t dbPort);
  void SourceSendPktThread(std::atomic_int & done_scan, int thread_id);
  void RunRPCServer(uint16_t dbPort, uint16_t server_port);

  void DestinationReceivePktThread(std::atomic_int & mg_term, int thread_id); 
  void InsertKVPairsThread(std::atomic_int & mg_term, uint32_t thread_id);

  void DestinationRequestHandlerThread(uint32_t thread_id);
  void DestinationRedisHandlerThread(uint32_t thread_id);
  int PriorityPullStub(std::shared_ptr<KeyValueStoreClient> client, uint16_t dbPort, uint32_t thread_id, std::string key);
  std::unordered_map<uint16_t, std::shared_ptr<KeyValueStoreClient>> InitRPCClient();

  void SourceRequestHandlerThread(uint32_t thread_id);
  void SourceRedisHandlerThread(uint32_t thread_id);
  void EnqueueDataPktLog();

  bool check_late_group_id(uint16_t dbPort, uint32_t group_id);
  bool check_wrong_group_id(uint16_t dbPort, uint32_t group_id);
  uint32_t get_group_id(uint16_t dbPort, char * key_pkt);

  void IPCServer();

  std::string server_name;
  bool server_type; 

  StorageConfig storage_config;
  AgentConfig agent_config;

  std::shared_ptr<ShardAgent> dst_migr_agent;
  std::shared_ptr<ShardAgent> src_migr_agent;

  std::shared_ptr<ShardAgent> dst_req_agent; // for request server agent, redis-plus-plus is thread-safe
  std::shared_ptr<ShardAgent> src_req_agent; // for source prioritypull server
  

  std::vector<std::shared_ptr<ServerSocket>> server_sockets;
  std::vector<std::shared_ptr<ClientSocket>> client_sockets;
 
  std::vector<std::thread> threads_migr;
  std::vector<std::thread> threads_req; // for either source or destination request handlers
  std::vector<std::thread> threads_grpc;
  std::vector<std::thread> threads_redis;
  moodycamel::ConcurrentQueue<kv_pair_string> kv_buffer[MAX_THREAD_NUM];
  moodycamel::ConcurrentQueue<struct mg_data> pkt_buffer[MAX_THREAD_NUM];

  std::vector<std::unordered_set<std::string>> priority_pull_buffer; // each thread has a priority_pull_buffer, which is a pull key list
  std::vector<std::vector<std::string>> keys;
  moodycamel::ConcurrentQueue<kv_pair_string> replay_buffer[MAX_THREAD_NUM]; // also replayed in priority

  std::vector<std::chrono::time_point<std::chrono::system_clock>> m_StartTime_Pull;
  std::vector<std::chrono::time_point<std::chrono::system_clock>> m_StartTime_Buffer;

  std::vector<std::shared_ptr<KeyValueStoreServiceImpl>> grpc_servers;
  std::unordered_map<uint16_t, size_t> src_port_idx;
  std::unordered_map<uint16_t, size_t> dst_port_idx;
  char val_not_found[VAL_SIZE];
  std::map<int, moodycamel::ConcurrentQueue<cmd_q>> op_queue;

  std::shared_ptr<ServerSocket> ipc_server_socket;
  std::shared_ptr<BlockingClientSocket> ipc_client_socket;
  std::set<uint16_t> migr_start_ports;
  std::thread thread_ipc;


  std::vector<std::vector<uint32_t>> group_start_pkt_sent;
  std::vector<std::vector<bool>> group_start_pkt_thread;
  std::vector<std::vector<uint32_t>> group_complete_pkt_sent;
  std::vector<std::vector<bool>> group_complete_pkt_thread;
  std::vector<uint32_t> migration_step;
  std::vector<uint32_t> migration_size_exp_vec;
  moodycamel::ConcurrentQueue<log_cmd_q> log_cmds;
  bool migration_start = false;
  bool migration_finish = false;

  std::atomic<unsigned long long> extra_prioritypull_bandwidth = 0;
  std::atomic<unsigned long long> extra_source_bandwidth = 0;

  // std::chrono::steady_clock::time_point migration_op_start;
  std::atomic<unsigned long long> total_migration_op = 0;
  std::atomic<unsigned long long> total_migration_time = 0;
  std::atomic<unsigned long long> total_req_op = 0;
  std::atomic<unsigned long long> total_req_time = 0;
};
