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
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <cstring>
#include <string>
#include <atomic>

#include "UDP_file_transfer.h"
#include "concurrentqueue.h"
#include "redis_agent.h"
#include "pkt_headers.h"
#include "socket.h"
#include "crc64.h"


using namespace sw::redis;

#define SCAN_BATCH 400
#define MSET_BATCH 100
#define PIPELINE_COUNT 200
#define REQ_PIPELINE 100

#define MIGR_PIPELINE 100
#define MIGRATE_SIZE_THRESHOLD 500 // set cmds number
#define MIGRATE_TIME_THRESHOLD 1 // seconds
#define MIGRATE_COMPLETE_SIZE 21 // 200 // set cmds number
#define REDIS_TIME_THRESHOLD 2

#define MILLION 1000000

#define MAX_SIZE_EXP 17

// Control client op and migration op CPU occupation 
#define SRC_THRESHOLD_MIGRATION 30000 // per thread
#define SRC_THRESHOLD_CLIENT 14000 // 28000 // 30000 // per thread
#define DST_THRESHOLD_MIGRATION 30000 // per thread
#define DST_THRESHOLD_CLIENT 70000 // per thread, not used
#define CHECK_PERIOD 1000 // milliseconds

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

struct StorageConfig {
  StorageConfig(uint16_t src_port, uint16_t dst_port): src_port(src_port), dst_port(dst_port) { }
    uint16_t src_port;
    uint16_t dst_port;
};

struct AgentConfig {
  AgentConfig(std::string src_trans_ip, 
            std::string dst_trans_ip,
            uint16_t src_start_port,
            uint16_t dst_start_port,
            uint16_t src_kv_start_port,
            uint16_t dst_kv_start_port,
            uint32_t migr_thread_num = 1,
            uint32_t migr_pkt_thread_num = 1,
            uint32_t req_thread_num = 1,
            uint32_t redis_scale_num = 2,
            uint16_t client_start_port = 9000): 
            src_trans_ip(std::move(src_trans_ip)),
            dst_trans_ip(std::move(dst_trans_ip)),
            src_start_port(src_start_port),
            dst_start_port(dst_start_port),
            src_kv_start_port(src_kv_start_port),
            dst_kv_start_port(dst_kv_start_port),
            migr_thread_num(migr_thread_num),
            migr_pkt_thread_num(migr_pkt_thread_num),
            req_thread_num(req_thread_num),
            redis_scale_num(redis_scale_num),
            client_start_port(client_start_port) { }
  
    std::string src_trans_ip;
    uint16_t src_start_port;
    std::string dst_trans_ip;
    uint16_t dst_start_port;
    uint16_t src_kv_start_port;
    uint16_t dst_kv_start_port;
    uint16_t migr_thread_num;
    uint32_t migr_pkt_thread_num;
    uint32_t req_thread_num;
    uint32_t redis_scale_num;

    uint32_t client_start_port;
};


struct dst_cmd_q {
    std::string op;
    std::string key;
    std::string val;
};


class MigrationManager {

public:
  MigrationManager(StorageConfig storage_config, AgentConfig agent_config, std::string server_name)
      : storage_config(storage_config), agent_config(agent_config), server_name(server_name) {

    crc64_init();

    group_start_pkt_sent.push_back(std::vector<uint32_t>());
    group_start_pkt_thread.push_back(std::vector<bool>());
    group_complete_pkt_sent.push_back(std::vector<uint32_t>());
    group_complete_pkt_thread.push_back(std::vector<bool>());
    for (uint32_t partition = 0; partition < agent_config.migr_thread_num; partition++) {
      group_start_pkt_sent.at(0).push_back(0);
      group_start_pkt_thread.at(0).push_back(false);
      group_complete_pkt_sent.at(0).push_back(0);
      group_complete_pkt_thread.at(0).push_back(false);
    }
    migration_step.push_back(0);
    migration_size_exp_vec.push_back(0);

    if (server_name == "destination") { 
      // destination migration agent 
      server_type = 1;
      dst_redis = std::make_shared<RedisAgent>("127.0.0.1", storage_config.dst_port, agent_config.redis_scale_num);
      // receiver = std::make_shared<Receiver>(agent_config.dst_start_port, agent_config.migr_pkt_thread_num);
      ipc_server_socket = std::make_shared<ServerSocket>(12345);

      std::vector<uint16_t> tmp;
      tmp.push_back(storage_config.dst_port);
      dst_migr_agent = std::make_shared<ShardAgent>("127.0.0.1", tmp, agent_config.migr_thread_num);
    
      std::cout << "Migration threads listen to UDP ports: ";
      for (int i = agent_config.dst_kv_start_port; i < agent_config.dst_kv_start_port + agent_config.migr_pkt_thread_num; i++) {
        std::cout << i << " ";
        server_sockets.emplace_back(std::make_shared<ServerSocket>(i));
      }
      std::cout << std::endl;


    }
    else if (server_name == "source") { // source background migration push
        server_type = 0;
        src_redis = std::make_shared<RedisAgent>("127.0.0.1", storage_config.src_port, agent_config.redis_scale_num);
        // sender = std::make_shared<Sender>(agent_config.dst_trans_ip, agent_config.dst_start_port, agent_config.src_start_port, agent_config.migr_pkt_thread_num);
        ipc_client_socket = std::make_shared<BlockingClientSocket>(agent_config.dst_trans_ip.c_str(), 12345, 12346);

        std::vector<uint16_t> tmp;
        tmp.push_back(storage_config.src_port);
        src_migr_agent = std::make_shared<ShardAgent>("127.0.0.1", tmp, agent_config.migr_thread_num);

        std::cout << "Migration threads connect to UDP ports: ";
        for (int i = 0; i < agent_config.migr_pkt_thread_num; i ++) {
          std::cout << agent_config.dst_kv_start_port + i << " ";
          client_sockets.emplace_back(std::make_shared<BlockingClientSocket>(agent_config.dst_trans_ip.c_str(), agent_config.dst_kv_start_port + i, agent_config.src_kv_start_port + i));
        }
        std::cout << std::endl;

        src_req_agent = std::make_shared<ShardAgent>("127.0.0.1", tmp, agent_config.redis_scale_num); // For PriorityPull and source request handler
        // src_redis_agent = std::make_shared<RedisAgent>("127.0.0.1", storage_config.src_port, agent_config.redis_scale_num); // For PriorityPull and source request handler
        redis_ports.push_back(storage_config.src_port);
        
        /*
        int i = 0;
        for (auto & port : storage_config.src_port_list) {
          src_port_idx[port] = i++;
        }
        */
        src_port_idx[storage_config.src_port] = 0;

        std::cout << "Source Request Agent listens to UDP ports: ";
        // Request agents continue to use migration agent start port + thread_num
        for (int i = agent_config.client_start_port; i < agent_config.client_start_port + agent_config.req_thread_num; i++) { 
          std::cout << i << " ";
          server_sockets.emplace_back(std::make_shared<ServerSocket>(i));
          send_src_exit_signal.emplace_back(false);
        }
        std::cout << std::endl;

        for (int i = 0; i < VAL_SIZE; i++)
          val_not_found[i] = 0xFF;

        op_queue.emplace(storage_config.src_port, moodycamel::ConcurrentQueue<cmd_q>());

    } 
    std::cout << "MigrationManager Initilization Succeed" << std::endl;
  }

  ~MigrationManager() { 
      ipc_client_socket->ClientSocketClose();
  }

  void SourceMigrationManager();
  void DestinationMigrationManager();

  void AOFParser(std::string aof_file_path, std::vector<dst_cmd_q> & new_commands, int offset);

  // void TestReceiveFile();
  // void TestSendFile();

private:

  std::string getFilePath(std::string type);
  
  void SourceSendLog(std::atomic_int & done_send);

  void ScanKVPairsThread(unsigned long size_exp, uint16_t dbPort, uint32_t thread_id);
  int EnqueueDataPkt(scan_reply res, uint16_t dbPort);
  void EnqueueGroupCtrlPkt(bool start, uint32_t group_id, uint16_t dbPort);
  void SourceSendPktThread(std::atomic_int & done_scan, int thread_id);
  void EnqueueDataPktLog(dst_cmd_q * insert_pairs, int count);

  void SourceRequestHandlerThread(uint32_t thread_id);
  void SourceRedisHandlerThread(uint32_t thread_id);

  void DestinationReceivePktThread(int thread_id); 
  void InsertKVPairsThread(uint32_t thread_id);

  bool check_late_group_id(uint16_t dbPort, uint32_t group_id);
  uint32_t get_group_id(uint16_t dbPort, char * key_pkt);

  std::string server_name;
  bool server_type; 

  StorageConfig storage_config;
  AgentConfig agent_config;

  std::shared_ptr<RedisAgent> dst_redis;
  std::shared_ptr<RedisAgent> src_redis;
  std::shared_ptr<ShardAgent> src_req_agent; // for source prioritypull server

  std::unordered_map<uint16_t, size_t> src_port_idx;

  std::ifstream inFile;

  // std::shared_ptr<Sender> sender;
  // std::shared_ptr<Receiver> receiver;
  std::atomic_int mg_term_flag = 0;
  
  std::shared_ptr<ServerSocket> ipc_server_socket;
  std::shared_ptr<BlockingClientSocket> ipc_client_socket;
  moodycamel::ConcurrentQueue<std::string> file_q;
  std::vector<std::thread> thread_redis;
  std::vector<std::thread> thread_migr;
  moodycamel::ConcurrentQueue<struct mg_data> pkt_buffer[1];
  moodycamel::ConcurrentQueue<kv_pair_string> kv_buffer[1];

  std::vector<std::thread> threads_migr;
  std::vector<std::thread> threads_req;

  std::vector<std::shared_ptr<ServerSocket>> server_sockets;
  std::vector<std::shared_ptr<BlockingClientSocket>> client_sockets;

  std::shared_ptr<ShardAgent> dst_migr_agent;
  std::shared_ptr<ShardAgent> src_migr_agent;
  
  std::atomic_int migr_term = false; 
  std::vector<uint16_t> redis_ports;
  std::map<int, moodycamel::ConcurrentQueue<cmd_q>> op_queue;
  char val_not_found[VAL_SIZE];

  std::shared_ptr<RedisAgent> src_redis_agent;
  std::atomic_bool exit_flag = false;
  std::vector<bool> send_src_exit_signal;
  moodycamel::ConcurrentQueue<dst_cmd_q> log_cmds;

  std::vector<std::vector<uint32_t>> group_start_pkt_sent;
  std::vector<std::vector<bool>> group_start_pkt_thread;
  std::vector<std::vector<uint32_t>> group_complete_pkt_sent;
  std::vector<std::vector<bool>> group_complete_pkt_thread;
  std::vector<uint32_t> migration_step;
  std::vector<uint32_t> migration_size_exp_vec;

  bool migration_start = false;
  bool migration_finish = false;
  std::atomic<unsigned long long> extra_source_bandwidth = 0;
  std::atomic<long long> migration_op = 0;
  std::atomic<unsigned long long> total_migration_op = 0;
  std::atomic<unsigned long long> total_migration_time = 0;
  std::atomic<unsigned long long> total_req_op = 0;
  std::atomic<unsigned long long> total_req_time = 0;
};


