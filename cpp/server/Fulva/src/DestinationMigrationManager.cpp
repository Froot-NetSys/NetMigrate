#include <iostream>
#include <string>
#include <mutex>
#include <random>

#include "MigrationManager.h"

#define DEBUG 0
#define LEVEL_2_DEBUG 0

std::mutex iomutex;
std::mutex insert_mutex;
std::mutex MCR_update_mutex;
std::vector<std::mutex> mcr_mutex(MAX_THREAD_NUM);


// Destination Agent

void MigrationManager::DestinationReceivePktThread(std::atomic_int & mg_term, int thread_id) {

  char buf[BUFSIZE];
  struct mg_hdr_t mg_hdr;
  struct mg_data mg_data;
  struct mg_group_ctrl mg_group_ctrl;
  struct mg_ctrl mg_ctrl;

  char buf_reply[BUFSIZE];
  struct mg_hdr_t mg_hdr_reply;
  int recv_pkts = 0;
  uint32_t mcr_begin[MAX_THREAD_NUM];
  size_t mcr_size = 0;

  char key_pkt[KEY_SIZE + 1];
  char val_pkt[VAL_SIZE + 1];

  kv_pair_string kv_payload[MAX_KV_NUM];
  std::string tmp_key;

  while (mg_term < storage_config.dst_port_list.size()) { // Loop until migration_termination pkt received by one of the destination threads
    server_sockets[thread_id]->Recv(buf); 
    DeserializeMgHdr(buf, &mg_hdr);
    recv_pkts++;
  
    switch (mg_hdr.op) {
      case _MG_INIT:
        DeserializeMgCtrlPkt(buf, &mg_ctrl);
#if DEBUG == 1
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "Thread " << thread_id << ": received _MG_INIT packet, Migration pair: " \
                  << mg_ctrl.srcAddr << " " << mg_ctrl.srcPort << " " << mg_ctrl.dstAddr << " " << mg_ctrl.dstPort << std::endl;
        }
        
#endif 
        mg_hdr_reply.op = _MG_INIT_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port_list[dst_port_idx.at(mg_hdr.dbPort)];
        SerializeMgHdr(buf_reply, &mg_hdr_reply);
        server_sockets[thread_id]->Send(buf_reply, HDR_SIZE);
        break;

      case _MG_TERMINATE:
        DeserializeMgCtrlPkt(buf, &mg_ctrl);
#if DEBUG == 1
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "Thread " << thread_id << ": received _MG_TERMINATE packet, Migration pair: " \
                  << mg_ctrl.srcAddr << " " << mg_ctrl.srcPort << " " << mg_ctrl.dstAddr << " " << mg_ctrl.dstPort << std::endl;
        }
       
#endif 
       
        mg_hdr_reply.op = _MG_TERMINATE_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port_list[dst_port_idx.at(mg_hdr.dbPort)];
        SerializeMgHdr(buf_reply, &mg_hdr_reply);
        server_sockets[thread_id]->Send(buf_reply, HDR_SIZE);

        mg_term++;
        break;

      case _MG_MIGRATE: 
        DeserializeMgDataPkt(buf, &mg_data);
        for (int i = 0; i < mg_data.kv_num; i++) {
          DeserializeKey(key_pkt, mg_data.kv_payload[i].key);
          DeserializeVal(val_pkt, mg_data.kv_payload[i].val);
          kv_payload[i] = std::make_pair(std::string(key_pkt), std::string(val_pkt));
        }

        kv_buffer[dst_port_idx[mg_hdr.dbPort]].enqueue_bulk(kv_payload, mg_data.kv_num);      

#if DEBUG == 1 && LEVEL_2_DEBUG == 1
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "Thread " << thread_id << ": received _MG_MIGRATE packet" << std::endl;
        }
        
#endif 

        /*
        tmp_key = "mg_migrate+" + std::to_string(thread_id);
        kv_buffer[dst_port_idx[mg_hdr.dbPort]].enqueue(std::make_pair(tmp_key, std::to_string(mg_hdr.seq)));
        */
        
        mg_hdr_reply.op = _MG_MIGRATE_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port_list[dst_port_idx.at(mg_hdr.dbPort)];
        SerializeMgHdr(buf_reply, &mg_hdr_reply);
        server_sockets[thread_id]->Send(buf_reply, HDR_SIZE);
        
        break;

      case _MG_MIGRATE_GROUP_START:
        DeserializeMgGroupCtrlPkt(buf, &mg_group_ctrl);
#if DEBUG == 1
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "Thread " << thread_id << ": received _MG_MIGRATE_GROUP_START packet, Group id = " << mg_group_ctrl.group_id << std::endl;
        }
        
#endif
        /*
        tmp_key = "mg_migrate_group_start+" + std::to_string(thread_id);
        kv_buffer[dst_port_idx[mg_hdr.dbPort]].enqueue(std::make_pair(tmp_key, std::to_string(mg_hdr.seq)));
        */

        mg_hdr_reply.op = _MG_MIGRATE_GROUP_START_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port_list[dst_port_idx.at(mg_hdr.dbPort)];
        SerializeMgHdr(buf_reply, &mg_hdr_reply);
        server_sockets[thread_id]->Send(buf_reply, HDR_SIZE);
        
        break;

      case _MG_MIGRATE_GROUP_COMPLETE: // a bucket finished here for Fulva
        DeserializeMgGroupCtrlPkt(buf, &mg_group_ctrl);
/*
        // Fulva: update P_i in MCR 
        for (int i = 0; i < MCR[dst_port_idx[mg_hdr.dbPort]].size(); i++) {
          uint32_t bucket_id = ((mg_group_ctrl.group_id) << remain_size_exp[dst_port_idx[mg_hdr.dbPort]]) + (1 << remain_size_exp[dst_port_idx[mg_hdr.dbPort]]) - 1;
          if (i == MCR[dst_port_idx[mg_hdr.dbPort]].size() - 1 && bucket_id >= MCR_begin[dst_port_idx[mg_hdr.dbPort]][i] || \
              bucket_id >= MCR_begin[dst_port_idx[mg_hdr.dbPort]][i] && bucket_id < MCR_begin[dst_port_idx[mg_hdr.dbPort]][i+1]) { // TODO: group_id << remain_size_exp, can be tuned together with source
             {
              const std::lock_guard<std::mutex> lock(mcr_mutex[dst_port_idx[mg_hdr.dbPort]]);
              MCR[dst_port_idx[mg_hdr.dbPort]][i] = bucket_id; // TODO: group_id << remain_size_exp, can be tuned together with source
             }
             break;
          }
        }
*/ 
#if DEBUG == 1
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "Thread " << thread_id << ": received _MG_MIGRATE_GROUP_COMPLETE packet, Group id = " << mg_group_ctrl.group_id << std::endl;
        }
#endif 

        tmp_key = "mg_migrate_group_complete+" + std::to_string(thread_id) + std::string("++") + std::to_string(mg_group_ctrl.group_id);
        kv_buffer[dst_port_idx[mg_hdr.dbPort]].enqueue(std::make_pair(tmp_key, std::to_string(mg_hdr.seq)));
        
        mg_hdr_reply.op = _MG_MIGRATE_GROUP_COMPLETE_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port_list[dst_port_idx.at(mg_hdr.dbPort)];
        SerializeMgHdr(buf_reply, &mg_hdr_reply);
        server_sockets[thread_id]->Send(buf_reply, HDR_SIZE);
        
        break;
      
      case FULVA_MCR_BEGIN:
        DeserializeMCRBeginPkt(buf, mcr_begin, agent_config.migr_thread_num, &remain_size_exp[dst_port_idx[mg_hdr.dbPort]]);
        mcr_size = agent_config.migr_thread_num;
        for (int i = 0; i < mcr_size; i++) {
          MCR_begin[dst_port_idx[mg_hdr.dbPort]][i] = mcr_begin[i];
        }


        {
#if DEBUG == 1          
          std::cout << "dbPort = " << mg_hdr.dbPort << " mcr_size = " << mcr_size << std::endl;
          for (int i = 0; i < mcr_size; i++) {
            std::cout << MCR_begin[dst_port_idx[mg_hdr.dbPort]][i] << " ";
          }
          std::cout << std::endl;
#endif
        }
 

        mg_hdr_reply.op = FULVA_MCR_BEGIN_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port_list[dst_port_idx.at(mg_hdr.dbPort)];
        SerializeMgHdr(buf_reply, &mg_hdr_reply);
        server_sockets[thread_id]->Send(buf_reply, HDR_SIZE);
              
        break;

      default:
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "DestinationReceivePktThread " << thread_id << ": unknown op field in packet" << std::endl;
        }
    }
  }

  {
    const std::lock_guard<std::mutex> lock(iomutex);
    std::cout << "DestinationReceivePktThread " << thread_id << ": received " << recv_pkts << " packets" << std::endl;
  }
}

void MigrationManager::InsertKVPairsThread(std::atomic_int & mg_term, uint32_t thread_id) {

  char buf_reply[BUFSIZE];
  struct mg_hdr_t mg_hdr_reply;

  kv_pair_string insert_pairs[MSET_BATCH];
  size_t buf_size = 0;
    for (int i = 0; i < dst_port_idx.size(); i++) {
      buf_size += kv_buffer[i].size_approx();
    }
    for (int i = 0; i < dst_port_idx.size(); i++) {
      buf_size += replay_buffer[i].size_approx();
    }

  size_t dequeued_count = 0;
  uint16_t dbPort = 0; 
  std::unordered_map<uint16_t, int> pipe_count;
  for (auto & port : storage_config.dst_port_list) {
    pipe_count.emplace(port, 0);
  }

  int result = 0;
  int insert_key_total = 0;

  std::vector<uint64_t> migration_op(storage_config.dst_port_list.size(), 0);
  auto migration_timer = std::chrono::high_resolution_clock::now();

  while (mg_term < storage_config.dst_port_list.size() || buf_size) {
    dequeued_count = 0;
    dbPort = 0;
    for (int i = thread_id % dst_port_idx.size(); i < dst_port_idx.size(); i++) {
      if (replay_buffer[i].size_approx()) {
        dequeued_count = replay_buffer[i].try_dequeue_bulk(insert_pairs, MSET_BATCH);
        if (dequeued_count) {
          dbPort = storage_config.dst_port_list[i];
          break;
        }
      }
    }
    if (dequeued_count == 0) {
      for (int i = 0; i < thread_id % dst_port_idx.size(); i++) {
        if (replay_buffer[i].size_approx()) {
          dequeued_count = replay_buffer[i].try_dequeue_bulk(insert_pairs, MSET_BATCH);
          if (dequeued_count) {
            dbPort = storage_config.dst_port_list[i];
            break;
          }
        }
      }
    }

    if (dequeued_count == 0) {
      for (int i = thread_id % dst_port_idx.size(); i < dst_port_idx.size(); i++) {
        dequeued_count = kv_buffer[i].try_dequeue_bulk(insert_pairs, MSET_BATCH);
        if (dequeued_count) {
          dbPort = storage_config.dst_port_list[i];
          break;
        }
      }
      if (dequeued_count == 0) {
        for (int i = 0; i < thread_id % dst_port_idx.size(); i++) 
        {
          dequeued_count = kv_buffer[i].try_dequeue_bulk(insert_pairs, MSET_BATCH);
          if (dequeued_count) {
            dbPort = storage_config.dst_port_list[i];
            break;
          }
        }
      }
    }

    if (dequeued_count != 0) {
      std::vector<kv_pair_string> insert_pairs_vec;
      insert_pairs_vec.clear();
      uint32_t reply_thread_id;
      for (uint32_t i = 0; i < dequeued_count; i++) {
        /*
        if (insert_pairs[i].first.find("mg_migrate+") != std::string::npos) {
          mg_hdr_reply.op = _MG_MIGRATE_REPLY;
          mg_hdr_reply.seq = std::stoi(insert_pairs[i].second);
          mg_hdr_reply.dbPort = dbPort;
          SerializeMgHdr(buf_reply, &mg_hdr_reply);
          reply_thread_id = std::stoi(insert_pairs[i].first.substr(insert_pairs[i].first.find("mg_migrate+") + 11));
          // std::cout << "reply_thread_id = " << reply_thread_id << " " << "dbPort = " << dbPort << " seq = " << mg_hdr_reply.seq <<  std::endl;
          server_sockets[reply_thread_id]->Send(buf_reply, HDR_SIZE);
        }
        else if (insert_pairs[i].first.find("mg_migrate_group_start+") != std::string::npos) {
          mg_hdr_reply.op = _MG_MIGRATE_GROUP_START_REPLY;
          mg_hdr_reply.seq = std::stoi(insert_pairs[i].second);
          mg_hdr_reply.dbPort = dbPort;
          SerializeMgHdr(buf_reply, &mg_hdr_reply);
          reply_thread_id = std::stoi(insert_pairs[i].first.substr(insert_pairs[i].first.find("mg_migrate_group_start+") + 23));
          // std::cout << "reply_thread_id = " << reply_thread_id << " " << "dbPort = " << dbPort << " seq = " << mg_hdr_reply.seq <<  std::endl;
          server_sockets[reply_thread_id]->Send(buf_reply, HDR_SIZE);
        }
        else 
        */
        if (insert_pairs[i].first.find("mg_migrate_group_complete+") != std::string::npos) {

          // Fulva: update P_i in MCR 
          uint32_t group_id = std::stoi(insert_pairs[i].first.substr(insert_pairs[i].first.find("++") + 2));
          for (int idx = 0; idx < MCR[dst_port_idx[dbPort]].size(); idx++) {
            uint32_t bucket_id = ((group_id) << remain_size_exp[dst_port_idx[dbPort]]) + (1 << remain_size_exp[dst_port_idx[dbPort]]) - 1;
            if (idx == MCR[dst_port_idx[dbPort]].size() - 1 && bucket_id >= MCR_begin[dst_port_idx[dbPort]][idx] || \
                bucket_id >= MCR_begin[dst_port_idx[dbPort]][idx] && bucket_id < MCR_begin[dst_port_idx[dbPort]][idx+1]) { // TODO: group_id << remain_size_exp, can be tuned together with source
              {
                const std::lock_guard<std::mutex> lock(mcr_mutex[dst_port_idx[dbPort]]);
                MCR[dst_port_idx[dbPort]][idx] = bucket_id; // TODO: group_id << remain_size_exp, can be tuned together with source
              }
              break;
            }
          }

#if DEBUG == 1
          std::cout << "MCR_begin = ";
          for (int idx = 0; idx < MCR_begin[dst_port_idx[dbPort]].size(); idx++) {
            std::cout << MCR_begin[dst_port_idx[dbPort]][idx] << " ";
          }
          std::cout << std::endl;
          std::cout << "MCR = ";
          for (int idx = 0; idx < MCR[dst_port_idx[dbPort]].size(); idx++) {
            std::cout << MCR[dst_port_idx[dbPort]][idx] << " ";
          }
          std::cout << std::endl;
#endif 

          /*
          mg_hdr_reply.op = _MG_MIGRATE_GROUP_COMPLETE_REPLY;
          mg_hdr_reply.seq = std::stoi(insert_pairs[i].second);
          mg_hdr_reply.dbPort = dbPort;
          SerializeMgHdr(buf_reply, &mg_hdr_reply);
          reply_thread_id = std::stoi(insert_pairs[i].first.substr(insert_pairs[i].first.find("mg_migrate_group_complete+") + 26, insert_pairs[i].first.find("++") - 1));
          // std::cout << "group_id = " << group_id << " reply_thread_id = " << reply_thread_id << " " << "dbPort = " << dbPort << " seq = " << mg_hdr_reply.seq <<  std::endl;
          server_sockets[reply_thread_id]->Send(buf_reply, HDR_SIZE);
          */
        }
        else 
          insert_pairs_vec.emplace_back(insert_pairs[i]);
      }

      dequeued_count = insert_pairs_vec.size();

      if (dequeued_count > 0) {
        while (true) {
          auto end = std::chrono::high_resolution_clock::now();
          auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - migration_timer);
          if (time_elapsed.count() >= CHECK_PERIOD) {
            std::fill(migration_op.begin(), migration_op.end(), 0);
            migration_timer = std::chrono::high_resolution_clock::now();
          }

          // if (migration_op.at(dst_port_idx[dbPort]) < DST_THRESHOLD_MIGRATION) 
          {
            insert_key_total += dequeued_count;
        
            // Pipeline command:
            auto migration_timer = std::chrono::high_resolution_clock::now();
            dst_migr_agent->shard.at(dbPort)->pipe.at(thread_id)->mset(insert_pairs_vec.begin(), insert_pairs_vec.end());
            auto migration_timer_end = std::chrono::high_resolution_clock::now();
            total_migration_time += std::chrono::duration_cast<std::chrono::microseconds>(migration_timer_end - migration_timer).count();
             
            pipe_count.at(dbPort)++;

            if (pipe_count.at(dbPort) == PIPELINE_COUNT) {
              auto migration_timer = std::chrono::high_resolution_clock::now();
              try {
                auto replies = dst_migr_agent->shard.at(dbPort)->pipe.at(thread_id)->exec();
              } catch (const ReplyError &err) {
                std::cout << err.what() << std::endl;
              }
              auto migration_timer_end = std::chrono::high_resolution_clock::now();
              total_migration_time += std::chrono::duration_cast<std::chrono::microseconds>(migration_timer_end - migration_timer).count();

              pipe_count.at(dbPort) = 0;
            }
            break;
          }
        }
      }
    }
    
    buf_size = 0;
    for (int i = 0; i < dst_port_idx.size(); i++) {
      buf_size += kv_buffer[i].size_approx();
    }
    for (int i = 0; i < dst_port_idx.size(); i++) {
      buf_size += replay_buffer[i].size_approx();
    }
  } 

  for (auto & port : storage_config.dst_port_list) {
    if (pipe_count.at(port) > 0) {
      auto migration_timer = std::chrono::high_resolution_clock::now();
      try {
        auto replies = dst_migr_agent->shard.at(port)->pipe.at(thread_id)->exec();
      } catch (const ReplyError &err) {
        std::cout << err.what() << std::endl;
      }
      auto migration_timer_end = std::chrono::high_resolution_clock::now();
      total_migration_time += std::chrono::duration_cast<std::chrono::microseconds>(migration_timer_end - migration_timer).count();
    }
  }

  {
    const std::lock_guard<std::mutex> lock(iomutex);
    printf("InsertKVPairsThread %d: totally insert %d keys\n", thread_id, insert_key_total);
  }
  
}


void MigrationManager::DestinationMigrationManager() {
  total_migration_time = 0;
  std::atomic_int mg_term = 0;
  std::vector<std::thread::native_handle_type> thread_handles;

  uint32_t scale_id = 0;
  for (uint32_t j = 0; j < agent_config.redis_scale_num; j++) {
    for (uint32_t i = 0; i < storage_config.dst_port_list.size(); i++) {
      threads_redis.emplace_back(&MigrationManager::DestinationRedisHandlerThread, this, scale_id, std::ref(mg_term));
      scale_id++;
    }
  }
  
  // Request Agent threads
  for (uint32_t i = 0; i < agent_config.req_thread_num; i++) {
    threads_req.emplace_back(&MigrationManager::DestinationRequestHandlerThread, this, agent_config.migr_pkt_thread_num + i);
  }

  // Migration Agent threads
  // Start Producer and Consumer threads
  auto start = std::chrono::high_resolution_clock::now();
  for (uint32_t i = 0; i < agent_config.migr_pkt_thread_num; i++) {
    // Receive packets from source agent and insert KV pairs into buffer
    threads_migr.emplace_back(&MigrationManager::DestinationReceivePktThread, this, std::ref(mg_term), i);
    thread_handles.emplace_back(threads_migr.back().native_handle());
  }

  for (uint32_t i = 0; i < agent_config.migr_thread_num; i++) {  
    // Mset KV pairs into redis instance
    threads_migr.emplace_back(&MigrationManager::InsertKVPairsThread, this, std::ref(mg_term), i);
    thread_handles.emplace_back(threads_migr.back().native_handle());
  }

  // Waiting for migration agent threads completion
  while (mg_term < storage_config.dst_port_list.size()) ;
  migration_finish = true;

  // sleep for 1ms, and then kill all receiving threads
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  for (uint32_t i = 0; i < agent_config.migr_pkt_thread_num; i++) {
    pthread_cancel(thread_handles.at(i));
  }

  for (uint32_t i = 0; i < threads_migr.size(); i++) {
    threads_migr.at(i).join();
  }

  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
  std::cout << "Destination Migratoin Agent Running Time (ms): " << duration.count() << std::endl;
  std::cout << "total migration op: " << total_migration_op << std::endl;
  std::cout << "total migration op time: " << total_migration_time << std::endl;

  // Waiting for Request agent threads completion; currently use Ctrl-C to terminate request agent threads since we don't know when requests will not come
  for (uint32_t i = 0; i < threads_req.size(); i++) {
    threads_req.at(i).join();
  }

  for (uint32_t i = 0; i < threads_redis.size(); i++) {
    threads_redis.at(i).join();
  }

}


inline std::unordered_map<uint16_t, std::shared_ptr<KeyValueStoreClient>> MigrationManager::InitRPCClient() {

    uint16_t server_port = 50051;
    std::unordered_map<uint16_t, std::shared_ptr<KeyValueStoreClient>> client;
    for (auto & redis_port : storage_config.dst_port_list) {
      auto channel = grpc::CreateChannel(this->agent_config.src_ip + ":"+ std::to_string(server_port), grpc::InsecureChannelCredentials());
      // auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(server_port), grpc::InsecureChannelCredentials());
      std::cout << "gRPC Client Connect to " << this->agent_config.src_ip + ":" + std::to_string(server_port) << std::endl;
      client.emplace(redis_port, std::make_shared<KeyValueStoreClient>(channel));
      server_port++;
    }
    return client;
}

size_t MigrationManager::TryAppendMCR(uint32_t thread_id, uint16_t dbPort, uint32_t * mcr) {
  // change to always update first redis instance's MCR (for replicas)
  dbPort = storage_config.dst_port_list[0];
    
  auto endTime = std::chrono::system_clock::now();
  auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - m_StartTime_MCR.at(thread_id)[dst_port_idx[dbPort]]).count();

  if (elapsed_time >= MCR_TIMEOUT) {
    // piggyback all P_i in MCR to client 
    for (int i = 0; i < MCR[dst_port_idx[dbPort]].size(); i++) {
      mcr[i] = MCR[dst_port_idx[dbPort]][i];
    }
    m_StartTime_MCR.at(thread_id)[dst_port_idx[dbPort]] = std::chrono::system_clock::now();
    return MCR[dst_port_idx[dbPort]].size();
  }
 
  return 0;
}

std::default_random_engine generator;
std::uniform_int_distribution<int> uniform_distr{1, 100};

bool pre(std::pair<uint32_t, std::string> a, std::pair<uint32_t, std::string> b) {
  return a.first > b.first;
}

void MigrationManager::TryAppendSPHMetadata(uint32_t thread_id, std::shared_ptr<KeyValueStoreClient> client, uint16_t dbPort, struct sph_metadata * sph_md) {
  // change to always update first redis instance's MCR (for replicas)
  dbPort = storage_config.dst_port_list[0];
  
  auto endTime = std::chrono::system_clock::now();
  auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - m_StartTime_Samp.at(thread_id)[dst_port_idx[dbPort]]).count();
  sph_md->num = 0;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  kv_pair_string key_values[KEY_PAYLOAD_SIZE];

  auto & samp = sampling_pool.at(thread_id).at(dst_port_idx[dbPort]);

  if (samp.size() == 0) 
    return ;

  if (elapsed_time >= SAMPLING_PULL_TIMEOUT || samp.size() >= KEY_PAYLOAD_SIZE) {
    uint8_t num = 0;
    char key_pkt[KEY_SIZE + 1];
    std::make_heap(samp.begin(), samp.end(), pre);
    // Step 1: get all keys and clear in set
    for (auto iter = samp.begin(); iter != samp.end(); iter++) {
      memcpy(&sph_md->keys[num * KEY_SIZE], (iter->second).c_str(), KEY_SIZE);
      num++;
      keys.emplace_back(iter->second);
      if (num == KEY_PAYLOAD_SIZE) break;
    }
   
    for (int i = 0; i < keys.size(); i++) {
      std::pop_heap(samp.begin(), samp.end());
      samp.pop_back();
    }
   
    sph_md->num = num;

    // Step 2: call SamplingPull (PriorityPull) 
    client->PriorityPull(keys, values); // **TODO**: check with asynchronous pull, whether the following syntax is correct!  
    
    // Step 3: Destination replay values
    uint32_t kv_num = 0;
    for (int i = 0; i < keys.size(); i++) {
      if (values[i] != "-1") {
        key_values[i] = std::make_pair(keys[i], values[i]);
        kv_num++;
      }
    }
    replay_buffer[dst_port_idx[dbPort]].enqueue_bulk(key_values, kv_num); 

    m_StartTime_Samp.at(thread_id).at(dst_port_idx[dbPort]) = std::chrono::system_clock::now();
  }
}

void MigrationManager::DestinationRedisHandlerThread(uint32_t thread_id, std::atomic_int & mg_term) {
    uint32_t redis_id = thread_id % storage_config.dst_port_list.size();
    uint32_t scale_id = thread_id / storage_config.dst_port_list.size();

    char buf[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    uint32_t rand = 0;
    
    struct mg_hdr_t mg_hdr;

    OptionalString val;
    struct mg_read_reply reply_pkt_read;
    struct mg_write_reply reply_pkt_write;

    cmd_q data;
    cmd_q bulk_data[REQ_PIPELINE];
    std::vector<cmd_q> cmds;
    int socket_id;
    uint64_t seq;
    uint8_t ver;

    struct sph_metadata sph_md;
    size_t sph_size = 0;

    size_t mcr_size = 0;
    uint32_t mcr[MAX_THREAD_NUM];
    uint32_t mcr_begin[MAX_THREAD_NUM];

    auto client = InitRPCClient();

    while (true) {
        if (op_queue[storage_config.dst_port_list[redis_id]].size_approx() > 0) {

            // size_t count = op_queue[storage_config.dst_port_list[redis_id]].try_dequeue_bulk(bulk_data, REQ_PIPELINE);
            size_t count = 0;
            for (uint32_t i = 0; i < REQ_PIPELINE; i++) {
              bool res = op_queue[storage_config.dst_port_list[redis_id]].try_dequeue(data);
              if (res) {
                // bulk_data[i] = data;
                auto req_timer = std::chrono::high_resolution_clock::now();
                dst_req_agent->shard.at(storage_config.dst_port_list[redis_id])->AppendCmd(scale_id, data.op, data.key, data.val);
                auto req_timer_end = std::chrono::high_resolution_clock::now(); 
                if (migration_finish == false) {
                  total_req_op += 1;
                  total_req_time += std::chrono::duration_cast<std::chrono::microseconds>(req_timer_end - req_timer).count();
                }
                cmds.push_back(data);
                count++;
              }
              else break;
            }
            /*
            for (uint32_t i = 0; i < count; i++) { 
                std::string op = bulk_data[i].op; // currently only read and write 
                std::string key = bulk_data[i].key;
                std::string val = bulk_data[i].val;
                dst_req_agent->shard.at(storage_config.dst_port_list[redis_id])->AppendCmd(op, key, val);
                cmds.push_back(bulk_data[i]);
            }
            */

            auto req_timer = std::chrono::high_resolution_clock::now();
            auto pipe_replies = dst_req_agent->shard.at(storage_config.dst_port_list[redis_id])->pipe.at(scale_id)->exec();
            auto req_timer_end = std::chrono::high_resolution_clock::now(); 
            if (migration_finish == false) {
              total_req_time += std::chrono::duration_cast<std::chrono::microseconds>(req_timer_end - req_timer).count();
            }
          
            for (uint32_t i = 0; i < cmds.size(); i++) {
                socket_id = cmds[i].socket_id;
                seq = cmds[i].seq;
                ver = cmds[i].ver;
                if (cmds[i].op == "GET") {
                    reply_pkt_read = {0};
                    auto val = pipe_replies.get<OptionalString>(i);

                    mg_hdr.op = _MG_READ_REPLY;
                    mg_hdr.dbPort = storage_config.dst_port_list[redis_id];
                    mg_hdr.seq = seq;
                  
                    if (mg_term ==  storage_config.dst_port_list.size()) {
                      ver |= 128;
                    }

                    reply_pkt_read.ver = ver;
                      

                    if (val.has_value()) {
                        strncpy(reply_pkt_read.val, val.value().c_str(), std::min(VAL_SIZE, (int)val.value().size()));
                        reply_pkt_read.ver = ver | (server_type << 1) | 1; // destination, sucess
                    }
                    else {
                        strncpy(reply_pkt_read.val, val_not_found, VAL_SIZE);
                        reply_pkt_read.ver = ver | (server_type << 1) | 0; // destination, fail (key not found)
                        if (mg_term < storage_config.dst_port_list.size()) {
                          // Sampling rate 1%, hot keys
                          // random number generator
                          rand = uniform_distr(generator);
                          if (rand == 1) {
                            auto & samp = sampling_pool.at(thread_id).at(dst_port_idx[mg_hdr.dbPort]);
                            bool flag = 0;
                            for (size_t j = 0; j < samp.size(); j++) {
                              if (samp[j].second == cmds[i].key) {
                                samp[j].first++;
                                flag = 1;
                                break;
                              }
                            }
                            if (!flag) {
                              samp.push_back(std::make_pair(1, cmds[i].key));
                            }
                          }
                        }
                    }

                    SerializeMgHdr(buf, &mg_hdr);

                    mcr_size = sph_size = 0;
                    if (mg_term < storage_config.dst_port_list.size()) {
                      mcr_size = TryAppendMCR(thread_id, mg_hdr.dbPort, mcr);

                      if (mcr_size != 0) {
                        reply_pkt_read.ver |= 16;
                        SerializeReadReplyPkt(buf, &reply_pkt_read);
                        SerializeReadReplyMCRPkt(buf, mcr, mcr_size);
                      } 
                      else {
                        TryAppendSPHMetadata(thread_id, client.at(mg_hdr.dbPort), mg_hdr.dbPort, &sph_md);
                        sph_size = 0;
                        if (sph_md.num) {
                          reply_pkt_read.ver |= 32;
                          SerializeReadReplyPkt(buf, &reply_pkt_read);
                          SerializeReadReplySPHPkt(buf, &sph_md);
                          sph_size = NUM_SIZE + KEY_PAYLOAD_SIZE;
                          
                        }
                        else 
                          SerializeReadReplyPkt(buf, &reply_pkt_read);
                      }
                    }
                    else 
                      SerializeReadReplyPkt(buf, &reply_pkt_read);

                    server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply) + sph_size + mcr_size * sizeof(*mcr));  
                }
                else { // "SET"
                    auto res = pipe_replies.get<bool>(i);
                    reply_pkt_write = {0};
                    reply_pkt_write.ver = (server_type << 1) | (res > 0); 
                    
                    mg_hdr.op = _MG_WRITE_REPLY;
                    mg_hdr.dbPort = storage_config.dst_port_list[redis_id];
                    mg_hdr.seq = seq;

                    if (mg_term ==  storage_config.dst_port_list.size()) {
                      ver |= 128;
                    }

                    reply_pkt_write.ver = ver;

                    SerializeMgHdr(buf, &mg_hdr);
                    mcr_size = sph_size = 0;
                    if (mg_term < storage_config.dst_port_list.size()) {
                      mcr_size = TryAppendMCR(thread_id, mg_hdr.dbPort, mcr);
                      if (mcr_size != 0) {
                        reply_pkt_write.ver |= 16;
                        SerializeWriteReplyPkt(buf, &reply_pkt_write);
                        SerializeWriteReplyMCRPkt(buf, mcr, mcr_size);
                      } 
                      else {
                        TryAppendSPHMetadata(thread_id, client.at(mg_hdr.dbPort), mg_hdr.dbPort, &sph_md);
                        sph_size = 0;
                        if (sph_md.num) {
                          reply_pkt_write.ver |= 32;
                          SerializeWriteReplyPkt(buf, &reply_pkt_write);
                          SerializeWriteReplySPHPkt(buf, &sph_md);
                          sph_size = NUM_SIZE + KEY_PAYLOAD_SIZE;
                        }
                        else 
                          SerializeWriteReplyPkt(buf, &reply_pkt_write);
                      }
                    }
                    else 
                      SerializeWriteReplyPkt(buf, &reply_pkt_write);

                    server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_write_reply) + sph_size + mcr_size * sizeof(*mcr));
                }
            }
            std::vector<cmd_q>().swap(cmds);
        }
    }
}

void MigrationManager::DestinationRequestHandlerThread(uint32_t thread_id) {
    char buf[BUFSIZE], buf_reply[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    uint32_t recvPkt = 0;
    struct mg_hdr_t mg_hdr;

    OptionalString val;
    std::vector<Optional<std::string>> values;
    std::vector<kv_pair_string> key_values;
    std::vector<std::string> keys;
    struct mg_read pkt_read;
    struct mg_read_reply reply_pkt_read;
    struct mg_write pkt_write;
    struct mg_write_reply reply_pkt_write;
    struct mg_delete pkt_delete;
    struct mg_delete_reply reply_pkt_delete;

    struct mg_multi_read pkt_multi_read;
    struct mg_multi_read_reply reply_pkt_multi_read;
    struct mg_multi_write pkt_multi_write;
    struct mg_multi_write_reply reply_pkt_multi_write;
    struct mg_multi_delete pkt_multi_delete;
    struct mg_multi_delete_reply reply_pkt_multi_delete;

    std::map<uint16_t, std::vector<cmd_q>> op_cmds;
    cmd_q tmp_write;
    cmd_q tmp_read;

    for (auto dbPort : storage_config.dst_port_list) {
      op_cmds.emplace(dbPort, std::vector<cmd_q>());
    }

    long long res = 0;
    int recvlen = 0;

    char key_pkt[KEY_SIZE + 1];
    char val_pkt[VAL_SIZE + 1];

    auto client = InitRPCClient();
    
    while (true) {

        if ((recvlen = server_sockets[thread_id]->Recv(buf)) < 0)
        {
            std::cout << "recvlen = " << recvlen << std::endl;
            continue;
        }
        
        recvPkt++;
        if(recvPkt % MILLION == 0) {
        // if (recvPkt % 1 == 0) {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "RequestHandler thread " << thread_id << ": Recv packet " << recvPkt << ", dbPort " << mg_hdr.dbPort << std::endl;
          std::cout << "total req op = " << total_req_op << std::endl;
          std::cout << "total req op time = " << total_req_time << std::endl;
        }
            

        // Deserialize packet and handle client requests to Redis
        DeserializeMgHdr(buf, &mg_hdr);
    
        switch (mg_hdr.op) {
            case _MG_READ: 
                DeserializeReadPkt(buf, &pkt_read);
                mg_hdr.op = _MG_READ_REPLY;
                DeserializeKey(key_pkt, pkt_read.key);
                
                tmp_read.op = "GET";
                tmp_read.key = std::string(key_pkt);
                tmp_read.val = "";
                tmp_read.socket_id = thread_id;
                tmp_read.seq = mg_hdr.seq;
                tmp_read.ver = pkt_read.ver;
                op_cmds[mg_hdr.dbPort].push_back(tmp_read);
                break;

            case _MG_WRITE:
                DeserializeWritePkt(buf, &pkt_write);
                mg_hdr.op = _MG_WRITE_REPLY;
                DeserializeKey(key_pkt, pkt_write.key);
                strncpy(val_pkt, pkt_write.val, VAL_SIZE);

                tmp_write.op = "SET";
                tmp_write.key = std::string(key_pkt);
                tmp_write.val = std::string(val_pkt);
                tmp_write.socket_id = thread_id;
                tmp_write.seq = mg_hdr.seq;
                tmp_write.ver = 0;
                op_cmds[mg_hdr.dbPort].push_back(tmp_write);
                break;

            /*
            case _MG_DELETE:
                
                DeserializeDeletePkt(buf, &pkt_delete);
                mg_hdr.op = _MG_DELETE_REPLY;
                DeserializeKey(key_pkt, pkt_delete.key);
                res = dst_req_agent->shard.at(mg_hdr.dbPort)->redis->del(std::string(key_pkt));
                reply_pkt_delete = {0};
                reply_pkt_delete.ver = (server_type << 1) | (res > 0); 
                
                SerializeMgHdr(buf, &mg_hdr);
                
                mcr_size = sph_size = 0;
                {
                  mcr_size = TryAppendMCR(thread_id, mg_hdr.dbPort, mcr);
                  if (mcr_size != 0) {
                    reply_pkt_delete.ver |= 16;
                    SerializeDeleteReplyPkt(buf, &reply_pkt_delete);
                    SerializeDeleteReplyMCRPkt(buf, mcr, mcr_size);
                  } 
                  else {
                    TryAppendSPHMetadata(thread_id, client.at(mg_hdr.dbPort), mg_hdr.dbPort, &sph_md);
                    sph_size = 0;
                    if (sph_md.num) {
                      reply_pkt_delete.ver |= 32;
                      SerializeDeleteReplyPkt(buf, &reply_pkt_delete);
                      SerializeDeleteReplySPHPkt(buf, &sph_md);
                      sph_size = NUM_SIZE + KEY_PAYLOAD_SIZE;
                    }
                    else 
                      SerializeDeleteReplyPkt(buf, &reply_pkt_delete);
                  }
                }

                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(mg_delete_reply) + sph_size + mcr_size * sizeof(*mcr));
                break;
            
            case _MG_MULTI_READ: 

                DeserializeMultiReadPkt(buf, &pkt_multi_read);
                reply_pkt_multi_read = {0};
                reply_pkt_multi_read.bitmap = pkt_multi_read.bitmap;
                mg_hdr.op = _MG_MULTI_READ_REPLY;

                // Sampling rate 1%, hot keys
                // random number generator
                rand = uniform_distr(generator);

                // deal with mget 
                keys.clear();
                values.clear();
                for (uint8_t i = 0; i < 4; i++) {
                  if (pkt_multi_read.bitmap & (1 << i)) {
                    DeserializeKey(key_pkt, pkt_multi_read.ver_key[i].key);
                    keys.emplace_back(std::string(key_pkt));
                  }
                }

                dst_req_agent->shard.at(mg_hdr.dbPort)->redis->mget(keys.begin(), keys.end(), std::back_inserter(values));
              
                reply_pkt_multi_read.bitmap = pkt_multi_read.bitmap;

                for (int i = 0, idx = 0; i < values.size(); i++) {
                  val = values[i];
                  while (idx < 4 && (pkt_multi_read.bitmap & (1 << idx)) == 0)
                    idx++;
                  if (idx == 4) break;
                  if (pkt_multi_read.bitmap & (1 << idx)) {
                    if (val) {
                      strncpy(reply_pkt_multi_read.ver_val[idx].val, val.value().c_str(), std::min(VAL_SIZE, (int)val.value().size()));
                      reply_pkt_multi_read.ver_val[idx].ver = pkt_multi_read.ver_key[idx].ver | (server_type << 1) | 1;
                    }
                    else {
                      strncpy(reply_pkt_multi_read.ver_val[idx].val, val_not_found, VAL_SIZE);
                      reply_pkt_multi_read.ver_val[idx].ver = pkt_multi_read.ver_key[idx].ver | (server_type << 1) | 0; // key not exist
                      // Sampling rate 1%, hot keys
                      // random number generator
                      rand = uniform_distr(generator);
                      if (rand == 1) {
                        auto & samp = sampling_pool.at(thread_id - agent_config.migr_thread_num).at(dst_port_idx[mg_hdr.dbPort]);
                        bool flag = 0;
                        for (size_t i = 0; i < samp.size(); i++) {
                          if (samp[i].second == keys[idx]) {
                            samp[i].first++;
                            flag = 1;
                            break;
                          }
                        }
                        if (!flag) {
                          samp.push_back(std::make_pair(1, keys[idx]));
                        }
                      }
                    }
                  }
                }

                SerializeMgHdr(buf, &mg_hdr);

                mcr_size = sph_size = 0;
                {
                  mcr_size = TryAppendMCR(thread_id, mg_hdr.dbPort, mcr);
                  if (mcr_size != 0) {
                    reply_pkt_multi_read.ver_val[0].ver |= 16;
                    SerializeMultiReadReplyPkt(buf, &reply_pkt_multi_read);
                    SerializeMultiReadReplyMCRPkt(buf, mcr, mcr_size);
                  } 
                  else {
                    TryAppendSPHMetadata(thread_id, client.at(mg_hdr.dbPort), mg_hdr.dbPort, &sph_md);
                    sph_size = 0;
                    if (sph_md.num) {
                      reply_pkt_multi_read.ver_val[0].ver |= 32; // use first ver to indicate SPH piggyback
                      SerializeMultiReadReplyPkt(buf, &reply_pkt_multi_read);
                      SerializeMultiReadReplySPHPkt(buf, &sph_md);
                      sph_size = NUM_SIZE + KEY_PAYLOAD_SIZE;
                    }
                    else 
                      SerializeMultiReadReplyPkt(buf, &reply_pkt_multi_read);
                  }
                }
                
                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_multi_read_reply) + sph_size + mcr_size * sizeof(*mcr));
                break;
            
            case _MG_MULTI_WRITE: 
                DeserializeMultiWritePkt(buf, &pkt_multi_write);
                mg_hdr.op = _MG_MULTI_WRITE_REPLY;
                // deal with multi-write
                reply_pkt_multi_write = {0};
                reply_pkt_multi_write.bitmap = pkt_multi_write.bitmap;
                key_values.clear();
                for (uint8_t i = 0; i < 4; i++) {
                  if (pkt_multi_write.bitmap & (1 << i)) {
                    DeserializeKey(key_pkt, &pkt_multi_write.keys[i * KEY_SIZE]);
                    strncpy(val_pkt, &pkt_multi_write.vals[i * VAL_SIZE], VAL_SIZE);
                    key_values.emplace_back(std::make_pair(std::string(key_pkt), std::string(val_pkt)));
                  }
                }
                dst_req_agent->shard.at(mg_hdr.dbPort)->redis->mset(key_values.begin(), key_values.end());
                
                reply_pkt_multi_write.bitmap = pkt_multi_write.bitmap;
                for (uint8_t i = 0; i < 4; i++) {
                  if (pkt_multi_write.bitmap & (1 << i)) {
                    reply_pkt_multi_write.ver[i] = (server_type << 1) | 1;
                  }
                }
                
                SerializeMgHdr(buf, &mg_hdr);

                mcr_size = sph_size = 0;
                {
                  mcr_size = TryAppendMCR(thread_id, mg_hdr.dbPort, mcr);
                  if (mcr_size != 0) {
                    reply_pkt_multi_write.ver[0] |= 16;
                    SerializeMultiWriteReplyPkt(buf, &reply_pkt_multi_write);
                    SerializeMultiWriteReplyMCRPkt(buf, mcr, mcr_size);
                  } 
                  else {
                    TryAppendSPHMetadata(thread_id, client.at(mg_hdr.dbPort), mg_hdr.dbPort, &sph_md);
                    sph_size = 0;
                    if (sph_md.num) {
                      reply_pkt_multi_write.ver[0] |= 32;
                      SerializeMultiWriteReplyPkt(buf, &reply_pkt_multi_write);
                      SerializeMultiWriteReplySPHPkt(buf, &sph_md);
                      sph_size = NUM_SIZE + KEY_PAYLOAD_SIZE;
                    }
                    else 
                      SerializeMultiWriteReplyPkt(buf, &reply_pkt_multi_write);
                  }
                }

                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_multi_write_reply) + sph_size + mcr_size * sizeof(*mcr));
                break;

            case _MG_MULTI_DELETE: 
                DeserializeMultiDeletePkt(buf, &pkt_multi_delete);
                mg_hdr.op = _MG_MULTI_DELETE_REPLY;
                // deal with multi-delete
                reply_pkt_multi_delete = {0};
                reply_pkt_multi_delete.bitmap = pkt_multi_delete.bitmap;
                for (uint8_t i = 0; i < 4; i++) {
                  if (pkt_multi_delete.bitmap & (1 << i)) {
                    DeserializeKey(key_pkt, &pkt_multi_delete.keys[i * KEY_SIZE]);
                    keys.emplace_back(std::string(key_pkt));
                  }
                }
                dst_req_agent->shard.at(mg_hdr.dbPort)->redis->del(keys.begin(), keys.end());
                reply_pkt_multi_delete.bitmap = pkt_multi_delete.bitmap;
                for (uint8_t i = 0; i < 4; i++) {
                  if (pkt_multi_delete.bitmap & (1 << i)) {
                    reply_pkt_multi_delete.ver[i] = (server_type << 1) | 1; // minor TODO: change to del command return value
                  }
                }
                
                SerializeMgHdr(buf, &mg_hdr);

                mcr_size = sph_size = 0;
                {
                  mcr_size = TryAppendMCR(thread_id, mg_hdr.dbPort, mcr);
                  if (mcr_size != 0) {
                    reply_pkt_multi_delete.ver[0] |= 16;
                    SerializeMultiDeleteReplyPkt(buf, &reply_pkt_multi_delete);
                    SerializeMultiDeleteReplyMCRPkt(buf, mcr, mcr_size);
                  } 
                  else {
                    TryAppendSPHMetadata(thread_id, client.at(mg_hdr.dbPort), mg_hdr.dbPort, &sph_md);
                    sph_size = 0;
                    if (sph_md.num) {
                      reply_pkt_multi_delete.ver[0] |= 32;
                      SerializeMultiDeleteReplyPkt(buf, &reply_pkt_multi_delete);
                      SerializeMultiDeleteReplySPHPkt(buf, &sph_md);
                      sph_size = NUM_SIZE + KEY_PAYLOAD_SIZE;
                    }
                    else  
                      SerializeMultiDeleteReplyPkt(buf, &reply_pkt_multi_delete);
                  }
                }

                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_multi_delete_reply) + sph_size + mcr_size * sizeof(*mcr));
                break;
              */
            default:
                std::cout << "Currently not supported request type" << std::endl;
        }   
        for (auto dbPort : storage_config.dst_port_list) {
          if (op_cmds[dbPort].size() % REQ_PIPELINE == 0) {
            op_queue[dbPort].enqueue_bulk(op_cmds[dbPort].data(), op_cmds[dbPort].size());
            std::vector<cmd_q>().swap(op_cmds[dbPort]);
          }
        } 
    }
}