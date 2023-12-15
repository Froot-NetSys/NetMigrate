#include "MigrationManager.h"

#define DEBUG 0

std::mutex iomutex;
std::mutex insert_mutex;

void MigrationManager::DestinationReceivePktThread(int thread_id) {

  char buf[BUFSIZE];
  struct mg_hdr_t mg_hdr;
  struct mg_data mg_data;
  struct mg_group_ctrl mg_group_ctrl;
  struct mg_ctrl mg_ctrl;

  char buf_reply[BUFSIZE];
  struct mg_hdr_t mg_hdr_reply;
  int recv_pkts = 0;

  char key_pkt[KEY_SIZE + 1];
  char val_pkt[VAL_SIZE + 1];

  kv_pair_string kv_payload[MAX_KV_NUM];

  while (mg_term_flag < 1) { // Loop until migration_termination pkt received by one of the destination threads
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
        mg_hdr_reply.dbPort = storage_config.src_port;

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
        mg_hdr_reply.dbPort = storage_config.src_port;
        SerializeMgHdr(buf_reply, &mg_hdr_reply);
        server_sockets[thread_id]->Send(buf_reply, HDR_SIZE);

        mg_term_flag++;
        break;

      case _MG_MIGRATE: 
        DeserializeMgDataPkt(buf, &mg_data);
        for (int i = 0; i < mg_data.kv_num; i++) {
          DeserializeKey(key_pkt, mg_data.kv_payload[i].key);
          DeserializeVal(val_pkt, mg_data.kv_payload[i].val);
          kv_payload[i] = std::make_pair(std::string(key_pkt), std::string(val_pkt));
        }
        kv_buffer[0].enqueue_bulk(kv_payload, mg_data.kv_num); 
#if DEBUG == 1
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "Thread " << thread_id << ": received _MG_MIGRATE packet" << std::endl;
        }
        
#endif 
        mg_hdr_reply.op = _MG_MIGRATE_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port;

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
        mg_hdr_reply.op = _MG_MIGRATE_GROUP_START_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port;
        SerializeMgHdr(buf_reply, &mg_hdr_reply);
        server_sockets[thread_id]->Send(buf_reply, HDR_SIZE);
        break;

      case _MG_MIGRATE_GROUP_COMPLETE:
        DeserializeMgGroupCtrlPkt(buf, &mg_group_ctrl);
#if DEBUG == 1
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "Thread " << thread_id << ": received _MG_MIGRATE_GROUP_COMPLETE packet, Group id = " << mg_group_ctrl.group_id << std::endl;
        }
        
#endif  

        mg_hdr_reply.op = _MG_MIGRATE_GROUP_COMPLETE_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port;
        SerializeMgHdr(buf_reply, &mg_hdr_reply);
        server_sockets[thread_id]->Send(buf_reply, HDR_SIZE);
        break;

      default:
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "DestinationReceivePktThread " << thread_id << ": wrong op field in packet" << std::endl;
        }
    }
  }

  {
    const std::lock_guard<std::mutex> lock(iomutex);
    std::cout << "DestinationReceivePktThread " << thread_id << ": received " << recv_pkts << " packets" << std::endl;
  }
}

void MigrationManager::InsertKVPairsThread(uint32_t thread_id) {
  kv_pair_string insert_pairs[MSET_BATCH];
  size_t buf_size = kv_buffer[0].size_approx();

  size_t dequeued_count = 0;
  uint16_t dbPort = 0; 
  std::unordered_map<uint16_t, int> pipe_count;
  pipe_count.emplace(storage_config.dst_port, 0);
  

  int result = 0;
  int insert_key_total = 0;

  std::vector<uint64_t> migration_op(1, 0);
  auto migration_timer = std::chrono::high_resolution_clock::now();

  while (mg_term_flag < 1 || buf_size) {
    dequeued_count = 0;
    dbPort = 0;

    dequeued_count = kv_buffer[0].try_dequeue_bulk(insert_pairs, MSET_BATCH);
    dbPort = storage_config.dst_port; 

    if (dequeued_count != 0) {
      while (true) {
        auto end = std::chrono::high_resolution_clock::now();
          auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - migration_timer);
          if (time_elapsed.count() >= CHECK_PERIOD) {
            // std::cout << "migration_op/s=" << migration_op.at(0) << std::endl;
            std::fill(migration_op.begin(), migration_op.end(), 0);
            migration_timer = std::chrono::high_resolution_clock::now();
          }

          // if (migration_op.at(0) < DST_THRESHOLD_MIGRATION) 
          {
            insert_key_total += dequeued_count;
            std::vector<kv_pair_string> insert_pairs_vec(insert_pairs, insert_pairs + dequeued_count);
            // Pipeline command:
            auto migration_timer = std::chrono::high_resolution_clock::now();
            dst_migr_agent->shard.at(dbPort)->pipe.at(thread_id)->mset(insert_pairs_vec.begin(), insert_pairs_vec.end());
            auto migration_timer_end = std::chrono::high_resolution_clock::now();
            total_migration_time += std::chrono::duration_cast<std::chrono::microseconds>(migration_timer_end - migration_timer).count();
            pipe_count.at(dbPort)++;
            migration_op.at(0) += insert_pairs_vec.size();

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

    buf_size = kv_buffer[0].size_approx();
  } 

  
  if (pipe_count.at(storage_config.dst_port) > 0) {
    auto migration_timer = std::chrono::high_resolution_clock::now();
    try {
      auto replies = dst_migr_agent->shard.at(storage_config.dst_port)->pipe.at(thread_id)->exec();
    } catch (const ReplyError &err) {
      std::cout << err.what() << std::endl;
    }
    auto migration_timer_end = std::chrono::high_resolution_clock::now();
    total_migration_time += std::chrono::duration_cast<std::chrono::microseconds>(migration_timer_end - migration_timer).count();
  }

  {
    const std::lock_guard<std::mutex> lock(iomutex);
    printf("InsertKVPairsThread %d: totally insert %d keys\n", thread_id, insert_key_total);
  }
  
}




void MigrationManager::DestinationMigrationManager() {
  mg_term_flag = 0;
  std::vector<std::thread::native_handle_type> thread_handles;

  // Migration Agent threads
  // Start Producer and Consumer threads
  auto start = std::chrono::high_resolution_clock::now();
  for (uint32_t i = 0; i < agent_config.migr_pkt_thread_num; i++) {
    // Receive packets from source agent and insert KV pairs into buffer
    threads_migr.emplace_back(&MigrationManager::DestinationReceivePktThread, this, i);
    thread_handles.emplace_back(threads_migr.back().native_handle());
  }

  for (uint32_t i = 0; i < agent_config.migr_thread_num; i++) {  
    // Mset KV pairs into redis instance
    threads_migr.emplace_back(&MigrationManager::InsertKVPairsThread, this, i);
    thread_handles.emplace_back(threads_migr.back().native_handle());
  }

  // Waiting for migration agent threads completion
  while (mg_term_flag < 1) ;
  migration_finish = true;
  std::cout << "total migration op: " << total_migration_op << std::endl;
  std::cout << "total migration op time: " << total_migration_time << std::endl;

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
}

