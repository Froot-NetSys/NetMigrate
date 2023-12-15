#include <iostream>
#include <string>
#include <mutex>
#include <cstring>
#include <algorithm>

#include "MigrationManager.h"

#define DEBUG 0

std::mutex iomutex;
std::mutex insert_mutex;

// Destination Agent

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

  while (mg_term_flag < storage_config.dst_port_list.size()) { // Loop until migration_termination pkt received by one of the destination threads
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

        mg_term_flag++;
        break;

      case _MG_MIGRATE: 
        DeserializeMgDataPkt(buf, &mg_data);
        for (int i = 0; i < mg_data.kv_num; i++) {
          DeserializeKey(key_pkt, mg_data.kv_payload[i].key);
          DeserializeVal(val_pkt, mg_data.kv_payload[i].val);
          kv_payload[i] = std::make_pair(std::string(key_pkt), std::string(val_pkt));
        }
        kv_buffer[dst_port_idx[mg_hdr.dbPort]].enqueue_bulk(kv_payload, mg_data.kv_num); 
#if DEBUG == 1
        {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "Thread " << thread_id << ": received _MG_MIGRATE packet" << std::endl;
        }
        
#endif 
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
        mg_hdr_reply.op = _MG_MIGRATE_GROUP_START_REPLY;
        mg_hdr_reply.seq = mg_hdr.seq;
        mg_hdr_reply.dbPort = storage_config.src_port_list[dst_port_idx.at(mg_hdr.dbPort)];
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
        mg_hdr_reply.dbPort = storage_config.src_port_list[dst_port_idx.at(mg_hdr.dbPort)];
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

  while (mg_term_flag < storage_config.dst_port_list.size() || buf_size) {
    dequeued_count = 0;
    dbPort = 0;
    for (int i = thread_id % dst_port_idx.size(); i < dst_port_idx.size(); i++) 
    {
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
      for (int i = thread_id % dst_port_idx.size(); i < dst_port_idx.size(); i++) 
      {
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
            std::vector<kv_pair_string> insert_pairs_vec(insert_pairs, insert_pairs + dequeued_count);
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
  mg_term_flag = 0;
  std::vector<std::thread::native_handle_type> thread_handles;

  // Request Agent threads
  for (uint32_t i = 0; i < agent_config.req_thread_num; i++) {
    threads_req.emplace_back(&MigrationManager::DestinationRequestHandlerThread, this, agent_config.migr_pkt_thread_num + i);
  }

  uint32_t scale_id = 0;
  for (uint32_t j = 0; j < agent_config.redis_scale_num; j++) {
    for (uint32_t i = 0; i < dst_port_idx.size(); i++) {
      threads_redis.emplace_back(&MigrationManager::DestinationRedisHandlerThread, this, scale_id);
      scale_id++;
    }
  }
  

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
  while (mg_term_flag < storage_config.dst_port_list.size()) ;
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

  // std::cout << "Rocksteady PriorityPull Extra Bandwidth Usage: " << extra_prioritypull_bandwidth << " (Bytes)" << std::endl;

  // Waiting for Request agent threads completion; currently use Ctrl-C to terminate request agent threads since we don't know when requests will not come
  for (uint32_t i = 0; i < threads_req.size(); i++) {
    threads_req.at(i).join();
  }

  for (uint32_t i = 0; i < threads_redis.size(); i++) {
    threads_redis.at(i).join();
  }

}


// PriorityPull is asynchronized, batched and de-duplicated

int MigrationManager::PriorityPullStub(std::shared_ptr<KeyValueStoreClient> client, uint16_t dbPort, uint32_t thread_id, std::string key) {
  // std::cout << "in priority pull" << std::endl;
  if (priority_pull_buffer.at(thread_id).find(key) == priority_pull_buffer.at(thread_id).end()) { // de-duplicated
    priority_pull_buffer.at(thread_id).emplace(key); 
    keys.at(thread_id).emplace_back(key);
  }
  
  auto endTime = std::chrono::system_clock::now();
  auto elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - m_StartTime_Pull.at(thread_id)).count();
  if (keys.at(thread_id).size() && (keys.at(thread_id).size() >= PRIORITY_PULL_BATCH_SIZE || elapsed_time >= PRIORITY_PULL_TIMEOUT)) { // batched
    std::vector<std::string> values;
    kv_pair_string key_values[MAX_KV_NUM];
    client->PriorityPull(keys.at(thread_id), values); // Asynchronous 
    uint32_t kv_num = 0;
    for (int i = 0; i < keys.at(thread_id).size(); i++) {
      if (values[i] != "-1") {
        key_values[i] = std::make_pair(keys.at(thread_id)[i], values[i]);
        kv_num++;
      }
    }
    keys.at(thread_id).clear();
    m_StartTime_Pull.at(thread_id) = std::chrono::system_clock::now();
    replay_buffer[dst_port_idx[dbPort]].enqueue_bulk(key_values, kv_num); 
    // std::cout << "kv_num = " << kv_num << std::endl;

    // extra_prioritypull_bandwidth += keys.at(thread_id).size() * (KEY_SIZE + VAL_SIZE);
  }

  endTime = std::chrono::system_clock::now();
  elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - m_StartTime_Buffer.at(thread_id)).count();
  if (elapsed_time >= PRIORITY_PULL_BUFFER_CLEAR) { 
    priority_pull_buffer.at(thread_id).clear();
    m_StartTime_Buffer.at(thread_id) = std::chrono::system_clock::now();
  }

  return 0;
}

inline std::unordered_map<uint16_t, std::shared_ptr<KeyValueStoreClient>> MigrationManager::InitRPCClient() {

    uint16_t server_port = 50051;
    std::unordered_map<uint16_t, std::shared_ptr<KeyValueStoreClient>> client;
    for (auto & redis_port : storage_config.dst_port_list) {
      auto channel = grpc::CreateChannel(this->agent_config.src_ip + ":"+ std::to_string(server_port), grpc::InsecureChannelCredentials());
      std::cout << "gRPC Client Connect to " << this->agent_config.src_ip + ":" + std::to_string(server_port) << std::endl;
      client.emplace(redis_port, std::make_shared<KeyValueStoreClient>(channel));
      server_port++;
    }
    return client;
}

// each Redis handler thread corresponds to a Redis instance and its port
void MigrationManager::DestinationRedisHandlerThread(uint32_t thread_id) { 
    uint32_t redis_id = thread_id % dst_port_idx.size();
    uint32_t scale_id = thread_id / dst_port_idx.size();

    char buf[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    struct mg_hdr_t mg_hdr;

    OptionalString val;
    struct mg_read_reply reply_pkt_read;
    struct mg_write_reply reply_pkt_write;

    cmd_q bulk_data[REQ_PIPELINE];
    std::vector<cmd_q> cmds;
    int socket_id;
    uint64_t seq;
    uint8_t ver;
    uint64_t miss = 0;

    auto client = InitRPCClient();

    while (true) {
        
        if (op_queue[storage_config.dst_port_list[redis_id]].size_approx() > 0) {
            size_t count = op_queue[storage_config.dst_port_list[redis_id]].try_dequeue_bulk(bulk_data, REQ_PIPELINE);
            for (uint32_t i = 0; i < count; i++) { 
                std::string op = bulk_data[i].op; // currently only read and write 
                std::string key = bulk_data[i].key;
                std::string val = bulk_data[i].val;
                // std::cout << "before append cmd " << i << std::endl;
                auto req_timer = std::chrono::high_resolution_clock::now();
                dst_req_agent->shard.at(storage_config.dst_port_list[redis_id])->AppendCmd(scale_id, op, key, val);
                auto req_timer_end = std::chrono::high_resolution_clock::now(); 
                if (migration_finish == false) {
                  total_req_op += 1;
                  total_req_time += std::chrono::duration_cast<std::chrono::microseconds>(req_timer_end - req_timer).count();
                }
                
                // std::cout << "after append cmd " << i << std::endl;
                cmds.push_back(bulk_data[i]);
            }

            // std::cout << "before exec" << std::endl;
            auto req_timer = std::chrono::high_resolution_clock::now();
            auto pipe_replies = dst_req_agent->shard.at(storage_config.dst_port_list[redis_id])->pipe.at(scale_id)->exec();
            auto req_timer_end = std::chrono::high_resolution_clock::now(); 
            if (migration_finish == false) {
              total_req_time += std::chrono::duration_cast<std::chrono::microseconds>(req_timer_end - req_timer).count();
            }
            
            // std::cout << "after exec" << std::endl;

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

                    if (val.has_value()) {
                        strncpy(reply_pkt_read.val, val.value().c_str(), std::min(VAL_SIZE, (int)val.value().size()));
                        reply_pkt_read.ver = ver | (server_type << 1) | 1; // destination, sucess
                    } 
                    else {
                        // miss++;
                        if (priority_pull_buffer.at(thread_id).find(cmds[i].key) != priority_pull_buffer.at(thread_id).end()) { // handle keys not in either source or destination
                          strncpy(reply_pkt_read.val, val_not_found, VAL_SIZE);
                          reply_pkt_read.ver = ver | (server_type << 1) | 0; // destination, fail
                        }
                        else {
                          // Issue PriorityPull 
                          PriorityPullStub(client.at(mg_hdr.dbPort), mg_hdr.dbPort, thread_id, std::string(cmds[i].key)); // Asynchronous
                          strncpy(reply_pkt_read.val, val_not_found, VAL_SIZE);
                          reply_pkt_read.ver = ver | 128; // client will try again after a random time 
                        }
                    }
                    

                    SerializeMgHdr(buf, &mg_hdr);
                    SerializeReadReplyPkt(buf, &reply_pkt_read);
                    
                    server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply));
                }
                else { // "SET"
                    auto res = pipe_replies.get<bool>(i);
                    reply_pkt_write = {0};
                    reply_pkt_write.ver = (server_type << 1) | (res > 0); 
                    
                    mg_hdr.op = _MG_WRITE_REPLY;
                    mg_hdr.dbPort = storage_config.dst_port_list[redis_id];
                    mg_hdr.seq = seq;

                    SerializeMgHdr(buf, &mg_hdr);
                    SerializeWriteReplyPkt(buf, &reply_pkt_write);
                    server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_write_reply));
                }
            }
            std::vector<cmd_q>().swap(cmds);
        }
    }
}

// pipeline version, a thread is mapped to one redis instance
// corresponding client thread is also mapped to one redis instance
void MigrationManager::DestinationRequestHandlerThread(uint32_t thread_id) { 
    char buf[BUFSIZE], buf_reply[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    uint32_t recvPkt = 0;
    struct mg_hdr_t mg_hdr;

    OptionalString val;
    char key_pkt[KEY_SIZE + 1];
    char val_pkt[VAL_SIZE + 1];
    std::vector<Optional<std::string>> values;
    std::vector<kv_pair_string> key_values;
    std::vector<std::string> keys;
    struct mg_read pkt_read;
    struct mg_write pkt_write;

    std::map<uint16_t, std::vector<cmd_q>> op_cmds;
    cmd_q tmp_write;
    cmd_q tmp_read;

    for (auto dbPort : storage_config.dst_port_list) {
      op_cmds.emplace(dbPort, std::vector<cmd_q>());
    }


    long long res = 0;
    int recvlen = 0;
    long long hit = 0;
    
    while (true) {
        if ((recvlen = server_sockets[thread_id]->Recv(buf)) < 0)
        {
            std::cout << "recvlen = " << recvlen << std::endl;
            continue;
        }
            
        // Deserialize packet and handle client requests to Redis
        DeserializeMgHdr(buf, &mg_hdr);

        recvPkt++;
        if(recvPkt % MILLION == 0) {
        // if(recvPkt % 1 == 0) {
          const std::lock_guard<std::mutex> lock(iomutex);
          std::cout << "RequestHandler thread " << thread_id << ": Recv packet " << recvPkt << ", dbPort " << mg_hdr.dbPort << std::endl;
          std::cout << "total req op = " << total_req_op << std::endl;
          std::cout << "total req op time = " << total_req_time << std::endl;
        }

        switch (mg_hdr.op) {
            case _MG_READ: 
                DeserializeReadPkt(buf, &pkt_read);
                assert(pkt_read.bitmap == 0x1); // switch will not change bitmap of single-op
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
                assert(pkt_write.bitmap == 0x1); // switch will not change bitmap of single-op
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
            case _MG_MULTI_READ: 

                DeserializeMultiReadPkt(buf, &pkt_multi_read);
                reply_pkt_multi_read = {0};
                reply_pkt_multi_read.bitmap = pkt_multi_read.bitmap;
                mg_hdr.op = _MG_MULTI_READ_REPLY;

                // deal with mget and priority pull
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
                      if (priority_pull_buffer.at(thread_id - agent_config.migr_thread_num).find(keys[idx]) \
                        != priority_pull_buffer.at(thread_id - agent_config.migr_thread_num).end()) { // handle keys not in either source or destination
                        strncpy(reply_pkt_multi_read.ver_val[idx].val, val_not_found, VAL_SIZE);
                        reply_pkt_multi_read.ver_val[idx].ver = pkt_multi_read.ver_key[idx].ver | (server_type << 1) | 0; // destination, fail
                      }
                      else {
                        // Issue PriorityPull 
                        PriorityPullStub(client.at(mg_hdr.dbPort), mg_hdr.dbPort, thread_id - agent_config.migr_thread_num, keys[idx]); // Asynchronous
                        strncpy(reply_pkt_multi_read.ver_val[idx].val, val_not_found, VAL_SIZE);
                        reply_pkt_multi_read.ver_val[idx].ver = pkt_multi_read.ver_key[idx].ver; // trick: source => client will try again after a random time 
                      }
                    }
                  }
                }

                SerializeMgHdr(buf, &mg_hdr);
                SerializeMultiReadReplyPkt(buf, &reply_pkt_multi_read);
                
                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_multi_read_reply));
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
                SerializeMultiWriteReplyPkt(buf, &reply_pkt_multi_write);
                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_multi_write_reply));
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