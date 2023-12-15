#include "MigrationManager.h"
#include <algorithm>
#include <iterator>

#define DEBUG 0

std::mutex iomutex_source;
std::mutex insert_mutex_source;

void MigrationManager::ScanKVPairsThread(unsigned long size_exp, uint16_t dbPort, uint32_t thread_id) {
    
    std::chrono::time_point<std::chrono::system_clock> start, end;
    start = std::chrono::system_clock::now();
    uint64_t debug_count = 0;
    uint64_t debug_scan_count = 0; 
    
    auto migration_timer = std::chrono::high_resolution_clock::now();

    unsigned long group_id_size_exp, remain_size_exp, step, group_id_st, group_id_end;
    // Calculating group_id_st and group_id_end
    group_id_size_exp = std::min(size_exp, (unsigned long) MAX_SIZE_EXP);
    remain_size_exp = size_exp - group_id_size_exp;
    step = (1 << group_id_size_exp) / agent_config.migr_thread_num; // assume thread_num is power of 2
    // std::cout << group_id_size_exp << " " << size_exp << " " << step << " " << agent_config.migr_thread_num << " " << (1 << group_id_size_exp) << std::endl;
    assert(step > 0);
    group_id_st = thread_id * step;
    group_id_end = group_id_st + step;

#if DEBUG == 1
    {
      const std::lock_guard<std::mutex> lock(iomutex_source);
      std::cout << "Thread " << thread_id << ": group id range: [" << group_id_st << ", " << group_id_end << ")" << std::endl;
    }
#endif

    unsigned long cursor = group_id_st << remain_size_exp;
    uint32_t last_group_id = group_id_st;
    uint32_t current_group_id = (cursor >> remain_size_exp);
    EnqueueGroupCtrlPkt(true, current_group_id, dbPort);

    while (current_group_id < group_id_end) {

      /*
      end = std::chrono::system_clock::now();
      auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds> (end - start).count();
      if (elapsed_seconds == 1) {
        std::cout << "enqueue: " << debug_count << std::endl;
        std::cout << "scan: " << debug_scan_count << std::endl;
        debug_count = 0;
        debug_scan_count = 0;
        start = std::chrono::system_clock::now();
      }
      */



#if DEBUG == 1
      {
        const std::lock_guard<std::mutex> lock(iomutex_source);
        std::cout << "Thread " << thread_id << ": before scan, cursor = " << cursor << std::endl;
      }
#endif
      scan_reply res;

      if (thread_id == 0) {
        auto end = std::chrono::high_resolution_clock::now();
        auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - migration_timer);
        if (time_elapsed.count() >= CHECK_PERIOD) { // 1 second
          std::cout << "total_migration_op/s=" << migration_op << std::endl;
          migration_op = 0;
          migration_timer = std::chrono::high_resolution_clock::now();
        }
      }
      

      // if (migration_op < SRC_THRESHOLD_MIGRATION) 
      {
        total_migration_op++;
        auto migration_timer = std::chrono::high_resolution_clock::now();
        try {
          res = src_migr_agent->shard.at(dbPort)->redis \
              ->command<scan_reply>("migratescan", cursor, SCAN_BATCH, group_id_size_exp);
        } catch (const ReplyError &err) {
          std::cout << err.what() << std::endl;
        }
        auto migration_timer_end = std::chrono::high_resolution_clock::now();
        total_migration_time += std::chrono::duration_cast<std::chrono::microseconds>(migration_timer_end - migration_timer).count();
      }
      // else 
      //   continue;
      debug_scan_count++;
      

#if DEBUG == 1 
      {
        const std::lock_guard<std::mutex> lock(iomutex_source);
        std::cout << "Thread " << thread_id << ": get scan reply" << std::endl;
      }
#endif  
    
    if (res.size() != 3) {
      const std::lock_guard<std::mutex> lock(iomutex_source);
      printf("migratescan Error!\n");
    }
    
    debug_count += EnqueueDataPkt(res, dbPort);
    // std::cout << res[1].size() << std::endl;

    cursor = std::stoul(res[0][0]);
    if (cursor == 0) cursor = (1 << (group_id_size_exp + remain_size_exp));

#if DEBUG == 1  
    {
      const std::lock_guard<std::mutex> lock(iomutex_source);
      std::cout << "Thread " << thread_id << ": after scan, cursor = " << cursor << " " << (group_id_end << remain_size_exp) << std::endl;
    }
#endif

    last_group_id = current_group_id;
    current_group_id = (cursor >> remain_size_exp);
    if (current_group_id == last_group_id + 1) { 
      EnqueueGroupCtrlPkt(false, last_group_id, dbPort);
      if (current_group_id < group_id_end) 
        EnqueueGroupCtrlPkt(true, current_group_id, dbPort);
    }
  }

    {
      const std::lock_guard<std::mutex> lock(iomutex_source);
      std::cout << "Thread " << thread_id << ": instance " << dbPort << " scan finished" << std::endl;
    }
}

void MigrationManager::EnqueueGroupCtrlPkt(bool start, uint32_t group_id, uint16_t dbPort) {
  struct mg_data mg_data;
  if (start)
    mg_data.kv_num = 0xFE; // trick here
  else 
    mg_data.kv_num = 0xFF;
  mg_data.kv_payload[0].key[0] = (char)(group_id >> 24) & 0xFF;
  mg_data.kv_payload[0].key[1] = (char)(group_id >> 16) & 0xFF;
  mg_data.kv_payload[0].key[2] = (char)(group_id >> 8) & 0xFF;
  mg_data.kv_payload[0].key[3] = (char)(group_id) & 0xFF;
  pkt_buffer[0].enqueue(mg_data);
}

int MigrationManager::EnqueueDataPkt(scan_reply res, uint16_t dbPort) {
  int enqueued_count = 0;
  struct mg_data mg_data;
  if (res[1].size() == 0) return 0;
  uint32_t remain_num = res[1].size(); 
  uint32_t idx = 0;
  while (remain_num > MAX_KV_NUM) {
    mg_data.kv_num = MAX_KV_NUM;
    for (uint8_t i = 0; i < mg_data.kv_num; i++) {
      SerializeKey(res[1][i + idx].c_str(), mg_data.kv_payload[i].key);
      SerializeVal(res[2][i + idx].c_str(), mg_data.kv_payload[i].val);
    } 
    pkt_buffer[0].enqueue(mg_data);
    enqueued_count += mg_data.kv_num;
    remain_num -= MAX_KV_NUM;
    idx += MAX_KV_NUM;
  }
  if (remain_num) {
    mg_data.kv_num = remain_num;
    for (uint8_t i = 0; i < mg_data.kv_num; i++) {
      SerializeKey(res[1][i + idx].c_str(), mg_data.kv_payload[i].key);
      SerializeVal(res[2][i + idx].c_str(), mg_data.kv_payload[i].val);
    } 
    pkt_buffer[0].enqueue(mg_data);
    enqueued_count += mg_data.kv_num;
  }
  
  // src_migr_agent->shard.at(dbPort)->redis->del(res[1].begin(), res[1].end()); // Can delete after migration finishes
  return enqueued_count;
}

void MigrationManager::SourceSendPktThread(std::atomic_int & done_scan, int thread_id) {

  char buf[BUFSIZE], buf_reply[BUFSIZE];
  struct mg_hdr_t mg_hdr;
  struct mg_hdr_t mg_hdr_reply;
  struct mg_data mg_data;
  struct mg_group_ctrl mg_group_ctrl;

  bool res = false;
  mg_hdr.seq = 0;

  size_t buf_size = pkt_buffer[0].size_approx();



  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();
  uint64_t debug_count = 0;

  
  while (done_scan < agent_config.migr_thread_num || buf_size) {


    end = std::chrono::system_clock::now();
    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds> (end - start).count();
    if (elapsed_seconds == 1) {
      // std::cout << "dequeue: " << debug_count << std::endl;
      debug_count = 0;
      start = std::chrono::system_clock::now();
    }

    


    for (int i = 0; i < 1; i++) {
      res = pkt_buffer[i].try_dequeue(mg_data);
      if (res) {
        mg_hdr.dbPort = storage_config.dst_port;
        if (mg_data.kv_num < 0xFE) { // migration data packet
          mg_hdr.op = _MG_MIGRATE;
          mg_hdr.seq ++;
          SerializeMgHdr(buf, &mg_hdr);
          SerializeMgDataPkt(buf, &mg_data);
          client_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(mg_data));
          client_sockets[thread_id]->Recv(buf_reply);
          debug_count ++;
          DeserializeMgHdr(buf_reply, &mg_hdr_reply);
          // assert(mg_hdr_reply.seq == mg_hdr.seq); 
        }
        else { // migration group start or complete
          if (mg_data.kv_num == 0xFE) 
            mg_hdr.op = _MG_MIGRATE_GROUP_START;
          else if (mg_data.kv_num == 0xFF)
            mg_hdr.op = _MG_MIGRATE_GROUP_COMPLETE;
          mg_hdr.seq ++;
          SerializeMgHdr(buf, &mg_hdr);
          mg_group_ctrl.group_id = ((mg_data.kv_payload[0].key[0] << 24) & 0xFF000000) + ((mg_data.kv_payload[0].key[1] << 16) & 0xFF0000) + ((mg_data.kv_payload[0].key[2] << 8) & 0xFF00) + (mg_data.kv_payload[0].key[3] & 0xFF);
          SerializeMgGroupCtrlPkt(buf, &mg_group_ctrl);
          client_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(mg_group_ctrl));
          client_sockets[thread_id]->Recv(buf_reply);
          debug_count ++;
          DeserializeMgHdr(buf_reply, &mg_hdr_reply); 
          // assert(mg_hdr_reply.seq == mg_hdr.seq); 
          if (mg_hdr.op == _MG_MIGRATE_GROUP_START) {
            uint32_t index = mg_group_ctrl.group_id / migration_step.at(i); 
            group_start_pkt_thread.at(i).at(index) = true;
            group_start_pkt_sent.at(i).at(index) = mg_group_ctrl.group_id;
          }
          else if (mg_hdr.op == _MG_MIGRATE_GROUP_COMPLETE) {
            uint32_t index = mg_group_ctrl.group_id / migration_step.at(i); 
            group_complete_pkt_thread.at(i).at(index) = true;
            group_complete_pkt_sent.at(i).at(index) = mg_group_ctrl.group_id;
          }
        } 
      }
    }
    buf_size = 0;
    for (int i = 0; i < 1; i++) {
      buf_size += pkt_buffer[i].size_approx();
    }
  }

  {
    const std::lock_guard<std::mutex> lock(iomutex_source);
    std::cout << "SourceSendPktThread " << thread_id << ": final packet sequence number " << mg_hdr.seq << std::endl;
  }

  exit_flag = true;
  
}

void MigrationManager::EnqueueDataPktLog(dst_cmd_q * insert_pairs, int count) {
  struct mg_data mg_data;
  if (count == 0) return;
  uint32_t remain_num = count;
  uint32_t idx = 0;
  while (remain_num > MAX_KV_NUM) {
    mg_data.kv_num = MAX_KV_NUM;
    for (uint8_t i = 0; i < mg_data.kv_num; i++) {
      SerializeKey(insert_pairs[i + idx].key.c_str(), mg_data.kv_payload[i].key);
      SerializeVal(insert_pairs[i + idx].val.c_str(), mg_data.kv_payload[i].val);
    } 
    pkt_buffer[0].enqueue(mg_data);
    remain_num -= MAX_KV_NUM;
    idx += MAX_KV_NUM;
  }
  if (remain_num) {
    mg_data.kv_num = remain_num;
    for (uint8_t i = 0; i < mg_data.kv_num; i++) {
      SerializeKey(insert_pairs[i + idx].key.c_str(), mg_data.kv_payload[i].key);
      SerializeVal(insert_pairs[i + idx].val.c_str(), mg_data.kv_payload[i].val);
    } 
    pkt_buffer[0].enqueue(mg_data);
  }
}


void MigrationManager::SourceSendLog(std::atomic_int & done_send) {
    size_t log_cmd_size = log_cmds.size_approx();
    int dequeue_count = 0;
    dst_cmd_q insert_pairs[MIGRATE_SIZE_THRESHOLD];

    while (done_send < agent_config.migr_thread_num || log_cmd_size > MIGRATE_COMPLETE_SIZE) {
      if (log_cmd_size >= MIGRATE_SIZE_THRESHOLD) {
        dequeue_count = log_cmds.try_dequeue_bulk(insert_pairs, MIGRATE_SIZE_THRESHOLD);
        EnqueueDataPktLog(insert_pairs, dequeue_count);
        extra_source_bandwidth += dequeue_count * (KEY_SIZE + VAL_SIZE);
      }
      log_cmd_size = log_cmds.size_approx();
    }

    try {
        src_redis->redis->command("client", "pause", 100000, "ALL"); 
    } catch (const ReplyError & err) {
        std::cout << "ReplyError: " << err.what() << std::endl;
    }

    while (log_cmds.size_approx()) {
      dequeue_count = log_cmds.try_dequeue_bulk(insert_pairs, MIGRATE_SIZE_THRESHOLD);
      EnqueueDataPktLog(insert_pairs, dequeue_count);
      std::cout << "dequeue count = " << dequeue_count << std::endl;
      extra_source_bandwidth += dequeue_count * (KEY_SIZE + VAL_SIZE);
    }

}

inline uint32_t MigrationManager::get_group_id(uint16_t dbPort, char * key) {
  uint32_t size_exp = migration_size_exp_vec.at(0);
  uint32_t dict_mask = (1 << size_exp) - 1;
  uint32_t group_id_size_exp = std::min(size_exp, (uint32_t)MAX_SIZE_EXP);
  uint32_t remain_size_exp = size_exp - group_id_size_exp;
  uint32_t bucket_id = crc64(0, (const unsigned char*) key, KEY_SIZE) & dict_mask;
  return (bucket_id >> remain_size_exp);
}

inline bool MigrationManager::check_late_group_id(uint16_t dbPort, uint32_t group_id) {
  uint32_t db_idx = 0;
  for (uint32_t i = 0; i < group_start_pkt_sent.at(db_idx).size(); i++) {
    
    if (group_id >= migration_step.at(db_idx) * i && group_id < migration_step.at(db_idx) * (i+1)) {
      // std::cout << group_id << " " << i << " " << migration_step.at(db_idx) * i << " " << migration_step.at(db_idx) * (i+1) << std::endl;
      if (group_start_pkt_thread.at(db_idx)[i] && group_id <= group_start_pkt_sent.at(db_idx)[i]) {
        // std::cout << group_id << " " << group_start_pkt_sent.at(db_idx)[i] << std::endl;
        return true;
      }
    }
    
  }
  return false;
}

void MigrationManager::SourceRequestHandlerThread(uint32_t thread_id) {
    char buf[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    uint32_t recvPkt = 0;
    struct mg_hdr_t mg_hdr;

    OptionalString val;
    struct mg_read pkt_read;
    struct mg_write pkt_write;
    int recvlen = 0;

    char key_pkt[KEY_SIZE + 1] = {0};
    char val_pkt[VAL_SIZE + 1] = {0};

    std::map<uint16_t, std::vector<cmd_q>> op_cmds;
    for (auto dbPort : redis_ports) {
      op_cmds.emplace(dbPort, std::vector<cmd_q>());
    }
    cmd_q tmp_write;
    cmd_q tmp_read;
    dst_cmd_q tmp_set;
    
    uint32_t group_id = 0;

    while (true) {

        /*
        if (migration_start && thread_id > 1)
          continue;
        */

        if ((recvlen = server_sockets[thread_id]->Recv(buf)) < 0) {
            const std::lock_guard<std::mutex> lock(iomutex_source);
            std::cout << "recvlen = " << recvlen << std::endl;
            continue;
        }
        
        recvPkt++;
        if(recvPkt % MILLION == 0) {
            const std::lock_guard<std::mutex> lock(iomutex_source);
            std::cout << "Thread " << thread_id << ": Recv packet " << recvPkt << std::endl;
            std::cout << "extra source bandwidth = " << extra_source_bandwidth << " (Bytes)" << std::endl;
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
                
                
                tmp_write.op = tmp_set.op = "SET";
                tmp_write.key = tmp_set.key = std::string(key_pkt);
                tmp_write.val = tmp_set.val = std::string(val_pkt);
                tmp_write.socket_id = thread_id;
                tmp_write.seq = mg_hdr.seq;
                tmp_write.ver = 0;
                op_cmds[mg_hdr.dbPort].push_back(tmp_write);

                group_id = get_group_id(mg_hdr.dbPort, key_pkt);
                // std::cout << "check = " << check_late_group_id(mg_hdr.dbPort, group_id) << std::endl;
                if (check_late_group_id(mg_hdr.dbPort, group_id)) {
                  log_cmds.enqueue(tmp_set);
                }

                break;

            default:
                {
                    const std::lock_guard<std::mutex> lock(iomutex_source);
                    std::cout << "Currently not handled packet" << std::endl;
                }
        }

        for (auto dbPort : redis_ports) {
          if (op_cmds[dbPort].size() % REQ_PIPELINE == 0) {
            op_queue[dbPort].enqueue_bulk(op_cmds[dbPort].data(), op_cmds[dbPort].size());
            std::vector<cmd_q>().swap(op_cmds[dbPort]);
          }
        }
        
         
    }

}

void MigrationManager::SourceRedisHandlerThread(uint32_t thread_id) {
    uint32_t redis_id = thread_id % src_port_idx.size();
    uint32_t scale_id = thread_id / src_port_idx.size();

    char buf[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    struct mg_hdr_t mg_hdr;

    OptionalString val;
    bool res;
    struct mg_read_reply reply_pkt_read;
    struct mg_write_reply reply_pkt_write;

    cmd_q bulk_data[REQ_PIPELINE];
    std::vector<cmd_q> cmds;
    int socket_id;
    uint64_t seq;
    uint8_t ver;

    uint32_t last_cmd_idx;

    uint32_t send_src_exit_count = 0;

    auto client_timer = std::chrono::high_resolution_clock::now();
    uint64_t client_op = 0;

    while (true) {

        auto end = std::chrono::high_resolution_clock::now();
        auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - client_timer);
        if (time_elapsed.count() >= CHECK_PERIOD) {
          // std::cout << "client_op/s=" << client_op << std::endl;
          client_op = 0;
          client_timer = std::chrono::high_resolution_clock::now();
        }
      
        // if (migration_start == false || client_op < SRC_THRESHOLD_CLIENT) 
        {
        
          if (op_queue[redis_ports[redis_id]].size_approx() > 0) {
              size_t count = op_queue[redis_ports[redis_id]].try_dequeue_bulk(bulk_data, REQ_PIPELINE);
              for (uint32_t i = 0; i < count; i++) {
                  std::string op = bulk_data[i].op;
                  std::string key = bulk_data[i].key;
                  std::string val = bulk_data[i].val;
                  auto req_timer = std::chrono::high_resolution_clock::now();
                  try {
                      // src_redis_agent->AppendCmd(scale_id, op, key, val);
                      src_req_agent->shard.at(redis_ports[redis_id])->AppendCmd(scale_id, op, key, val);
                  }
                  catch (const Error &err) {
                      std::cout << err.what() << std::endl;
                      // std::cout << "redis io error catched" << std::endl;
                      exit_flag = true;
                  }
                  auto req_timer_end = std::chrono::high_resolution_clock::now();
                  
                  if (migration_start) {
                    total_req_op += 1;
                    total_req_time += std::chrono::duration_cast<std::chrono::microseconds>(req_timer_end - req_timer).count();
                  }
                      
                  cmds.push_back(bulk_data[i]);
              }

              last_cmd_idx = 0;
            
              if (!exit_flag) {
                  try {
                      // auto pipe_replies = src_redis_agent->pipe.at(scale_id)->exec();
                      auto req_timer = std::chrono::high_resolution_clock::now();
                      auto pipe_replies = src_req_agent->shard.at(redis_ports[redis_id])->pipe.at(scale_id)->exec();
                      auto req_timer_end = std::chrono::high_resolution_clock::now();
                      if (migration_start)
                        total_req_time += std::chrono::duration_cast<std::chrono::microseconds>(req_timer_end - req_timer).count();

                      for (uint32_t i = 0; i < cmds.size(); i++) {
                          last_cmd_idx = i;
                          if (exit_flag) 
                              break;
                      
                          socket_id = cmds[i].socket_id;
                          seq = cmds[i].seq;
                          ver = cmds[i].ver | (migration_start << 7);
                          client_op += 1;
                          if (cmds[i].op == "GET") {
                              reply_pkt_read = {0};
                              val = pipe_replies.get<OptionalString>(i);
                              
                              if (val.has_value()) {
                                  strncpy(reply_pkt_read.val, val.value().c_str(), std::min(VAL_SIZE, (int)val.value().size()));
                                  reply_pkt_read.ver = ver | (server_type << 1) | 1; 
                              }
                              else {
                                  strncpy(reply_pkt_read.val, val_not_found, VAL_SIZE);
                                  reply_pkt_read.ver = ver | (server_type << 1); 
                              }
                              
                              mg_hdr.op = _MG_READ_REPLY;
                              mg_hdr.dbPort = redis_ports[redis_id];
                              mg_hdr.seq = seq;

                              SerializeMgHdr(buf, &mg_hdr);
                              SerializeReadReplyPkt(buf, &reply_pkt_read);
                              
                              server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply));
                          }
                          else {
                              res = pipe_replies.get<bool>(i);

                              reply_pkt_write = {0};
                              reply_pkt_write.ver = (server_type << 1) | (res > 0); 
                              
                              mg_hdr.op = _MG_WRITE_REPLY;
                              mg_hdr.dbPort = redis_ports[redis_id];
                              mg_hdr.seq = seq;

                              SerializeMgHdr(buf, &mg_hdr);
                              SerializeWriteReplyPkt(buf, &reply_pkt_write);
                              server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_write_reply));
                          }
                      }
                      std::vector<cmd_q>().swap(cmds);
                  }
                  catch (const Error &err) {
                    std::cout << err.what() << std::endl;
                      // std::cout << "redis io error catched" << std::endl;
                      exit_flag = true;
                  }
              }

              if (exit_flag) {
                  for (uint32_t i = last_cmd_idx; i < cmds.size(); i++) {
                      socket_id = cmds[i].socket_id;
                      seq = cmds[i].seq;
                      ver = cmds[i].ver | (migration_start << 7);
                      if (cmds[i].op == "GET") {
                          reply_pkt_read = {0};
                          reply_pkt_read.ver = 255;
                          mg_hdr.op = _MG_READ_REPLY;
                          mg_hdr.dbPort = redis_ports[redis_id];
                          mg_hdr.seq = seq;
                          SerializeMgHdr(buf, &mg_hdr);
                          SerializeReadReplyPkt(buf, &reply_pkt_read);
                          server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply));
                      }
                      else { // "SET"
                          reply_pkt_write = {0};
                          reply_pkt_write.ver = 255;
                          mg_hdr.op = _MG_WRITE_REPLY;
                          mg_hdr.dbPort = redis_ports[redis_id];
                          mg_hdr.seq = seq;
                          SerializeMgHdr(buf, &mg_hdr);
                          SerializeWriteReplyPkt(buf, &reply_pkt_write);
                          server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_write_reply));
                      }
                      send_src_exit_signal[socket_id] = true;    
                  }

                  send_src_exit_count = 0;
                  for (uint32_t i = 0; i < agent_config.req_thread_num; i++) {
                      send_src_exit_count += send_src_exit_signal[i];
                  }
                  if (send_src_exit_count == agent_config.req_thread_num) {
                      std::cout << "Send switch to destination signal to clients. Exit..." << std::endl;
                      exit(1);
                  }
              }  
              
          }
        }
    }
}


void MigrationManager::SourceMigrationManager() {

    for (uint32_t i = 0; i < agent_config.req_thread_num; i++) {
        threads_req.emplace_back(&MigrationManager::SourceRequestHandlerThread, this, i);
    }

    uint32_t redis_thread_id = 0;
    for (uint32_t j = 0; j < agent_config.redis_scale_num; j++) { 
        for (uint32_t i = 0; i < redis_ports.size(); i++) {
            threads_req.emplace_back(&MigrationManager::SourceRedisHandlerThread, this, redis_thread_id);
            redis_thread_id++;
        }
    }

    std::this_thread::sleep_for(std::chrono::seconds(300));

    migration_start = true;

    int status = 0;

    char buf[BUFSIZE];
    char buf_reply[BUFSIZE];
    struct mg_hdr_t mg_hdr;
    struct mg_hdr_t mg_hdr_reply;
    struct mg_ctrl mg_init;
    struct mg_ctrl mg_terminate;
    std::atomic_int done_scan = 0;
    std::atomic_int done_send = 0;

    std::thread thread_sendlog = std::thread(&MigrationManager::SourceSendLog, this, std::ref(done_send));

    auto start = std::chrono::high_resolution_clock::now();
    
    mg_hdr.op = _MG_INIT;
    mg_hdr.seq = 0;
    mg_hdr.dbPort = storage_config.dst_port;
    SerializeMgHdr(buf, &mg_hdr);

    inet_pton(AF_INET, agent_config.src_trans_ip.c_str(), &(mg_init.srcAddr));
    mg_init.srcPort = storage_config.src_port;
    inet_pton(AF_INET, agent_config.dst_trans_ip.c_str(), &(mg_init.dstAddr));
    mg_init.dstPort = storage_config.dst_port;
    SerializeMgCtrlPkt(buf, &mg_init);

    client_sockets[0]->Send(buf, HDR_SIZE + (ADDR_SIZE + PORT_SIZE) * 2);
    client_sockets[0]->Recv(buf_reply);

    // Pause dict rehashing and get dict size
    unsigned long size_exp = 0;
    std::string res;
    try {
      res = src_migr_agent->shard.at(storage_config.src_port)->redis->command<std::string>("migratestart");
    } catch (const ReplyError &err) {
      std::cout << err.what() << std::endl;
    } 
    if (res[0] >= '0' && res[0] <= '9')
      size_exp = std::stoul(res);
    
    unsigned long group_id_size_exp = std::min(size_exp, (unsigned long) MAX_SIZE_EXP);

    migration_step.at(0) = (1 << group_id_size_exp) / agent_config.migr_thread_num;
    migration_size_exp_vec.at(0) = size_exp;

    for (uint32_t i = 0; i < agent_config.migr_thread_num; i++) {
      threads_migr.emplace_back(&MigrationManager::ScanKVPairsThread, this, size_exp, storage_config.src_port, i);
    }

    for (uint32_t j = 0; j < agent_config.migr_pkt_thread_num; j++) {
      // Store KV pairs into pkts and send to destination proxy
      threads_migr.emplace_back(&MigrationManager::SourceSendPktThread, this, std::ref(done_scan), j);
    }

    // Waiting for scanning finishing
    for (uint32_t i = 0; i < agent_config.migr_thread_num; i++) {
      threads_migr.at(i).join();
      done_scan++;
    }

    std::cout << "Total migration op:" << total_migration_op << std::endl;
    std::cout << "Total migration op time: " << total_migration_time << std::endl;


    // Waiting for all data migration packets sent out
    for (uint32_t i = agent_config.migr_thread_num; i < threads_migr.size(); i++) {
      threads_migr.at(i).join();
      done_send++;
    }

    // Resume dict rehashing and delete migrated keys
    try {
      src_migr_agent->shard.at(storage_config.src_port)->redis->command<void>("migratefinish");
    } catch (const ReplyError &err) {
      std::cout << err.what() << std::endl;
    } 

    // Send termination pkt to destination agent
    mg_hdr.op = _MG_TERMINATE;
    mg_hdr.seq = 0; // TODO: change to real seq 
    mg_hdr.dbPort = storage_config.dst_port;
    SerializeMgHdr(buf, &mg_hdr);

    mg_terminate = mg_init;
    SerializeMgCtrlPkt(buf, &mg_terminate);
    
    client_sockets[0]->Send(buf, HDR_SIZE + (ADDR_SIZE + PORT_SIZE) * 2);
    client_sockets[0]->Recv(buf_reply);
    DeserializeMgHdr(buf_reply, &mg_hdr_reply);
    // assert(mg_hdr_reply.seq == mg_hdr.seq);

    thread_sendlog.join();
    std::cout << "Source Extra Bandwidth Usage: " << extra_source_bandwidth << " (Bytes)" << std::endl;

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    std::cout << "Source Migration Agent Running Time (ms): " << duration.count() << std::endl;


    for (uint32_t i = 0; i < threads_req.size(); i++) {
        threads_req.at(i).join();
    }
    
}

