#include <iostream>
#include <string>
#include <mutex>

#include "MigrationManager.h"

#define DEBUG 0

std::mutex iomutex_source;
std::mutex insert_mutex_source;
std::mutex send_mcr_begin_mutex;

// Source Agent 

void MigrationManager::ScanKVPairsThread(unsigned long size_exp, uint16_t dbPort, uint32_t thread_id) { 
    auto migration_timer = std::chrono::high_resolution_clock::now();
    
    unsigned long group_id_size_exp, remain_size_exp, step, group_id_st, group_id_end;
    // Calculating group_id_st and group_id_end
    group_id_size_exp = std::min(size_exp, (unsigned long) MAX_SIZE_EXP);
    remain_size_exp = size_exp - group_id_size_exp; // remain_size_exp = 0
    step = (1 << group_id_size_exp) / agent_config.migr_thread_num; // assume thread_num is power of 2
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
        if (time_elapsed.count() >= CHECK_PERIOD) {
          // std::cout << "total_migration_op/s=" << migration_op << std::endl;
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
    
    EnqueueDataPkt(res, dbPort);
    cursor = std::stoul(res[0][0]);
    if (cursor == 0) cursor = (1 << (group_id_size_exp + remain_size_exp));

#if DEBUG == 1  
    {
      const std::lock_guard<std::mutex> lock(iomutex_source);
      std::cout << "Thread " << thread_id << ": cursor = " << cursor << " " << (group_id_end << remain_size_exp) << std::endl;
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
      std::cout << "ScanKVPairsThread " << thread_id << ": instance " << dbPort << " scan finished" << std::endl;
    }
}

void MigrationManager::EnqueueGroupCtrlPkt(bool start, uint32_t group_id, uint16_t dbPort) {
  struct mg_data mg_data;
  if (start)
    mg_data.kv_num = 0xFE; // trick here
  else 
    mg_data.kv_num = 0xFF;
  mg_data.kv_payload[0].key[0] = (group_id >> 24) & 0xFF;
  mg_data.kv_payload[0].key[1] = (group_id >> 16) & 0xFF;
  mg_data.kv_payload[0].key[2] = (group_id >> 8) & 0xFF;
  mg_data.kv_payload[0].key[3] = (group_id) & 0xFF;
  pkt_buffer[src_port_idx[dbPort]].enqueue(mg_data);
}

void MigrationManager::EnqueueDataPkt(scan_reply res, uint16_t dbPort) {
  struct mg_data mg_data;
  if (res[1].size() == 0) return;
  uint32_t remain_num = res[1].size(); 
  uint32_t idx = 0;
  while (remain_num > MAX_KV_NUM) {
    mg_data.kv_num = MAX_KV_NUM;
    for (uint8_t i = 0; i < mg_data.kv_num; i++) {
      SerializeKey(res[1][i + idx].c_str(), mg_data.kv_payload[i].key);
      SerializeVal(res[2][i + idx].c_str(), mg_data.kv_payload[i].val);
    } 
    pkt_buffer[src_port_idx[dbPort]].enqueue(mg_data);
    remain_num -= MAX_KV_NUM;
    idx += MAX_KV_NUM;
  }
  if (remain_num) {
    mg_data.kv_num = remain_num;
    for (uint8_t i = 0; i < mg_data.kv_num; i++) {
      SerializeKey(res[1][i + idx].c_str(), mg_data.kv_payload[i].key);
      SerializeVal(res[2][i + idx].c_str(), mg_data.kv_payload[i].val);
    } 
    pkt_buffer[src_port_idx[dbPort]].enqueue(mg_data);
  }
  // src_migr_agent->shard.at(dbPort)->redis->del(res[1].begin(), res[1].end()); // Can delete after migration finishes
}

void MigrationManager::SourceSendPktThread(std::atomic_int & done_scan, int thread_id) {

  char buf[BUFSIZE], buf_reply[BUFSIZE];
  struct mg_hdr_t mg_hdr;
  struct mg_hdr_t mg_hdr_reply;
  struct mg_data mg_data;
  struct mg_group_ctrl mg_group_ctrl;

  bool res = false;
  mg_hdr.seq = 0;

  size_t buf_size = 0;
  for (int i = 0; i < src_port_idx.size(); i++) {
    buf_size += pkt_buffer[i].size_approx();
  }
  
  while (done_scan < storage_config.src_port_list.size() * agent_config.migr_thread_num || buf_size) {
    for (int i = 0; i < src_port_idx.size(); i++) {
      res = pkt_buffer[i].try_dequeue(mg_data);
      if (res) {
        mg_hdr.dbPort = storage_config.dst_port_list[i];
        if (mg_data.kv_num < 0xFE) { // migration data packet
          mg_hdr.op = _MG_MIGRATE;
          mg_hdr.seq ++;
          SerializeMgHdr(buf, &mg_hdr);
          SerializeMgDataPkt(buf, &mg_data);
          client_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(mg_data));
          client_sockets[thread_id]->Recv(buf_reply);
          DeserializeMgHdr(buf_reply, &mg_hdr_reply);
          assert(mg_hdr_reply.seq == mg_hdr.seq); 
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
          DeserializeMgHdr(buf_reply, &mg_hdr_reply);
          assert(mg_hdr_reply.seq == mg_hdr.seq); 
        } 
      }
    }
    buf_size = 0;
    for (int i = 0; i < src_port_idx.size(); i++) {
      buf_size += pkt_buffer[i].size_approx();
    }
  }

  {
    const std::lock_guard<std::mutex> lock(iomutex_source);
    std::cout << "SourceSendPktThread " << thread_id << ": final packet sequence number " << mg_hdr.seq << std::endl;
  }
  
}

void MigrationManager::SourceMigrationManager() {

  char buf[BUFSIZE];
  char buf_reply[BUFSIZE];
  struct mg_hdr_t mg_hdr;
  struct mg_hdr_t mg_hdr_reply;
  struct mg_ctrl mg_init;
  uint32_t mcr_begin[MAX_THREAD_NUM];
  size_t mcr_size = 0;
  struct mg_ctrl mg_terminate;

  auto start = std::chrono::high_resolution_clock::now();

  std::atomic_int done_scan = 0;
  std::vector<unsigned long> size_exp_vec;

  char ipc_buf[BUFSIZE];

  // Send MCRBegin packet to client and destination;
  for (int i = 0; i < storage_config.src_port_list.size(); i++) {
    // Pause dict rehashing and get dict size
    unsigned long size_exp = 0;
    std::string res;
    try {
      res = src_migr_agent->shard.at(storage_config.src_port_list[i])->redis->command<std::string>("migratestart");
    } catch (const ReplyError &err) {
      std::cout << err.what() << std::endl;
    } 
    if (res[0] >= '0' && res[0] <= '9')
      size_exp = std::stoul(res);

    // Send MCR begin range packet to destination migration agent 
    mg_hdr.op = FULVA_MCR_BEGIN;
    mg_hdr.seq = i; // TODO: correct seq
    mg_hdr.dbPort = storage_config.dst_port_list[i];
    SerializeMgHdr(buf, &mg_hdr);
    mcr_size = agent_config.migr_thread_num;
    unsigned long step = (1 << size_exp) / mcr_size;
    for (int j = 0; j < mcr_size; j++) {
      mcr_begin[j] = j * step;
    }
    
    unsigned long group_id_size_exp = std::min(size_exp, (unsigned long) MAX_SIZE_EXP);
    SerializeMCRBeginPkt(buf, mcr_begin, mcr_size, size_exp - group_id_size_exp);

    client_sockets[0]->Send(buf, HDR_SIZE + mcr_size * 4 + 4);
    client_sockets[0]->Recv(buf_reply);

    // Send ipc message
    std::string tmp_port = std::to_string(storage_config.src_port_list[i]);
    strcpy(ipc_buf, tmp_port.c_str());
    ipc_client_socket->Send(ipc_buf, tmp_port.size());
    migration_start = true;

    // size_exp_vec.push_back(size_exp);
    // Start Producer and Consumer threads
    for (uint32_t j = 0; j < agent_config.migr_thread_num; j++) {
      // Scan Redis instance and insert into buffer
      threads_migr.emplace_back(&MigrationManager::ScanKVPairsThread, this, size_exp, storage_config.src_port_list[i], j);
    }
  }

  for (uint32_t j = 0; j < agent_config.migr_pkt_thread_num; j++) {
    // Store KV pairs into pkts and send to destination proxy
    threads_migr.emplace_back(&MigrationManager::SourceSendPktThread, this, std::ref(done_scan), j);
  }

  // Waiting for scanning finishing
  uint32_t scan_thread_num = storage_config.src_port_list.size() * agent_config.migr_thread_num;
  for (uint32_t i = 0; i < scan_thread_num; i++) {
    threads_migr.at(i).join();
    done_scan++;
  }
  
  // Waiting for all data migration packets sent out
  for (uint32_t i = scan_thread_num; i < threads_migr.size(); i++) {
    threads_migr.at(i).join();
  }

  for (int i = 0; i < storage_config.src_port_list.size(); i++) {
    // Resume dict rehashing and delete migrated keys
    try {
      src_migr_agent->shard.at(storage_config.src_port_list[i])->redis->command<void>("migratefinish");
    } catch (const ReplyError &err) {
      std::cout << err.what() << std::endl;
    } 

    // Send termination pkt to destination agent
    mg_hdr.op = _MG_TERMINATE;
    mg_hdr.seq = i; // TODO: change to real seq 
    mg_hdr.dbPort = storage_config.dst_port_list[i];
    SerializeMgHdr(buf, &mg_hdr);

    mg_terminate = mg_init;
    SerializeMgCtrlPkt(buf, &mg_terminate);
    
    client_sockets[0]->Send(buf, HDR_SIZE + (ADDR_SIZE + PORT_SIZE) * 2);
    client_sockets[0]->Recv(buf_reply);
    DeserializeMgHdr(buf_reply, &mg_hdr_reply);
    // assert(mg_hdr_reply.seq == mg_hdr.seq);
  } 
  
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
  std::cout << "Source Migration Agent Running Time (ms): " << duration.count() << std::endl;
  std::cout << "Total migration op:" << total_migration_op << std::endl;
  std::cout << "Total migration op time: " << total_migration_time << std::endl;

}


void MigrationManager::RunRPCServer(uint16_t dbPort, uint16_t server_port) {
  auto service = std::make_shared<KeyValueStoreServiceImpl>(src_req_agent->shard.at(dbPort)->redis, agent_config.src_ip, server_port);
  {
    const std::lock_guard<std::mutex> lock(insert_mutex_source);
    grpc_servers.emplace_back(service);
  }
  service->Run();
}

void MigrationManager::IPCServer() {
  char buf[BUFSIZE];
  uint16_t port = 0;
  while (true) {
    int recvlen = ipc_server_socket->Recv(buf);
    port = atoi(buf);
    migr_start_ports.insert(port);
    migration_start = true;
  }
}

void MigrationManager::StartSourceRPCServer() {

  thread_ipc = std::thread(&MigrationManager::IPCServer, this);

  // Pause dict rehashing 
  std::string res;
  unsigned long size_exp = 0;
  for (int i = 0; i < storage_config.src_port_list.size(); i++) {
    try {
      res = src_migr_agent->shard.at(storage_config.src_port_list[i])->redis->command<std::string>("migratestart");
    } catch (const ReplyError &err) {
      std::cout << err.what() << std::endl;
    } 
    if (res[0] >= '0' && res[0] <= '9') {
      size_exp = std::stoul(res);
      size_exp_vec.push_back(size_exp);
    }
  }

  uint32_t scale_id = 0;
  for (uint32_t j = 0; j < agent_config.redis_scale_num; j++) {
    for (uint32_t i = 0; i < storage_config.src_port_list.size(); i++) {
      threads_redis.emplace_back(&MigrationManager::SourceRedisHandlerThread, this, scale_id);
      scale_id ++;
    }
  }
  

  // Start Request Agent threads
  for (uint32_t i = 0; i < agent_config.req_thread_num; i++) {
    threads_req.emplace_back(&MigrationManager::SourceRequestHandlerThread, this, i);
  }
  
  // Start gRPC server for PriorityPull
  uint16_t server_port = 50051;
  for (auto &ins : src_req_agent->shard) { 
    threads_grpc.emplace_back(&MigrationManager::RunRPCServer, this, ins.first, server_port++);
    // One thread for one redis instance gRPC server
  }

  // Waiting for gRPC server threads to stop
  for (uint32_t i = 0; i < threads_grpc.size(); i++) {
    threads_grpc.at(i).join(); // will block here
  }

  for (uint32_t i = 0; i < storage_config.src_port_list.size(); i++) {
    threads_redis.at(i).join();
  }

  thread_ipc.join();
  
}

void MigrationManager::FinishSourceRPCServer() {
  // Shutdown gRPC server threads
  for (uint32_t i = 0; i < grpc_servers.size(); i++) {
    grpc_servers.at(i)->~KeyValueStoreServiceImpl();
  }
}

size_t MigrationManager::TryAppendMCRBegin(uint32_t thread_id, unsigned long size_exp, uint32_t * mcr_begin) {
  // piggyback all P_i in MCR_begin to client 
  unsigned long mcr_size = agent_config.migr_thread_num;
  unsigned long step = (1 << size_exp) / mcr_size;
  for (int j = 0; j < mcr_size; j++) {
    mcr_begin[j] = j * step;
  }
  return mcr_size;
}

void MigrationManager::SourceRedisHandlerThread(uint32_t thread_id) {

    uint32_t redis_id = thread_id % storage_config.src_port_list.size();
    uint32_t scale_id = thread_id / storage_config.src_port_list.size();

    char buf[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    struct mg_hdr_t mg_hdr;

    OptionalString val;
    struct mg_read_reply reply_pkt_read;
    struct mg_write_reply reply_pkt_write;

    size_t mcr_size = 0;
    uint32_t mcr[MAX_THREAD_NUM];
    uint32_t mcr_begin[MAX_THREAD_NUM];

    cmd_q bulk_data[REQ_PIPELINE];
    std::vector<cmd_q> cmds;
    int socket_id;
    uint64_t seq;
    uint8_t ver;

    auto client = InitRPCClient();

    bool mg_start_flag = false;

    auto client_timer = std::chrono::high_resolution_clock::now();
    uint64_t client_op = 0;

    while (true) {

        auto end = std::chrono::high_resolution_clock::now();
        auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - client_timer);
        if (time_elapsed.count() >= CHECK_PERIOD) {
          client_op = 0;
          client_timer = std::chrono::high_resolution_clock::now();
        }
      
        // if (migration_start == false || client_op < SRC_THRESHOLD_CLIENT) 
        {

          if (op_queue[storage_config.src_port_list[redis_id]].size_approx() > 0) {

              if (migr_start_ports.find(storage_config.src_port_list[redis_id]) != migr_start_ports.end()) {
                mg_start_flag = true;
              }
              size_t count = op_queue[storage_config.src_port_list[redis_id]].try_dequeue_bulk(bulk_data, REQ_PIPELINE);
              for (uint32_t i = 0; i < count; i++) { 
                  std::string op = bulk_data[i].op; // currently only read and write 
                  std::string key = bulk_data[i].key;
                  std::string val = bulk_data[i].val;
                  auto req_timer = std::chrono::high_resolution_clock::now();
                  src_req_agent->shard.at(storage_config.src_port_list[redis_id])->AppendCmd(scale_id, op, key, val);
                  auto req_timer_end = std::chrono::high_resolution_clock::now();
                  if (migration_start) {
                    total_req_op += 1;
                    total_req_time += std::chrono::duration_cast<std::chrono::microseconds>(req_timer_end - req_timer).count();
                  }

                  cmds.push_back(bulk_data[i]);
              }

              auto req_timer = std::chrono::high_resolution_clock::now();
              auto pipe_replies = src_req_agent->shard.at(storage_config.src_port_list[redis_id])->pipe.at(scale_id)->exec();
              auto req_timer_end = std::chrono::high_resolution_clock::now();
              if (migration_start) {
                total_req_time += std::chrono::duration_cast<std::chrono::microseconds>(req_timer_end - req_timer).count();
              }
              
              for (uint32_t i = 0; i < cmds.size(); i++) {
                  socket_id = cmds[i].socket_id;
                  seq = cmds[i].seq;
                  ver = cmds[i].ver;
                  if (cmds[i].op == "GET") {
                      reply_pkt_read = {0};
                      auto val = pipe_replies.get<OptionalString>(i);

                      if (val.has_value()) {
                          strncpy(reply_pkt_read.val, val.value().c_str(), std::min(VAL_SIZE, (int)val.value().size()));
                          reply_pkt_read.ver = ver | (server_type << 1) | 1; // source, sucess
                      }
                      else {
                          strncpy(reply_pkt_read.val, val_not_found, VAL_SIZE);
                          reply_pkt_read.ver = ver | (server_type << 1) | 0; // source, fail (key not found)
                      }

                      mg_hdr.op = _MG_READ_REPLY;
                      mg_hdr.dbPort = storage_config.src_port_list[redis_id];
                      mg_hdr.seq = seq;
                      SerializeMgHdr(buf, &mg_hdr);
                      
                      mcr_size = 0;
                      if (mg_start_flag && send_mcr_begin[socket_id]) {
                        mcr_size = TryAppendMCRBegin(thread_id, size_exp_vec.at(src_port_idx[mg_hdr.dbPort]), mcr);
                        if (mcr_size != 0) {
                          reply_pkt_read.ver |= 64;
                          SerializeReadReplyPkt(buf, &reply_pkt_read);
                          SerializeReadReplyMCRPkt(buf, mcr, mcr_size);
                          {
                            const std::lock_guard<std::mutex> lock(send_mcr_begin_mutex);
                            send_mcr_begin[socket_id] = false;
                          }
                        } 
                      }
                      else 
                        SerializeReadReplyPkt(buf, &reply_pkt_read);

                      server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply) + mcr_size * sizeof(*mcr));
                  }
                  else { // "SET"
                      auto res = pipe_replies.get<bool>(i);
                      reply_pkt_write = {0};
                      reply_pkt_write.ver = (server_type << 1) | (res > 0); 
                      
                      mg_hdr.op = _MG_WRITE_REPLY;
                      mg_hdr.dbPort = storage_config.src_port_list[redis_id];
                      mg_hdr.seq = seq;

                      SerializeMgHdr(buf, &mg_hdr);
                      mcr_size = 0;
                      if (mg_start_flag && send_mcr_begin[socket_id]) {
                        mcr_size = TryAppendMCRBegin(thread_id, size_exp_vec.at(src_port_idx[mg_hdr.dbPort]), mcr);
                        if (mcr_size != 0) {
                          reply_pkt_write.ver |= 64;
                          SerializeWriteReplyPkt(buf, &reply_pkt_write);
                          SerializeWriteReplyMCRPkt(buf, mcr, mcr_size);
                        } 
                        {
                          const std::lock_guard<std::mutex> lock(send_mcr_begin_mutex);
                          send_mcr_begin[socket_id] = false;
                        }
                      }
                      else {
                        SerializeWriteReplyPkt(buf, &reply_pkt_write);
                      }
                      server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_write_reply) + mcr_size * sizeof(*mcr));
                  }
              }
              std::vector<cmd_q>().swap(cmds);
          }
        }
    }
}

void MigrationManager::SourceRequestHandlerThread(uint32_t thread_id) {
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

    char key_pkt[KEY_SIZE + 1];
    char val_pkt[VAL_SIZE + 1];

    long long res = 0;
    int recvlen = 0;

    std::map<uint16_t, std::vector<cmd_q>> op_cmds;
    for (auto dbPort : storage_config.src_port_list) {
      op_cmds.emplace(dbPort, std::vector<cmd_q>());
    }
    cmd_q tmp_write;
    cmd_q tmp_read;
    
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
        // if (recvPkt % 1 == 0) {
          const std::lock_guard<std::mutex> lock(iomutex_source);
          std::cout << "Source Request Handler thread " << thread_id << ": Recv packet " << recvPkt << ", dbPort " << mg_hdr.dbPort << std::endl;
          std::cout << "total req op = " << total_req_op << std::endl;
          std::cout << "total req op time = " << total_req_time << std::endl;
        }


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
                res = src_req_agent->shard.at(mg_hdr.dbPort)->redis->del(std::string(key_pkt));
                reply_pkt_delete = {0};
                reply_pkt_delete.ver = (server_type << 1) | (res > 0); 
                
                SerializeMgHdr(buf, &mg_hdr);

                mcr_size = 0;
                if (send_mcr_begin[src_port_idx[mg_hdr.dbPort]]) {
                  mcr_size = TryAppendMCRBegin(thread_id, size_exp_vec.at(src_port_idx[mg_hdr.dbPort]), mcr);
                  if (mcr_size != 0) {
                    reply_pkt_delete.ver |= 64;
                    SerializeDeleteReplyPkt(buf, &reply_pkt_delete);
                    SerializeWriteReplyMCRPkt(buf, mcr, mcr_size);
                  } 
                  {
                    const std::lock_guard<std::mutex> lock(send_mcr_begin_mutex);
                    send_mcr_begin[src_port_idx[mg_hdr.dbPort]] = false;
                  }
                }
                else 
                  SerializeDeleteReplyPkt(buf, &reply_pkt_delete);

                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(mg_delete_reply) + mcr_size);
                break;
            
            case _MG_MULTI_READ: 

                DeserializeMultiReadPkt(buf, &pkt_multi_read);
                reply_pkt_multi_read = {0};
                reply_pkt_read.bitmap = pkt_multi_read.bitmap;
                mg_hdr.op = _MG_MULTI_READ_REPLY;

                // deal with mget 
                keys.clear();
                values.clear();
                for (uint8_t i = 0; i < 4; i++) {
                  if (pkt_multi_read.bitmap & (1 << i)) {
                    DeserializeKey(key_pkt, pkt_multi_read.ver_key[i].key);
                    keys.emplace_back(std::string(key_pkt));
                  }
                }

                src_req_agent->shard.at(mg_hdr.dbPort)->redis->mget(keys.begin(), keys.end(), std::back_inserter(values));
              
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
                    }
                  }
                }

                SerializeMgHdr(buf, &mg_hdr);
                
                
                mcr_size  = 0;
                if (send_mcr_begin[src_port_idx[mg_hdr.dbPort]]) {
                  mcr_size = TryAppendMCRBegin(thread_id, size_exp_vec.at(src_port_idx[mg_hdr.dbPort]), mcr);
                  if (mcr_size != 0) {
                    reply_pkt_multi_read.ver_val[0].ver |= 64;
                    SerializeMultiReadReplyPkt(buf, &reply_pkt_multi_read);
                    SerializeWriteReplyMCRPkt(buf, mcr, mcr_size);
                  } 
                  {
                    const std::lock_guard<std::mutex> lock(send_mcr_begin_mutex);
                    send_mcr_begin[src_port_idx[mg_hdr.dbPort]] = false;
                  }
                }
                else {
                  SerializeMultiReadReplyPkt(buf, &reply_pkt_multi_read);
                }

                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_multi_read_reply) + mcr_size * sizeof(*mcr));
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
                src_req_agent->shard.at(mg_hdr.dbPort)->redis->mset(key_values.begin(), key_values.end());
                
                reply_pkt_multi_write.bitmap = pkt_multi_write.bitmap;
                for (uint8_t i = 0; i < 4; i++) {
                  if (pkt_multi_write.bitmap & (1 << i)) {
                    reply_pkt_multi_write.ver[i] = (server_type << 1) | 1;
                  }
                }
                
                SerializeMgHdr(buf, &mg_hdr);

                mcr_size  = 0;
                if (send_mcr_begin[src_port_idx[mg_hdr.dbPort]]) {
                  mcr_size = TryAppendMCRBegin(thread_id, size_exp_vec.at(src_port_idx[mg_hdr.dbPort]), mcr);
                  if (mcr_size != 0) {
                    reply_pkt_multi_read.ver_val[0].ver |= 64;
                    SerializeMultiWriteReplyPkt(buf, &reply_pkt_multi_write);
                    SerializeWriteReplyMCRPkt(buf, mcr, mcr_size);
                  } 
                  {
                    const std::lock_guard<std::mutex> lock(send_mcr_begin_mutex);
                    send_mcr_begin[src_port_idx[mg_hdr.dbPort]] = false;
                  }
                }
                else {
                  SerializeMultiWriteReplyPkt(buf, &reply_pkt_multi_write);
                }

                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_multi_write_reply) + mcr_size * sizeof(*mcr));
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
                src_req_agent->shard.at(mg_hdr.dbPort)->redis->del(keys.begin(), keys.end());
                reply_pkt_multi_delete.bitmap = pkt_multi_delete.bitmap;
                for (uint8_t i = 0; i < 4; i++) {
                  if (pkt_multi_delete.bitmap & (1 << i)) {
                    reply_pkt_multi_delete.ver[i] = (server_type << 1) | 1; // minor TODO: change to del command return value
                  }
                }
                
                SerializeMgHdr(buf, &mg_hdr);

                mcr_size  = 0;
                if (send_mcr_begin[src_port_idx[mg_hdr.dbPort]]) {
                  mcr_size = TryAppendMCRBegin(thread_id, size_exp_vec.at(src_port_idx[mg_hdr.dbPort]), mcr);
                  if (mcr_size != 0) {
                    reply_pkt_multi_read.ver_val[0].ver |= 64;
                    SerializeMultiDeleteReplyPkt(buf, &reply_pkt_multi_delete);
                    SerializeWriteReplyMCRPkt(buf, mcr, mcr_size);
                  } 
                  {
                    const std::lock_guard<std::mutex> lock(send_mcr_begin_mutex);
                    send_mcr_begin[src_port_idx[mg_hdr.dbPort]] = false;
                  }
                }
                else 
                  SerializeMultiDeleteReplyPkt(buf, &reply_pkt_multi_delete);

                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_multi_delete_reply) + mcr_size * sizeof(*mcr));
                break;
            */
            default:
                std::cout << "Currently not supported request type" << std::endl;
        }    
        for (auto dbPort : storage_config.src_port_list) {
          if (op_cmds[dbPort].size() % REQ_PIPELINE == 0) {
            op_queue[dbPort].enqueue_bulk(op_cmds[dbPort].data(), op_cmds[dbPort].size());
            std::vector<cmd_q>().swap(op_cmds[dbPort]);
          }
        }  
    }
}