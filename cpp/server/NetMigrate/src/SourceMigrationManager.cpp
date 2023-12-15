#include <iostream>
#include <string>
#include <mutex>
#include <cstring>
#include <algorithm>

#include "MigrationManager.h"

#define DEBUG 0

std::mutex iomutex_source;
std::mutex insert_mutex_source;

// Source Agent 

void MigrationManager::ScanKVPairsThread(unsigned long size_exp, uint16_t dbPort, uint32_t thread_id) {

    
    std::chrono::time_point<std::chrono::system_clock> start, end;
  
    start = std::chrono::system_clock::now();
    uint64_t debug_count = 0;
    uint64_t debug_scan_count = 0;
    

    unsigned long group_id_size_exp, remain_size_exp, step, group_id_st, group_id_end;
    // Calculating group_id_st and group_id_end
    group_id_size_exp = std::min(size_exp, (unsigned long) MAX_SIZE_EXP);
    remain_size_exp = size_exp - group_id_size_exp;
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

/*
    if (thread_id == 0)
      migration_op_start = std::chrono::steady_clock::now();
*/

    while (current_group_id < group_id_end) {

      /*
      if (thread_id == 0) {
        auto migration_op_end = std::chrono::steady_clock::now();
        auto time_span = std::chrono::duration_cast<std::chrono::milliseconds>(migration_op_end - migration_op_start);
        if (time_span.count() >= CHECK_PERIOD) {
          std::cout << "total_migration_op/s=" << total_migration_op << std::endl;
          total_migration_op = 0;
          migration_op_start = std::chrono::steady_clock::now();
        }
      }
      */


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
      
      /*
      auto end = std::chrono::high_resolution_clock::now();
      auto time_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - migration_timer);
      if (time_elapsed.count() >= CHECK_PERIOD) {
        std::cout << "migration_op/s=" << migration_op << std::endl;
        migration_op = 0;
        migration_timer = std::chrono::high_resolution_clock::now();
      }
      */

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
  // pkt_buffer[src_port_idx[dbPort]].enqueue(mg_data);
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
    // pkt_buffer[src_port_idx[dbPort]].enqueue(mg_data);
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
    // pkt_buffer[src_port_idx[dbPort]].enqueue(mg_data);
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

  size_t buf_size = 0;
  /*
  for (int i = 0; i < src_port_idx.size(); i++) {
    buf_size += pkt_buffer[i].size_approx();
  }
  */
  buf_size = pkt_buffer[0].size_approx();


  
  std::chrono::time_point<std::chrono::system_clock> start, end;
  start = std::chrono::system_clock::now();
  uint64_t debug_count = 0;
  

  
  // while (done_scan  < storage_config.src_port_list.size() * agent_config.migr_thread_num || buf_size) {
  while (done_scan  < agent_config.migr_thread_num || buf_size) {
    
    
    end = std::chrono::system_clock::now();
    auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds> (end - start).count();
    if (elapsed_seconds == 1) {
      // std::cout << "dequeue: " << debug_count << std::endl;
      debug_count = 0;
      start = std::chrono::system_clock::now();
    }
    


    // for (int i = 0; i < src_port_idx.size(); i++) {
    for (int i = 0; i < 1; i++) {
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
    // for (int i = 0; i < src_port_idx.size(); i++) {
    for (int i = 0; i < 1; i++) {
      buf_size += pkt_buffer[i].size_approx();
    }
  }

  {
    const std::lock_guard<std::mutex> lock(iomutex_source);
    std::cout << "SourceSendPktThread " << thread_id << ": final packet sequence number " << mg_hdr.seq << std::endl;
  }
  
}

void MigrationManager::SourceMigrationManager() {

  total_migration_op = 0;

  uint32_t scale_id = 0;
  for (uint32_t j = 0; j < agent_config.redis_scale_num; j++) {
    for (uint32_t i = 0; i < storage_config.src_port_list.size(); i++) {
      threads_redis.emplace_back(&MigrationManager::SourceRedisHandlerThread, this, scale_id);
      scale_id++;
    }
  }
 

  // Request Agent threads
  for (uint32_t i = 0; i < agent_config.req_thread_num; i++) {
    threads_req.emplace_back(&MigrationManager::SourceRequestHandlerThread, this, i);
  }
  

  std::this_thread::sleep_for(std::chrono::seconds(300));


  char buf[BUFSIZE];
  char buf_reply[BUFSIZE];
  struct mg_hdr_t mg_hdr;
  struct mg_hdr_t mg_hdr_reply;
  struct mg_ctrl mg_init;
  struct mg_ctrl mg_terminate;

  auto start = std::chrono::high_resolution_clock::now();

  std::atomic_int done_scan = 0;
  std::vector<unsigned long> size_exp_vec;

  char ipc_buf[BUFSIZE];


  // Send _MG_INIT packet to switch and destination;
  // for (int i = 0; i < storage_config.src_port_list.size(); i++) {
  for (int i = 0; i < storage_config.src_port_list.size(); i++) { // one agent only migrates first replica in a shard

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

    unsigned long group_id_size_exp = std::min(size_exp, (unsigned long) MAX_SIZE_EXP);

    migration_step.at(i) = (1 << group_id_size_exp) / agent_config.migr_thread_num;
    migration_size_exp_vec.at(i) = size_exp;

    
    mg_hdr.op = _MG_INIT;
    mg_hdr.seq = i;
    mg_hdr.dbPort = storage_config.dst_port_list[i];
    SerializeMgHdr(buf, &mg_hdr);

    mg_init.srcAddr = ipv4Addr_to_i32(agent_config.src_ip);
    mg_init.srcPort = storage_config.src_port_list[i];
    mg_init.dstAddr = ipv4Addr_to_i32(agent_config.dst_ip);
    mg_init.dstPort = storage_config.dst_port_list[i];
    mg_init.size_exp = (uint8_t) size_exp;
    mg_init.remain_exp = (uint8_t)(size_exp - group_id_size_exp);
    size_exp_vec.push_back(size_exp);
    SerializeMgCtrlPkt(buf, &mg_init);

    client_sockets[0]->Send(buf, HDR_SIZE + (ADDR_SIZE + PORT_SIZE) * 2 + 2);

// #if DEBUG == 1
    std::cout << "send mg init pkt: " << mg_init.srcPort << " " << mg_init.dstPort << " " << mg_init.srcAddr << " " << mg_init.dstAddr << \
                "size_exp = " << (uint32_t) mg_init.size_exp << " remain_exp = " << (uint32_t) mg_init.remain_exp << std::endl;
// #endif

    client_sockets[0]->Recv(buf_reply);

    // Send ipc message
    std::string tmp_port = std::to_string(storage_config.src_port_list[i]);
    strcpy(ipc_buf, tmp_port.c_str());
    ipc_client_socket->Send(ipc_buf, tmp_port.size());
    migration_start = true;

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

    mg_terminate.srcAddr = ipv4Addr_to_i32(agent_config.src_ip);
    mg_terminate.srcPort = htons(storage_config.src_port_list[i]);
    mg_terminate.dstAddr = ipv4Addr_to_i32(agent_config.dst_ip);
    mg_terminate.dstPort = htons(storage_config.dst_port_list[i]);
    mg_terminate.size_exp = (uint8_t) size_exp_vec[i];
    unsigned long group_id_size_exp = std::min(size_exp_vec[i], (unsigned long) MAX_SIZE_EXP);
    mg_terminate.remain_exp = (uint8_t)(size_exp_vec[i] - group_id_size_exp);
    
    SerializeMgCtrlPkt(buf, &mg_terminate);
    
    client_sockets[0]->Send(buf, HDR_SIZE + (ADDR_SIZE + PORT_SIZE) * 2 + 2);
    client_sockets[0]->Recv(buf_reply);
    DeserializeMgHdr(buf_reply, &mg_hdr_reply);
    // assert(mg_hdr_reply.seq == mg_hdr.seq);
  } 
  
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
  std::cout << "Source Migration Agent Running Time (ms): " << duration.count() << std::endl;
  std::cout << "Source Extra Bandwidth Usage: " << extra_source_bandwidth << " (Bytes)" << std::endl;
  std::cout << "Total migration op:" << total_migration_op << std::endl;
  std::cout << "Total migration op time: " << total_migration_time << std::endl;

  for (uint32_t i = 0; i < threads_req.size(); i++) {
    threads_req.at(i).join();
  }

  for (uint32_t i = 0; i < threads_redis.size(); i++) {
    threads_redis.at(i).join();
  }
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
  }
}

void MigrationManager::StartSourceRPCServer() {

  thread_ipc = std::thread(&MigrationManager::IPCServer, this);

  // Pause dict rehashing 
  std::string res;
  for (int i = 0; i < storage_config.src_port_list.size(); i++) {
    try {
      res = src_migr_agent->shard.at(storage_config.src_port_list[i])->redis->command<std::string>("migratestart");
    } catch (const ReplyError &err) {
      std::cout << err.what() << std::endl;
      return ;
    } 
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

  thread_ipc.join();
  
}

void MigrationManager::FinishSourceRPCServer() {
  // Shutdown gRPC server threads
  for (uint32_t i = 0; i < grpc_servers.size(); i++) {
    grpc_servers.at(i)->~KeyValueStoreServiceImpl();
  }
}

void MigrationManager::SourceRedisHandlerThread(uint32_t thread_id) {
    uint32_t redis_id = thread_id % src_port_idx.size();
    uint32_t scale_id = thread_id / src_port_idx.size();
    
    char buf[BUFSIZE], buf_reply[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    uint32_t recvPkt = 0;
    struct mg_hdr_t mg_hdr;

    OptionalString val;
    struct mg_read pkt_read;
    struct mg_read_reply reply_pkt_read;
    struct mg_write pkt_write;
    struct mg_write_reply reply_pkt_write;
    struct mg_delete pkt_delete;
    struct mg_delete_reply reply_pkt_delete;

    struct mg_multi_read pkt_multi_read;
    // struct mg_multi_read_reply reply_pkt_multi_read;
    struct mg_multi_write pkt_multi_write;
    // struct mg_multi_write_reply reply_pkt_multi_write;
    struct mg_multi_delete pkt_multi_delete;
    // struct mg_multi_delete_reply reply_pkt_multi_delete;

    char key_pkt[KEY_SIZE + 1];
    char val_pkt[VAL_SIZE + 1];

    long long res = 0;
    int recvlen = 0;

    cmd_q bulk_data[REQ_PIPELINE];
    std::vector<cmd_q> cmds;
    int socket_id;
    uint64_t seq;
    uint8_t ver;
    uint64_t miss = 0;

    auto client = InitRPCClient();

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
          if (op_queue[storage_config.src_port_list[redis_id]].size_approx() > 0) {
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
              
              client_op += cmds.size();

              for (uint32_t i = 0; i < cmds.size(); i++) {
                  socket_id = cmds[i].socket_id;
                  seq = cmds[i].seq;
                  ver = cmds[i].ver;
                  if (cmds[i].op == "GET") {
                      reply_pkt_read = {0};
                      auto val = pipe_replies.get<OptionalString>(i);

                      mg_hdr.op = _MG_READ_REPLY;
                      mg_hdr.dbPort = storage_config.src_port_list[redis_id];
                      mg_hdr.seq = seq;

                      if (val.has_value()) {
                          strncpy(reply_pkt_read.val, val.value().c_str(), std::min(VAL_SIZE, (int)val.value().size()));
                          reply_pkt_read.ver = ver | (server_type << 1) | 1; // source, sucess
                      } 
                      else {
                          // miss++;
                          strncpy(reply_pkt_read.val, val_not_found, VAL_SIZE);
                          reply_pkt_read.ver = pkt_read.ver | (server_type << 1) | 0; // source, fail (key not found)
                      }    

                      SerializeMgHdr(buf, &mg_hdr);
                      SerializeReadReplyPkt(buf, &reply_pkt_read);
                      
                      server_sockets[socket_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply));
                  }
                  else { // "SET"
                      auto res = pipe_replies.get<bool>(i);
                      reply_pkt_write = {0};
                      reply_pkt_write.ver = ver | (server_type << 1) | (res > 0); 
                      
                      mg_hdr.op = _MG_WRITE_REPLY;
                      mg_hdr.dbPort = storage_config.src_port_list[redis_id];
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
}

/*
void MigrationManager::EnqueueDataPktLog() {
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
*/

inline bool MigrationManager::check_late_group_id(uint16_t dbPort, uint32_t group_id) {
  uint32_t db_idx = src_port_idx[dbPort];
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

inline bool MigrationManager::check_wrong_group_id(uint16_t dbPort, uint32_t group_id) {
  uint32_t db_idx = src_port_idx[dbPort];
  for (uint32_t i = 0; i < group_complete_pkt_sent.at(db_idx).size(); i++) {
    if (group_id >= migration_step.at(db_idx) * i && group_id < migration_step.at(db_idx) * (i+1)) {
      if (group_complete_pkt_thread.at(db_idx)[i] && group_id <= group_complete_pkt_sent.at(db_idx)[i]) {
        // std::cout << group_id << " " << group_start_pkt_sent.at(db_idx)[i] << std::endl;
        return true;
      }
    }
  }
  return false;
}

inline uint32_t MigrationManager::get_group_id(uint16_t dbPort, char * key) {
  uint32_t size_exp = migration_size_exp_vec.at(src_port_idx[dbPort]);
  uint32_t dict_mask = (1 << size_exp) - 1;
  uint32_t group_id_size_exp = std::min(size_exp, (uint32_t)MAX_SIZE_EXP);
  uint32_t remain_size_exp = size_exp - group_id_size_exp;
  uint32_t bucket_id = crc64(0, (const unsigned char*) key, KEY_SIZE) & dict_mask;
  return (bucket_id >> remain_size_exp);
}

void MigrationManager::SourceRequestHandlerThread(uint32_t thread_id) {
    char buf[BUFSIZE], buf_reply[BUFSIZE];
    memset(buf, 0, sizeof(char) * BUFSIZE);

    uint32_t recvPkt = 0;
    struct mg_hdr_t mg_hdr;

    OptionalString val;
    struct mg_read pkt_read;
    struct mg_read_reply reply_pkt_read;
    struct mg_write pkt_write;
    struct mg_write_reply reply_pkt_write;
    struct mg_delete pkt_delete;
    struct mg_delete_reply reply_pkt_delete;

    struct mg_multi_read pkt_multi_read;
    // struct mg_multi_read_reply reply_pkt_multi_read;
    struct mg_multi_write pkt_multi_write;
    // struct mg_multi_write_reply reply_pkt_multi_write;
    struct mg_multi_delete pkt_multi_delete;
    // struct mg_multi_delete_reply reply_pkt_multi_delete;

    char key_pkt[KEY_SIZE + 1];
    char val_pkt[VAL_SIZE + 1];

    long long res = 0;
    int recvlen = 0;

    std::map<uint16_t, std::vector<cmd_q>> op_cmds;
    cmd_q tmp_write;
    cmd_q tmp_read;

    for (auto dbPort : storage_config.src_port_list) {
      op_cmds.emplace(dbPort, std::vector<cmd_q>());
    }

    uint32_t group_id = 0;
    uint32_t late = 0;
    uint32_t mirror = 0;
    uint32_t wrong = 0;
    // log_cmd_q tmp_log_q;
    
    while (true) {
        if ((recvlen = server_sockets[thread_id]->Recv(buf)) < 0)
        {
            std::cout << "recvlen = " << recvlen << std::endl;
            continue;
        }
        
        recvPkt++;
        if(recvPkt % MILLION == 0) 
        {
          
          const std::lock_guard<std::mutex> lock(iomutex_source);
          std::cout << "RequestHandler thread " << thread_id << ": Recv packet " << recvPkt << std::endl;
          std::cout << "late per 1000000 reqs = " << late << std::endl;
          std::cout << "wrong per 1000000 reqs = " << wrong << std::endl;
          std::cout << "mirror per 1000000 reqs = " << mirror << std::endl;
          extra_source_bandwidth += late * (KEY_SIZE + VAL_SIZE);
          wrong = late = mirror = 0;
          std::cout << "extra source bandwidth = " << extra_source_bandwidth << " (Bytes)" << std::endl;
          std::cout << "total req op = " << total_req_op << std::endl;
          std::cout << "total req op time = " << total_req_time << std::endl;
        }
            

        // Deserialize packet and handle client requests to Redis
        DeserializeMgHdr(buf, &mg_hdr);
    
        switch (mg_hdr.op) {
            case _MG_READ: 
                DeserializeReadPkt(buf, &pkt_read);
                assert(pkt_read.bitmap == 0x1); // switch will not change bitmap of single-op
                reply_pkt_read = {0};
                reply_pkt_read.bitmap = pkt_read.bitmap;
                mg_hdr.op = _MG_READ_REPLY;
                DeserializeKey(key_pkt, pkt_read.key);
                tmp_read.op = "GET";
                tmp_read.key = std::string(key_pkt);
                tmp_read.val = "";
                tmp_read.socket_id = thread_id;
                tmp_read.seq = mg_hdr.seq;
                tmp_read.ver = pkt_read.ver;
                
                group_id = get_group_id(mg_hdr.dbPort, key_pkt);
                if (pkt_read.ver & 4) {
                  mirror++;
                }
                else {
                  // std::cout << "check = " << check_late_group_id(mg_hdr.dbPort, group_id) << std::endl;
                  if (check_late_group_id(mg_hdr.dbPort, group_id)) {
                    late++;
                  }
                }
                if (check_wrong_group_id(mg_hdr.dbPort, group_id)) {
                  wrong++;
                }
                
                op_cmds[mg_hdr.dbPort].push_back(tmp_read);
                break;

            case _MG_WRITE:
                DeserializeWritePkt(buf, &pkt_write);
                assert(pkt_write.bitmap == 0x1); // switch will not change bitmap of single-op
                reply_pkt_write = {0};
                reply_pkt_write.bitmap = pkt_write.bitmap;
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

                
                group_id = get_group_id(mg_hdr.dbPort, key_pkt);
                // std::cout << "check = " << check_late_group_id(mg_hdr.dbPort, group_id) << std::endl;
                if (check_late_group_id(mg_hdr.dbPort, group_id)) {
                  late++;
                }
                if (check_wrong_group_id(mg_hdr.dbPort, group_id)) {
                  wrong++;
                }
                
                // tmp_log_q.dbPort = mg_hdr.dbPort; // source port
                // tmp_log_q.key = tmp_write.key;
                // tmp_log_q.val = tmp_write.val;
                // log_cmds.enqueue(tmp_log_q);
                break;
/*
            case _MG_DELETE:
                
                DeserializeDeletePkt(buf, &pkt_delete);
                assert(pkt_delete.bitmap == 0x1); // switch will not change bitmap of single-op
                mg_hdr.op = _MG_DELETE_REPLY;
                DeserializeKey(key_pkt, pkt_delete.key);
                res = src_req_agent->shard.at(mg_hdr.dbPort)->redis->del(std::string(key_pkt));
                reply_pkt_delete = {0};
                reply_pkt_delete.bitmap = pkt_delete.bitmap;
                reply_pkt_delete.ver = (server_type << 1) | (res > 0); 
                
                SerializeMgHdr(buf, &mg_hdr);
                SerializeDeleteReplyPkt(buf, &reply_pkt_delete);
                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(mg_delete_reply));
                break;
            
            case _MG_MULTI_READ: 

                DeserializeMultiReadPkt(buf, &pkt_multi_read);
                // only key_s1 will be handled after switch processing, so it's same as _MG_READ
                mg_hdr.op = _MG_READ_REPLY;
                DeserializeKey(key_pkt, pkt_multi_read.ver_key[0].key);
                val = src_req_agent->shard.at(mg_hdr.dbPort)->redis->get(std::string(key_pkt));
                reply_pkt_read = {0};
                reply_pkt_read.bitmap = pkt_multi_read.bitmap;
                if (val.has_value()) {
                    strncpy(reply_pkt_read.val, val.value().c_str(), std::min(VAL_SIZE, (int)val.value().size()));
                    reply_pkt_read.ver = pkt_multi_read.ver_key[0].ver | (server_type << 1) | 1; // source, sucess
                }
                else {
                    strncpy(reply_pkt_read.val, val_not_found, VAL_SIZE);
                    reply_pkt_read.ver = pkt_multi_read.ver_key[0].ver | (server_type << 1) | 0; // source, fail (key not found)
                }

                SerializeMgHdr(buf, &mg_hdr);
                SerializeReadReplyPkt(buf, &reply_pkt_read);
                
                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_read_reply));
                break;
            
            case _MG_MULTI_WRITE: 
                DeserializeMultiWritePkt(buf, &pkt_multi_write);
                mg_hdr.op = _MG_WRITE_REPLY;
                DeserializeKey(key_pkt, &pkt_multi_write.keys[0]);
                strncpy(val_pkt, &pkt_multi_write.vals[0], VAL_SIZE);
                reply_pkt_write = {0};
                reply_pkt_write.bitmap = pkt_multi_write.bitmap;
                reply_pkt_write.ver = (server_type << 1) | (res > 0); 
                
                SerializeMgHdr(buf, &mg_hdr);
                SerializeWriteReplyPkt(buf, &reply_pkt_write);
                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(struct mg_write_reply));
                break;

            case _MG_MULTI_DELETE: 
                DeserializeMultiDeletePkt(buf, &pkt_multi_delete);
                mg_hdr.op = _MG_DELETE_REPLY;
                DeserializeKey(key_pkt, &pkt_multi_delete.keys[0]);
                res = src_req_agent->shard.at(mg_hdr.dbPort)->redis->del(std::string(key_pkt));
                reply_pkt_delete = {0};
                reply_pkt_delete.bitmap = pkt_multi_delete.bitmap;
                reply_pkt_delete.ver = (server_type << 1) | (res > 0); 
                
                SerializeMgHdr(buf, &mg_hdr);
                SerializeDeleteReplyPkt(buf, &reply_pkt_delete);
                server_sockets[thread_id]->Send(buf, HDR_SIZE + sizeof(mg_delete_reply));
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