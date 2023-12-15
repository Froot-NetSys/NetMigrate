#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <regex>
#include "MigrationManager.h"

using namespace std;

int main(int argc, char *argv[]) {

  if (argc < 13) {
    std::cerr << "Usage: Source_Migration server_type[destination, source] src_trans_ip dst_trans_ip src_redis_port dst_redis_port src_migration_agent_start_port dst_migration_agent_start_port src_kv_start_port dst_kv_start_port migr_thread_num migr_pkt_thread_num redis_scale_num" << std::endl;
    return 1;
  }

  std::string server_type = argv[1];
  if (server_type != "destination" && server_type != "source") {
    std::cerr << "server_type should be \"destination\" or \"source\"]" << std::endl;
    return 1;
  }

  std::string src_trans_ip = argv[2];
  std::string dst_trans_ip = argv[3];

  regex reg("[,]+");
  std::string str = argv[4];
  sregex_token_iterator iter(str.begin(), str.end(), reg, -1);
  sregex_token_iterator end;
  std::vector<std::string> src_redis_ports(iter, end);
  std::vector<uint16_t> src_redis_port_list;
  for (auto & port : src_redis_ports) {
    src_redis_port_list.push_back(stoi(port));
  }
  assert(src_redis_port_list.size() == 1);

  str = argv[5];
  sregex_token_iterator dst_iter(str.begin(), str.end(), reg, -1);
  sregex_token_iterator dst_end;
  std::vector<std::string> dst_redis_ports(dst_iter, dst_end);
  std::vector<uint16_t> dst_redis_port_list;
  for (auto & port : dst_redis_ports) {
    dst_redis_port_list.push_back(stoi(port));
  }
  assert(dst_redis_port_list.size() == 1);

  uint16_t src_migration_agent_start_port = atoi(argv[6]);
  uint16_t dst_migration_agent_start_port = atoi(argv[7]);

  uint16_t src_kv_start_port = atoi(argv[8]);
  uint16_t dst_kv_start_port = atoi(argv[9]);

  uint32_t migr_thread_num = atoi(argv[10]);
  uint32_t migr_pkt_thread_num = atoi(argv[11]);
  uint32_t req_thread_num = atoi(argv[12]);
  uint32_t redis_scale_num = atoi(argv[13]);
  uint16_t client_start_port = atoi(argv[14]);

  struct StorageConfig redis_config = StorageConfig(src_redis_port_list[0], dst_redis_port_list[0]);
  struct AgentConfig agent_config = AgentConfig(src_trans_ip, 
                                                dst_trans_ip, 
                                                src_migration_agent_start_port, 
                                                dst_migration_agent_start_port,
                                                src_kv_start_port,
                                                dst_kv_start_port,
                                                migr_thread_num, 
                                                migr_pkt_thread_num,
                                                req_thread_num,
                                                redis_scale_num,
                                                client_start_port);
  auto source_migration = MigrationManager(redis_config, agent_config, server_type);

  if (server_type == "destination") {
    source_migration.DestinationMigrationManager();
    // source_migration.TestReceiveFile();
  }
  else if (server_type == "source") {
    source_migration.SourceMigrationManager(); 
    // source_migration.TestSendFile();
  }

  return 0;
}