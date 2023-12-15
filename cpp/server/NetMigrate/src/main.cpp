#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <regex>
#include "MigrationManager.h"

using namespace std;

int main(int argc, char *argv[]) {

  if (argc < 7) {
    std::cerr << "Usage: Rocksteady server_type[destination, source] src_redis_ip [src_redis_port,...] src_migration_agent_start_port dst_redis_ip [dst_redis_port,...] dst_migration_agent_start_port src_trans_ip dst_trans_ip migr_thread_num migr_pkt_thread_num req_thread_num redis_scale_num" << std::endl;
    return 1;
  }

  std::string server_type = argv[1];
  if (server_type != "destination" && server_type != "source_pull" && server_type != "source_push") {
    std::cerr << "server_type should be \"destination\" or \"source_pull\" or \"source_push\"]" << std::endl;
    return 1;
  }

  std::string src_redis_ip = argv[2];

  regex reg("[,]+");
  std::string str = argv[3];
  sregex_token_iterator iter(str.begin(), str.end(), reg, -1);
  sregex_token_iterator end;
  std::vector<std::string> src_redis_ports(iter, end);
  std::vector<uint16_t> src_redis_port_list;
  for (auto & port : src_redis_ports) {
    src_redis_port_list.push_back(stoi(port));
  }
  uint16_t src_migration_agent_start_port = atoi(argv[4]);

  std::string dst_redis_ip = argv[5];

  str = argv[6];
  sregex_token_iterator dst_iter(str.begin(), str.end(), reg, -1);
  sregex_token_iterator dst_end;
  std::vector<std::string> dst_redis_ports(dst_iter, dst_end);
  std::vector<uint16_t> dst_redis_port_list;
  for (auto & port : dst_redis_ports) {
    dst_redis_port_list.push_back(stoi(port));
  }

  uint16_t dst_migration_agent_start_port = atoi(argv[7]);

  std::string src_trans_ip = argv[8];
  std::string dst_trans_ip = argv[9];
  uint32_t agent_thread_num = atoi(argv[10]);
  uint32_t migr_pkt_thread_num = atoi(argv[11]);
  uint32_t req_thread_num = atoi(argv[12]);
  uint32_t redis_scale_num = atoi(argv[13]);


  struct StorageConfig redis_config = StorageConfig(src_redis_ip, src_redis_port_list, dst_redis_ip, dst_redis_port_list);
  struct AgentConfig agent_config = AgentConfig(src_trans_ip, dst_trans_ip,
                                                src_migration_agent_start_port, dst_migration_agent_start_port, CLIENT_PORT_START,
                                                agent_thread_num, migr_pkt_thread_num, req_thread_num, 1, redis_scale_num);
  auto NetMigrate_migration = MigrationManager(redis_config, agent_config, server_type);
  
  if (server_type == "destination") {
    NetMigrate_migration.DestinationMigrationManager();
  }
  else if (server_type == "source_push") {
    NetMigrate_migration.SourceMigrationManager(); // This will shutdown SourceRPCServer
  }
  else if (server_type == "source_pull") {
    NetMigrate_migration.StartSourceRPCServer();
    std::this_thread::sleep_for(std::chrono::seconds(500)); // Assume after this period, migration is finished.
    // NetMigrate_migration.FinishSourceRPCServer();
  }

  return 0;
}
