#include <iostream>
#include <string>
#include <regex>

#include "server_agent.h"

int main(int argc, char *argv[]) {
    string redis_ip;
    uint16_t agent_start_port = 0;
    string server_type;
    map<int, RedisAgent *> redis_agent;
    uint32_t thread_num = 0;
    vector<std::shared_ptr<ServerSocket>> sockets;

    if (argc < 6) {
        std::cerr << "Usage: server_agent server_type(destination/source) redis_ip redis_port_1[,redis_port_2,redis_port_3,...] trans_ip agent_start_port thread_num" << std::endl;
        return 1;
    }

    server_type = argv[1];
    redis_ip = argv[2];
    
    regex reg("[,]+");
    std::string str = argv[3];
    sregex_token_iterator iter(str.begin(), str.end(), reg, -1);
    sregex_token_iterator end;
    vector<std::string> redis_ports(iter, end);
    
    std::string trans_ip = argv[4];
    agent_start_port = atoi(argv[5]);
    thread_num = atoi(argv[6]);

    auto server_agent = ServerAgent((server_type == "destination"), redis_ip, redis_ports, trans_ip, agent_start_port, thread_num);
        
    server_agent.RequestHandler();
    return 0;
}

