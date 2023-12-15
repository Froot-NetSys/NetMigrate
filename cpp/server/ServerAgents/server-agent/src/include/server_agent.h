#pragma once
#include <vector>
#include <map>
#include <thread>

#include "redis_agent.h"
#include "pkt_headers.h"
#include "socket.h"
#include "concurrentqueue.h" // thread-safe queue

using namespace std;

#define MILLION 1000000

class ServerAgent {
public:
    ServerAgent(bool server_type, string &redis_ip, vector<std::string> & redis_ports, string &trans_ip, uint16_t agent_start_port, uint32_t thread_num): 
        server_type(server_type), agent_start_port(agent_start_port), thread_num(thread_num) {
        
        for (auto &redis_port: redis_ports) {
            redis_agents.emplace(make_pair(stoi(redis_port), make_shared<RedisAgent>(redis_ip, stoi(redis_port))));
        }

        for (uint32_t i = 0; i < thread_num; i++) {
            cout << "Using agent_port: " << agent_start_port + i << endl;
            server_sockets.emplace_back(make_shared<ServerSocket>(agent_start_port + i));
        }

        for (int i = 0; i < VAL_SIZE; i++)
            val_not_found[i] = 0xFF;

    }
    ~ServerAgent() { }
   
    void RequestHandler();

private:
    void RequestHandlerThread(uint32_t thread_id);

    bool server_type;

    string redis_ip;
    map<int, shared_ptr<RedisAgent>> redis_agents;
    
    string trans_ip;
    uint16_t agent_start_port;
    vector<shared_ptr<ServerSocket>> server_sockets;

    uint32_t thread_num;
    vector<thread> threads;

    char val_not_found[VAL_SIZE];
};
