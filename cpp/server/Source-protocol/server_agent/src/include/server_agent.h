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
#define PIPELINE 100

struct cmd_q {
    string op;
    string key;
    string val;
    uint32_t socket_id;
    uint64_t seq;
    uint8_t ver;
};

class ServerAgent {
public:
    ServerAgent(bool server_type, string &redis_ip, vector<std::string> & redis_ports_, string &trans_ip, uint16_t agent_start_port, uint32_t thread_num, uint32_t scale_): 
        server_type(server_type), agent_start_port(agent_start_port), thread_num(thread_num), redis_handler_scale(scale_) {
        
        for (auto &redis_port: redis_ports_) {
            redis_ports.push_back(stoi(redis_port));
            op_queue.emplace(redis_ports.back(), moodycamel::ConcurrentQueue<cmd_q>());
            redis_agents.emplace(make_pair(stoi(redis_port), make_shared<RedisAgent>(redis_ip, stoi(redis_port), scale_)));
        }

        for (uint32_t i = 0; i < thread_num; i++) {
            cout << "Using agent_port: " << agent_start_port + i << endl;
            server_sockets.emplace_back(make_shared<ServerSocket>(agent_start_port + i));
            send_src_exit_signal.emplace_back(false);
        }

        for (int i = 0; i < VAL_SIZE; i++)
            val_not_found[i] = 0xFF;

    }
    ~ServerAgent() { }
   
    void RequestHandler();
    
private:
    void RequestHandlerThread(uint32_t thread_id);
    void RedisHandlerThread(uint32_t thread_id);

    bool server_type;

    string redis_ip;
    vector<uint16_t> redis_ports;
    map<int, shared_ptr<RedisAgent>> redis_agents;
    
    
    string trans_ip;
    uint16_t agent_start_port;
    vector<shared_ptr<ServerSocket>> server_sockets;

    uint32_t thread_num;
    vector<thread> threads;

    char val_not_found[VAL_SIZE];

    map<int, moodycamel::ConcurrentQueue<cmd_q>> op_queue;
    uint32_t redis_handler_scale;
    vector<bool> send_src_exit_signal;
    atomic_bool exit_flag = false;
};
