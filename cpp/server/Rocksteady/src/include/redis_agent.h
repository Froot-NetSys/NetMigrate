#ifndef REDIS_AGENT_H
#define REDIS_AGENT_H

#include <string>
#include <iostream>
#include <unordered_map>
#include <sw/redis++/redis++.h>
#include <sw/redis++/utils.h>
using namespace sw::redis;

class RedisAgent {
public:
    RedisAgent(std::string redis_ip, int redis_port, uint32_t scale_num) {
        std::string redis_address = "tcp://" + redis_ip + ":" + std::to_string(redis_port);
        std::cout << "Connect to redis_address = " << redis_address << std::endl;
        redis = std::make_shared<Redis>(redis_address);
        for (uint32_t i = 0; i < scale_num; i++) {
            pipe.emplace_back(std::make_shared<Pipeline>(redis->pipeline()));
        }
    }

    ~RedisAgent() { } 
    void AppendCmd(uint32_t scale_id, std::string op, std::string key, std::string val) { // used only by req_agent
        if (op == "GET") {
            pipe.at(scale_id)->get(key);
        }
        else {
            pipe.at(scale_id)->set(key, val);
        }
    }
        
    std::shared_ptr<Redis> redis;
    std::vector<std::shared_ptr<Pipeline>> pipe; // redis++ pipeline is not thread-safe 
};


// All redis instances in a shard are replicas to each other
// All replicas can serve client requests, including read and write
// But we only migrate the first replica in this shard without loss of generability

// In destination, other replicas in the shard are preloaded and not from migration
// One migration agent program only handles one shard now.

class ShardAgent {
public:
    ShardAgent(std::string redis_ip, std::vector<uint16_t> & port_list, uint32_t scale_num) {
        for (int i = 0; i < port_list.size(); i++) {
            shard.emplace(port_list[i], std::make_shared<RedisAgent>(redis_ip, port_list[i], scale_num));
        }
    }

    ~ShardAgent() { }
    std::unordered_map<uint16_t, std::shared_ptr<RedisAgent>> shard;
};

#endif // REDIS_AGENT_H