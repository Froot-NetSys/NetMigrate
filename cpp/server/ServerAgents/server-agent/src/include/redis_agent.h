#ifndef REDIS_AGENT_H
#define REDIS_AGENT_H

#include <string>
#include <iostream>
#include <sw/redis++/redis++.h>
#include <sw/redis++/utils.h>
using namespace sw::redis;

class RedisAgent {
public:
    RedisAgent(std::string redis_ip, int redis_port) {
        std::string redis_address = "tcp://" + redis_ip + ":" + std::to_string(redis_port);
        std::cout << "Connect to redis_address = " << redis_address << std::endl;
        redis = std::make_shared<Redis>(redis_address);
    }

    ~RedisAgent() { } 
    
    std::shared_ptr<Redis> redis;
};

#endif // REDIS_AGENT_H