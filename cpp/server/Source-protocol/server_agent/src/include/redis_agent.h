#ifndef REDIS_AGENT_H
#define REDIS_AGENT_H

#include <string>
#include <iostream>
#include <sw/redis++/redis++.h>
#include <sw/redis++/utils.h>
using namespace sw::redis;
using namespace std;

class RedisAgent {
public:
    RedisAgent(std::string redis_ip, int redis_port, uint32_t scale) {
        std::string redis_address = "tcp://" + redis_ip + ":" + std::to_string(redis_port);
        std::cout << "Connect to redis_address = " << redis_address << std::endl;
        redis = std::make_shared<Redis>(redis_address);
        for (uint32_t i = 0; i < scale; i++) {
            pipe.emplace_back(std::make_shared<Pipeline>(redis->pipeline()));
        }
    }

    ~RedisAgent() { } 

    void AppendCmd(uint32_t scale_id, string op, string key, string val);

    std::shared_ptr<Redis> redis;
    std::vector<std::shared_ptr<Pipeline>> pipe;
private:
    uint32_t cmd_size;
};

#endif // REDIS_AGENT_H