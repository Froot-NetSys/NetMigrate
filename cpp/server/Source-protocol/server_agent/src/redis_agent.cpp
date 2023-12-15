#include "redis_agent.h"

void RedisAgent::AppendCmd(uint32_t scale_id, string op, string key, string val) {
    
    if (op == "GET") {
        pipe.at(scale_id)->get(key);
    }
    else {
        pipe.at(scale_id)->set(key, val);
    }
    cmd_size++;
}
