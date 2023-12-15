#pragma once 

#include <string>
#include <mutex>
#include <random>
#include <atomic>

#include "../core/db.h"
#include "../../utils/socket.h"

#define MAX_THREAD_NUM 48
#define PIPELINE 100

namespace ycsbc {
    class KVDB: public DB {
    public: 
        KVDB() { }
        ~KVDB() { }
        void Init(uint32_t thread_id) override;
        void Cleanup() override;
        // All requests are pipelined version
        std::tuple<uint32_t, uint64_t> Read(const std::string &table, const std::vector<std::string> &keys,
                            const std::vector<std::string> *fields,
                            std::vector<std::vector<Field>> &results, uint32_t thread_id, int pipeline,
                            bool & migration_start, bool & migration_finish) override ;
        Status Scan(const std::string &table, const std::string &key,
                            int record_count, const std::vector<std::string> *fields,
                            std::vector<std::vector<Field> > &result, uint32_t thread_id) override;
        std::tuple<uint32_t, uint64_t> Update(const std::string &table, const std::vector<std::string> &keys,
                              std::vector<std::vector<Field>> &values, uint32_t thread_id, int pipeline,
                              bool & migration_start, bool & migration_finish) override;
        Status Insert(const std::string &table, const std::string &key,
                              std::vector<Field> &values, uint32_t thread_id, bool doload) override;
        Status Delete(const std::string &table, const std::string &key, uint32_t thread_id) override;

    private: 
        void UpdateSendRate();
        
        uint32_t thread_num;
        std::vector<std::shared_ptr<ClientSocket>> sockets;
        uint16_t src_agent_start_port;
        uint16_t dst_agent_start_port;
        std::vector<uint16_t> src_redis_ports;
        std::vector<uint16_t> dst_redis_ports;
        std::vector<uint16_t> redis_ports;
        unsigned int seq;
        uint32_t socket_id;
        std::default_random_engine generator;
        std::uniform_int_distribution<int> uniform_distr{0, MAX_THREAD_NUM-1};
        std::uniform_int_distribution<int> uniform_time{10, 30}; // TODO: tunable
        std::mutex switch_mutex;

        const float loss_rate_min = 0.01;
        const float loss_rate_max = 0.02;
        const uint64_t pkts_limit_us_min = 1; // minimum pkts per 1us
        const long send_rate_adjust_period = 100; // unit: microseconds
        uint64_t pkts_limit_us = pkts_limit_us_min;
        uint64_t pkts_sent_prev = 0;
        uint64_t pkts_recv_prev = 0;
        std::atomic_int pkts_sent = 0;
        std::atomic_int pkts_recv = 0;
        bool quick_start = true;
        const long FLAGS_tscale = 1;
    };

}
