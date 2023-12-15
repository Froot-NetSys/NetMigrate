#pragma once 

#include <string>
#include <mutex>
#include <atomic>
#include <random>
#include <unordered_set>

#include "../core/db.h"
#include "../../utils/socket.h"
#include "../../utils/constants.h"
#include "../../utils/pkt_headers.h"
#include "crc64.h"

#define MAX_THREAD_NUM 48
#define SPH_ERASE_TIMEOUT 1

#define PIPELINE 200

namespace ycsbc {
    class KVDB: public DB {
    public: 
        KVDB() { }
        ~KVDB() { }
        void Init(uint32_t thread_id) override;
        void Cleanup() override;
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

        uint32_t dict_hash(char * key);
        void UpdateMCR(uint32_t * mcr);
        void UpdateSPH(struct sph_metadata * sph_md);
        bool LookUpMCR_SPH(char * key);
        void UpdateMCRBegin(uint32_t * mcr_begin);

        uint32_t thread_num;
        uint32_t this_thread_id;
        std::vector<std::shared_ptr<ClientSocket>> sockets;
        uint16_t src_agent_start_port;
        std::vector<uint16_t> src_redis_ports;
        uint16_t dst_agent_start_port;
        std::vector<uint16_t> dst_redis_ports;
        unsigned int seq;
        std::default_random_engine generator;
        std::uniform_int_distribution<int> uniform_distr{0, MAX_THREAD_NUM-1};
        std::uniform_int_distribution<int> uniform_time{10, 30}; // TODO: tunable

        std::atomic<bool> under_migration = false;
        std::atomic<bool> finish_migration = false;
        uint32_t MCR[MAX_THREAD_NUM] = {0}; // stores P_1 to P_n as migration progress
        uint32_t MCR_begin[MAX_THREAD_NUM] = {0}; // stores the begin value of a hash range 
        size_t mcr_size = 0;
        std::unordered_set<uint32_t> SPH; // stores keys from Sampling Pulled keys, which will be removed once MCR includes the hash value
        std::chrono::time_point<std::chrono::system_clock> m_StartTime;
        uint32_t dict_mask = 0;

        
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
