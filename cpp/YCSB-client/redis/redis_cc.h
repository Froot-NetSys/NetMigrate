#ifndef YCSB_CPP_MASTER_REDIS_H
#define YCSB_CPP_MASTER_REDIS_H
#include <iostream>
#include <sw/redis++/redis++.h>
#include <sw/redis++/utils.h>
#include "../core/db.h"
#include "../core/properties.h"
#include "../core/db_factory.h"
using namespace sw::redis;
namespace ycsbc {
    class RedisDB: public DB {
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

         ~RedisDB() { }
         Redis *redis;
         Pipeline * pipe;
    };

}
#endif //YCSB_CPP_MASTER_REDIS_H
