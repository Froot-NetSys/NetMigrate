// ./ycsb-redis -run -db redis -P workloads/workloadc -P redis/redis.properties -p threadcount=3 -s

#include "redis_cc.h"
#include <iostream>
namespace ycsbc {
    void RedisDB::Init(uint32_t thread_id) {

        const utils::Properties& props =  *props_;
        std::string redis_address = props.GetProperty("redis.address");
        std::cout << "Redis Address: " << redis_address << std::endl;
        redis = new Redis(redis_address);
        pipe = new Pipeline(redis->pipeline());
    }

    void RedisDB::Cleanup() {
        delete redis;
    }

    std::tuple<uint32_t, uint64_t> RedisDB::Read(const std::string &table, const std::vector<std::string> &keys,
                            const std::vector<std::string> *fields,
                            std::vector<std::vector<Field>> &results, uint32_t thread_id, int pipeline,
                            bool & migration_start, bool & migration_finish){
        for (int i = 0; i < pipeline; i++) {
            std::string redis_key = table + "_" + keys[i];
            // auto res = redis->get(redis_key);
            pipe->get(redis_key);
        }

       auto pipe_replies = pipe->exec();
        for (int i = 0; i < pipeline; i++) {
            auto res = pipe_replies.get<OptionalString>(i);
            if (res) {
                Field f;
                f.name = f.value = *res;
                std::vector<Field> result;
                result.push_back(f);
                results.push_back(result);
            }
        }
        
        return std::make_tuple(pipeline, 0);
    }
    DB::Status RedisDB::Scan(const std::string &table, const std::string &key,
                int record_count, const std::vector<std::string> *fields,
                std::vector<std::vector<Field> > &result, uint32_t thread_id) {
        return DB::Status::kNotImplemented;
    }
    std::tuple<uint32_t, uint64_t> RedisDB::Update(const std::string &table, const std::vector<std::string> &keys,
                  std::vector<std::vector<Field>> &values, uint32_t thread_id, int pipeline,
                  bool & migration_start, bool & migration_finish){
        for (int i = 0; i < pipeline; i++) {
            std::string redis_key = table + "_" + keys[i];
            std::string redis_val = "";
            auto values_ = values[i];
            for (auto &val: values_)
                redis_val += val.name + val.value;
            // redis->set(redis_key, std::to_string(std::hash<std::string>{}(redis_val)));
            pipe->set(redis_key, std::to_string(std::hash<std::string>{}(redis_val)));
        }
        auto pipe_replies = pipe->exec();

        return std::make_tuple(pipeline, 0);
    }

    DB::Status RedisDB::Insert(const std::string &table, const std::string &key,
                  std::vector<Field> &values, uint32_t thread_id, bool doload){
        std::string redis_key = table + "_" + key;
        std::string redis_val = "";
        std::unordered_map<std::string, std::string> redis_kvs;

        for (auto &val: values)
            redis_val += val.name + val.value;

        redis->set(redis_key, std::to_string(std::hash<std::string>{}(redis_val)));

        return DB::Status::kOK;
    }
    DB::Status RedisDB::Delete(const std::string &table, const std::string &key, uint32_t thread_id){
        std::string redis_key = table + "_" + key;
        return redis->del(redis_key) == 1 ? DB::Status::kOK : DB::Status::kNotFound;
    }

    DB *NewRedisDB() {
        return new RedisDB;
    }

    const bool registered = DBFactory::RegisterDB("redis", NewRedisDB);

}