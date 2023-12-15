//
//  db_wrapper.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_DB_WRAPPER_H_
#define YCSB_C_DB_WRAPPER_H_

#include <string>
#include <vector>

#include "db.h"
#include "measurements.h"
#include "timer.h"
#include "utils.h"

namespace ycsbc {

class DBWrapper : public DB {
 public:
  DBWrapper(DB *db, Measurements *measurements) : db_(db), measurements_(measurements) {}
  ~DBWrapper() {
    delete db_;
  }
  void Init(uint32_t thread_id) {
    db_->Init(thread_id);
  }
  void Cleanup() {
    db_->Cleanup();
  }
  std::tuple<uint32_t, uint64_t> Read(const std::string &table, const std::vector<std::string> &keys,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &results, uint32_t thread_id, int pipeline,
              bool & migration_start, bool & migration_finish) {
    timer_.Start();
    uint32_t s;
    uint64_t extra_bandwidth;
    std::tie(s, extra_bandwidth) = db_->Read(table, keys, fields, results, thread_id, pipeline, migration_start, migration_finish);
    uint64_t elapsed = timer_.End();
    measurements_->Report(READ, elapsed, thread_id, s, extra_bandwidth, pipeline * 68);
    return std::make_tuple(s, extra_bandwidth);
  }
  Status Scan(const std::string &table, const std::string &key, int record_count,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result, uint32_t thread_id) {
    timer_.Start();
    Status s = db_->Scan(table, key, record_count, fields, result, thread_id);
    uint64_t elapsed = timer_.End();
    measurements_->Report(SCAN, elapsed, thread_id, 1, 0, 0);
    return s;
  }
  std::tuple<uint32_t, uint64_t> Update(const std::string &table, const std::vector<std::string> &keys, std::vector<std::vector<Field>> &values, 
                  uint32_t thread_id, int pipeline,
                  bool & migration_start, bool & migration_finish) {
    timer_.Start();
    uint32_t s;
    uint64_t extra_bandwidth;
    std::tie(s, extra_bandwidth) = db_->Update(table, keys, values, thread_id, pipeline, migration_start, migration_finish);
    uint64_t elapsed = timer_.End();
    measurements_->Report(UPDATE, elapsed, thread_id, s, extra_bandwidth, pipeline * 68);
    return std::make_tuple(s, extra_bandwidth);
  }
  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values, uint32_t thread_id, bool doload) {
    timer_.Start();
    Status s = db_->Insert(table, key, values, thread_id, doload);
    uint64_t elapsed = timer_.End();
    measurements_->Report(INSERT, elapsed, thread_id, 1, 0, 0);
    return s;
  }
  Status Delete(const std::string &table, const std::string &key, uint32_t thread_id) {
    timer_.Start();
    Status s = db_->Delete(table, key, thread_id);
    uint64_t elapsed = timer_.End();
    measurements_->Report(DELETE, elapsed, thread_id, 1, 0, 0);
    return s;
  }
 private:
  DB *db_;
  Measurements *measurements_;
  utils::Timer<uint64_t, std::nano> timer_;
};

} // ycsbc

#endif // YCSB_C_DB_WRAPPER_H_
