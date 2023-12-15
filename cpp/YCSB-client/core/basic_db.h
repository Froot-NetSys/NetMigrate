//
//  basic_db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_BASIC_DB_H_
#define YCSB_C_BASIC_DB_H_

#include "db.h"
#include "properties.h"

#include <iostream>
#include <string>
#include <mutex>

namespace ycsbc {

class BasicDB : public DB {
 public:
  void Init(uint32_t thread_id);

  std::tuple<uint32_t, uint64_t> Read(const std::string &table, const std::vector<std::string> &keys,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &results, uint32_t thread_id, int pipeline,
              bool & migration_start, bool & migration_finish);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result, uint32_t thread_id);

  std::tuple<uint32_t, uint64_t> Update(const std::string &table, const std::vector<std::string> &keys, std::vector<std::vector<Field>> &values, uint32_t thread_id, int pipeline,
                  bool & migration_start, bool & migration_finish);

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values, uint32_t thread_id, bool doload);

  Status Delete(const std::string &table, const std::string &key, uint32_t thread_id);

 private:
  static std::mutex mutex_;
};

DB *NewBasicDB();

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

