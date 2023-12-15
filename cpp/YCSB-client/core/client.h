//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "utils.h"
#include "countdown_latch.h"

namespace ycsbc {

inline long long ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const long long num_ops, bool is_loading,
                        bool init_db, bool cleanup_db, CountDownLatch *latch, uint32_t thread_id, bool & migration_start, bool & migration_finish) {
  if (init_db) {
    db->Init(thread_id);
  }

  long long oks = 0;

  long long PIPELINE = 200; // must be the times of server side pipeline configure
  uint32_t THREAD_NUM_THRE = 1;
  if (is_loading) {
    PIPELINE = 1;
  }

  for (long long i = 0; i < num_ops / PIPELINE; ++i) {
    /*
    bool bypass = false;
    if (migration_start == true && migration_finish == false) { // global bool variables to all client threads
      if (thread_id > THREAD_NUM_THRE)  // control client thread num during migration
        bypass = true;
    }

    while (bypass) {
      if (migration_start == true && migration_finish == false) { // global bool variables to all client threads
        if (thread_id > THREAD_NUM_THRE)  // control client thread num during migration
          bypass = true;
        else 
          bypass = false;
      }
      else 
        bypass = false;
    }
    */

    if (is_loading) {
      oks += wl->DoInsert(*db, thread_id, PIPELINE);
    } else {
        uint32_t success = 0;
        uint64_t extra_bandwidth = 0;
        std::tie(success, extra_bandwidth) = wl->DoTransaction(*db, thread_id, PIPELINE, migration_start, migration_finish);
        oks += success;
    }
  }

  if (cleanup_db) {
    db->Cleanup();
  }

  latch->CountDown();
  return oks;
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
