//
//  measurements.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "measurements.h"

#include <atomic>
#include <limits>
#include <numeric>
#include <sstream>
#include <algorithm>
#include <iostream>
#include <queue>

namespace ycsbc {

Measurements::Measurements() : count_{}, latency_sum_{}, latency_max_{} {
  std::fill(std::begin(latency_min_), std::end(latency_min_), std::numeric_limits<uint64_t>::max());
}

void Measurements::Report(Operation op, double latency, uint32_t thread_id, int pipeline_success, uint64_t extra_bandwidth, uint64_t original_bandwidth) {
  count_[op].fetch_add(pipeline_success, std::memory_order_relaxed);
  latencies.at(thread_id).push_back(latency);
  extra_bandwidth_.fetch_add(extra_bandwidth, std::memory_order_relaxed);
  original_total_bandwidth_.fetch_add(original_bandwidth, std::memory_order_relaxed);
  

  /*
  latency_sum_[op].fetch_add(latency, std::memory_order_relaxed);
  uint64_t prev_min = latency_min_[op].load(std::memory_order_relaxed);
  while (prev_min > latency
         && !latency_min_[op].compare_exchange_weak(prev_min, latency, std::memory_order_relaxed));
  uint64_t prev_max = latency_max_[op].load(std::memory_order_relaxed);
  while (prev_max < latency
         && !latency_max_[op].compare_exchange_weak(prev_max, latency, std::memory_order_relaxed));
  */
}

std::string Measurements::GetStatusMsg() {
  std::ostringstream msg_stream;
  msg_stream.precision(6);

  uint64_t size = 0;
  std::vector<double> latency_tmp;
  for (uint32_t i = 0; i < this->thread_num; i++) {
    latency_tmp.insert(latency_tmp.end(), latencies[i].begin(), latencies[i].end());
  }
  size = latency_tmp.size();
  std::sort(latency_tmp.begin(), latency_tmp.end());

  double latency_median = 0;
  double latency_99_9 = 0;
  double latency_quantile[100];
  if (size != 0) {
    /*
    uint64_t pos = uint64_t(size * 0.5);
    latency_median = latency_tmp[pos - 1] + (latency_tmp[pos] - latency_tmp[pos - 1]) * ((double) size * 0.5 - pos);

    pos = uint64_t(size * 0.999);
    latency_99_9 = latency_tmp[pos - 1] + (latency_tmp[pos] - latency_tmp[pos - 1]) * ((double) size * 0.999 - pos);
    */
    for (int i = 1; i < 100; i++) {
      uint64_t pos = uint64_t(size * i * 1.0 / 100.0);
      latency_quantile[i-1] = latency_tmp[pos - 1] + (latency_tmp[pos] - latency_tmp[pos - 1]) * ((double) size * i * 1.0 / 100.0 - pos);
    }
    uint64_t pos = uint64_t(size * 0.999);
    latency_quantile[99] = latency_tmp[pos - 1] + (latency_tmp[pos] - latency_tmp[pos - 1]) * ((double) size * 0.999 - pos);
  }

  uint64_t total_cnt = 0;
  msg_stream << std::fixed << " operations; ";
  
  for (int i = 0; i < 99; i++) {
    msg_stream << "latency_" + std::to_string(i+1) + "=" << latency_quantile[i] / 1000.0 << "(us); ";
  }
  msg_stream << "latency_99.9=" << latency_quantile[99] / 1000.0 << "(us); extra_bandwidth_cumulated=" << extra_bandwidth_ << "(Bytes); original_total_bandwidth=" << original_total_bandwidth_ << "(Bytes);";
  

  for (int i = 0; i < MAXOPTYPE; i++) {
    Operation op = static_cast<Operation>(i);
    uint64_t cnt = GetCount(op);
    if (cnt == 0)
      continue;
    msg_stream << " [" << kOperationString[op] << ":"
               << " op_count=" << cnt
              /*
               << " Max=" << latency_max_[op].load(std::memory_order_relaxed) / 1000.0
               << " Min=" << latency_min_[op].load(std::memory_order_relaxed) / 1000.0
               << " Avg="
               << ((cnt > 0)
                   ? static_cast<double>(latency_sum_[op].load(std::memory_order_relaxed)) / cnt
                   : 0) / 1000.0
              */
               << "]";
    total_cnt += cnt;
  }
  Reset();
  return std::to_string(total_cnt) + msg_stream.str();
}

void Measurements::Reset() {
  std::fill(std::begin(count_), std::end(count_), 0);
/*
  std::fill(std::begin(latency_sum_), std::end(latency_sum_), 0);
  std::fill(std::begin(latency_min_), std::end(latency_min_), std::numeric_limits<uint64_t>::max());
  std::fill(std::begin(latency_max_), std::end(latency_max_), 0);
*/
  for (uint32_t i = 0; i < this->thread_num; i++)  {
    latencies[i].clear(); // latency quantile for all types of operations each client thread
  }
  latency_median = 0;
  latency_99_9 = 0;
}

} // ycsbc
