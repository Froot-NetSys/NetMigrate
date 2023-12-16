# Acknowledgement

This repository comes from https://github.com/ls4154/YCSB-cpp. We use it for supporting Redis in NetMigrate project.
Install Redis-plus-plus before using Redis for YCSB.
Reference: https://gitlab.cs.washington.edu/sampa/redis/-/blob/master/YCSB/redis/src/main/java/com/yahoo/ycsb/db/RedisClient.java



# YCSB-cpp

Yahoo! Cloud Serving Benchmark([YCSB](https://github.com/brianfrankcooper/YCSB/wiki)) written in C++.
This is a fork of [YCSB-C](https://github.com/basicthinker/YCSB-C) with following changes.

 * Make Zipf distribution and data value more similar to the original YCSB
 * Status and latency report during benchmark
 * Supported Databases: LevelDB, RocksDB, LMDB, Redis
 * Supported Migration Progtocols (for Redis): NetMigrate, Rocksteady, Fulva, Sourc-base Migration

## Building

Simply use `make` to build.

The databases to bind must be specified. You may also need to add additional link flags (e.g., `-lsnappy`).

To bind LevelDB:
```
make BIND_LEVELDB=1
```

To build with additional libraries and include directories:
```
make BIND_LEVELDB=1 EXTRA_CXXFLAGS=-I/example/leveldb/include \
                    EXTRA_LDFLAGS="-L/example/leveldb/build -lsnappy"
```

Or modify config section in `Makefile`.

RocksDB build example:
```
EXTRA_CXXFLAGS ?= -I/example/rocksdb/include
EXTRA_LDFLAGS ?= -L/example/rocksdb -ldl -lz -lsnappy -lzstd -lbz2 -llz4

BIND_ROCKSDB ?= 1
```

## Examples for NetMigrate FAST'24 Experiments (4 baselines)

### Build and Run with kv_migration (for NetMigrate) APIs
```
make BIND_KVMIGRATION=1 # build NetMigrate client
./ycsb-kv_migration -run -db KV -P workloads/workloadc -P kv_migration/kv_migration.properties -p threadcount=8 -s > result/NetMigrate-c-16GB-100%.txt
```

### Build Fulva Client
```
make BIND_FULVA=1
./ycsb-fulva -run -db KV -P workloads/workloadc -P Fulva/kv_migration.properties -p threadcount=8 -s > result/fulva-c-16GB-100%.txt
```

### Build Rocksteady Client
```
make BIND_ROCKSTEADY=1
./ycsb-rocksteady -run -db KV -P workloads/workloadc -P Rocksteady/run.properties -p threadcount=8 -s > result/rocksteady-c-16GB-100%.txt
```

### Build Source-base Migration Client
```
make BIND_SOURCE=1
./ycsb-source -run -db KV -P workloads/workloadc  -P Source/run.properties -p threadcount=8 -s > result/source-c-16GB-100%.txt
```
