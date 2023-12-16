# Acknowledgement

This repository comes from https://github.com/ls4154/YCSB-cpp. We use it for supporting Redis in NetMigrate project.
Install Redis-plus-plus before using Redis for YCSB.
Reference: https://gitlab.cs.washington.edu/sampa/redis/-/blob/master/YCSB/redis/src/main/java/com/yahoo/ycsb/db/RedisClient.java



# YCSB-cpp

Yahoo! Cloud Serving Benchmark([YCSB](https://github.com/brianfrankcooper/YCSB/wiki)) written in C++.
This is a fork of [YCSB-C](https://github.com/basicthinker/YCSB-C) with following changes.

 * Make Zipf distribution and data value more similar to the original YCSB
 * Status and latency report during benchmark
 * Supported Databases: LevelDB, RocksDB, LMDB

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

## Load and Run Examples

### Build, Load and Run with kv_migration (for NetMigrate) APIs
```
make BIND_KVMIGRATION=1 # build NetMigrate client
./ycsb-kv_migration -load -db KV -P workloads/workloada -P kv_migration/kv_migration.properties -p threadcount=4 -p recordcount=100000 -s
./ycsb-kv_migration -run -db KV -P workloads/workloada -P kv_migration/kv_migration.properties -s
```

### Build Fulva Client
```
make BIND_FULVA=1
```

### Build Rocksteady Client
```
make BIND_ROCKSTEADY=1
```

### Build Source-base Migration Client
```
make BIND_SOURCE=1
```
