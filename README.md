# In-Memory Key-Value Store Live Migration with NetMigrate

NetMigrate is a key-value store live migration protocol by leveraging programmalbe switches. NetMigrate migrates KVS shards between nodes with zero service interruption and minimal performance impact. During migration, the switch data plane monitors the migration process in a fine-grained manner and directs client queries to the right server in real time.

## Content
* cpp/
    * server/: 4 migration protocols' server agents.
    * YCSB-client/: YCSB client implementation for 4 migration protocols.
    * utils/: Common utils.
* tna_kv_migration/: Switch data-plane and control-plane code for NetMigrate. 
* experiment_steps/: Experiment step README.
* README.md: This file.

## Testbed experiments
* Hardware 
   * A Barefoot Tofino switch.
   * Servers with a 40G NIC (we used an Intel XL710 for 40GbE QSFP+) and multi-core CPU, connected by the Tofino switch.
* Software
   * Tofino SDK (version 9.4.0) on the switch.
   * Python2.7 on the switch.

## Installation
### Pre-Requirments for Redis

* Install Redis with User-Defined Migration Functions
    ```
    git clone https://github.com/zzylol/redis.git
    cd redis
    git checkout -b KVMigration-7.0 origin/KVMigration-7.0
    make && sudo make install 
    ```
* Install [gRPC and Protocol Buffers](https://grpc.io/docs/languages/cpp/quickstart/).
* Install [redis-plus-plus and hiredis](https://github.com/sewenew/redis-plus-plus?tab=readme-ov-file#installation)

### Compile Migration Agents 
Build Fulva baseline:
```
cd $KV_Migration/cpp/server/Fulva/
mkdir build && cd build
cmake -DCMAKE_PREFIX_PATH=$GRPC_INSTALL_DIR ..
make 
```

Build Rocksteady baseline:
```
cd $KV_Migration/cpp/server/Rocksteady/
mkdir build && cd build
cmake -DCMAKE_PREFIX_PATH=$GRPC_INSTALL_DIR ..
make 
```

Build source-migration baseline:
```
cd $KV_Migration/cpp/server/Source-protocol/
mkdir build && cd build
cmake -DCMAKE_PREFIX_PATH=$GRPC_INSTALL_DIR ..
make 
```

Build NetMigrate: 
```
cd $KV_Migration/cpp/server/NetMigrate/
mkdir build && cd build
cmake -DCMAKE_PREFIX_PATH=$GRPC_INSTALL_DIR ..
make 
```

### Compile YCSB Clients
[YCSB-client REAMDE](cpp/YCSB-client/README.md)

### Compile and Run Switch Code
[Tofino switch P4 code and controller README](tna_kv_migration/README.md)

(**Note for FAST'24 artifact evaluation process**: We can provide testbed with Tofino SDK installed if needed.)

## Run Migration Experiments  

### 1. Load YCSB Data
1. Start Source Request Server Agents
Note: 
* change ```server_agent_start_port``` and ```thread_num``` in bash files to the same as YCSB client ```agent_start_port``` and ```thread_num``` properties.

```
cd ../ServerAgents/server-agent
make 
bash start_src_server_agent.sh # do this at source server
```

2. run YCSB client load
Note: we use Rocksteady's client to load data to source redis-server for all migration protocols.
```
cd ../../YCSB-client 
make BIND_ROCKSTEADY=1
./ycsb-rocksteady -load -db KV -P workloads/workloada -P Rocksteady/load.properties -p threadcount=4 -p recordcount=10000000 -s # load data first
```

### 2. Run Migration for Four Protocols
* [Rocksteady](experiment_steps/Rocksteady.md)
* [Fulva](experiment_steps/Fulva.md)
* [Source-migration](experiment_steps/Source.md)
* [NetMigrate](experiment_steps/NetMigrate.md)

## License
The code is released under [GNU Affero General Public License v3.0](LICENSE).
