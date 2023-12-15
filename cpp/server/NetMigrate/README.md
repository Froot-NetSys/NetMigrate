# UDP Ports in Switch
```
Clients:            [48640, 48703]
Server_agents:      [48704, 48767]
Migration_agents:   [48768, 48831]
Reserved:           [48832, 48895]
```

# Set up Migration Requirments

## Install modified Redis
```
git clone https://github.com/yunyunh123/redis-KVmigration.git
git checkout -b zeying/KVMigration-7.0 origin/zeying/KVMigration-7.0
make && sudo make install 
```

## Install gRPC and Protocol Buffers
Install [gRPC and Protocol Buffers](https://grpc.io/docs/languages/cpp/quickstart/).

## Install redis-plus-plus and hiredis
```

```

## Compile migration program 
```
cd $KV_Migration/cpp/server/Destination-protocol/
mkdir build && cd build
cmake -DCMAKE_PREFIX_PATH=$GRPC_INSTALL_DIR ..
make 
```


# Run Migration  

## Start redis-server with turnning off persistence mechanism
```
ps aux | grep redis # check existing redis-server
killall redis-server
```
```
redis-server --protected-mode no --port 6380 --save "" --appendonly no&
redis-server --protected-mode no --port 6381 --save "" --appendonly no&
redis-server --protected-mode no --port 6382 --save "" --appendonly no&
redis-server --protected-mode no --port 6383 --save "" --appendonly no&
redis-server --protected-mode no --port 6384 --save "" --appendonly no&
```
```
redis-server --protected-mode no --port 7380 --save "" --appendonly no&
redis-server --protected-mode no --port 7381 --save "" --appendonly no&
redis-server --protected-mode no --port 7382 --save "" --appendonly no&
redis-server --protected-mode no --port 7383 --save "" --appendonly no&
redis-server --protected-mode no --port 7384 --save "" --appendonly no&
```

## Load test data and verify in redis-cli
```
redis-cli -p 6380 flushall 
redis-cli -p 6381 flushall
redis-cli -p 6382 flushall 
redis-cli -p 6383 flushall
redis-cli -p 6384 flushall 
```
```
redis-cli -p 7380 flushall 
redis-cli -p 7381 flushall
redis-cli -p 7382 flushall 
redis-cli -p 7383 flushall
redis-cli -p 7384 flushall  
```

Note: In real test, we skip following mset command, which is used for correctness test.
```
redis-cli -p 6379
mset 1 11 2 12 3 13 4 14 5 15 6 16 7 17 8 18 9 19 10 20 11 21 12 22 13 23 14 24 15 25 16 26 17 27 18 28 19 29 20 30 21 31 22 32 23 33 24 34 25 35 
```

```
redis-cli -p 6380
scan 0 # expect none
```

## Load YCSB data
1. Start Source Request Server Agents
Note: 
* change ```server_agent_start_port``` and ```thread_num``` in bash files to the same as YCSB client ```agent_start_port``` and ```thread_num``` properties.

```
cd ../ServerAgents/server-agent
make 
bash start_src_server_agent_1.sh # do this at source server
```

2. run YCSB client load
Note: we only use Rocksteady's client to load for all migration protocols.
```
cd ../../YCSB-client 
make BIND_ROCKSTEADY=1
./ycsb-rocksteady -load -db KV -P workloads/workloada -P Rocksteady/load_1.properties -p threadcount=4 -p recordcount=10000000 -s # load data first
```

## Start source priority pull server

```
bash run_src_pull.sh
```
Note: We can only run source pull to test PriorityPull implementation/performance.


## Start destination migration script
Run destination script first.
```
bash run_dst.sh
```


## Run YCSB Clients
```
./ycsb-kv_migration -run -db KV -P workloads/workloadc -P kv_migration/kv_migration.properties -p threadcount=12 -s # run workload, modify workload properties to run a longer time for test
```

## Start source migration push server
```
bash run_src_push.sh
```

## Traffic Monitoring
```
sudo iftop -nN -i ens2f0np0
```


## Verify in redis-cli
```
redis-cli -p 6379
scan 0 # currently not delete so still list all key-values
```

```
redis-cli -p 6380
scan 0 # same as source instance before
```

