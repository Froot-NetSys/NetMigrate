# Set up

## Install modified Redis
```
git clone https://github.com/yunyunh123/redis-KVmigration.git
git checkout -b zeying/KVMigration-7.0 origin/zeying/KVMigration-7.0
make && sudo make install 
```

## Compile migration program 
```
cd $KV_Migration/cpp/baselines/Destination-protocol/
mkdir build && cd build
cmake ..
make 
```
# Run 

## Start redis-server 
```
ps aux | grep redis # check existing redis-server
killall redis-server
redis-server --protected-mode no --port 6379 
redis-server --protected-mode no --port 6380 
```

## Load test data and verify in redis-cli
```
redis-cli -p 6379 flushall 
redis-cli -p 6380 flushall 
```
```
redis-cli -p 6379
mset 1 11 2 12 3 13 4 14 5 15 6 16 7 17 8 18 9 19 10 20 11 21 12 22 13 23 14 24 15 25 16 26 17 27 18 28 19 29 20 30 21 31 22 32 23 33 24 34 25 35
```

```
redis-cli -p 6380
scan 0 # expect none
```

## Start Request Server Agents
Note: 
* change ```server_agent_start_port``` and ```thread_num``` in bash files to the same as YCSB client ```agent_start_port``` and ```thread_num``` properties.
```
cd server-agent
bash start_dst_server_agent.sh # do this at destination server
bash start_src_server_agent.sh # do this at source server
```

## Start YCSB Clients
```
cd $YCSB-client 
make BIND_KVMIGRATION=1
./ycsb-kv_migration -load -db KV -P workloads/workloada -P kv_migration/kv_migration.properties -p threadcount=4 -p recordcount=10000000 -s # load data first
./ycsb-kv_migration -run -db KV -P workloads/workloada -P kv_migration/kv_migration.properties -s # run workload, modify workload properties to run a longer time for test
```

## Run Migration Agents for migration metrics test
Note: 
* Run destination script first.
* Make sure request server agent ports, dst and src migration agent ports don't overlap.

```
cd migration-agent 
./run_dst.sh # do this at destination server 
./run_src.sh # do this at source server 
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

