# Run Migration  

## Start redis-server 
In destination:
```
ps aux | grep redis # check existing redis-server
sudo kill -9 xxxx (redis pid)
redis-server --protected-mode no --port 7380 --save "" --appendonly no&
```


## Start destination priority pull 
In destination server:

```
cd KV_Migration/cpp/server/Source-protocol/
bash destination_migr.sh
```

In another terminal:
```
cd NetMigrate/cpp/server/Source-protocol/server_agent/
bash start_dst_server_agent_4.sh
```



## Start source migration script
In source server:
```
cd NetMigrate/cpp/server/Source-protocol/
bash source_migr.sh
```


## Run YCSB Clients Immetiately

```
./ycsb-source -run -db KV -P workloads/workloadc  -P Source/run.properties -p threadcount=8 -s > result/source-c-16GB-100%.txt
```


## Limit Source Redis CPU
If limiting source Redis CPU to mimic load-balancing scenario, e.g., 70% and 40% source redis CPU limit:

use this:
```
ps aux | grep redis
cpulimit -p 1234 -l 70
```

```
ps aux | grep redis
cpulimit -p 1234 -l 40
```

