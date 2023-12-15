# Run Migration  

## Start destination redis-server 
In destination:
```
ps aux | grep redis # check existing redis-server
sudo kill -9 xxxx (redis pid)
redis-server --protected-mode no --port 7380 --save "" --appendonly no&
```

## Start source redis-server in source machine
```
redis-server --protected-mode no --port 6380 --save "" --appendonly no&
```

## Start source priority pull server
```
cd KV_Migration/cpp/server/Fulva
bash run_src_pull.sh
```
Note: We can only run source pull to test PriorityPull implementation/performance.

## Limit Source redis CPU
For parameter 70% and 40% source redis CPU limit:
use this:
```
ps aux | grep redis
cpulimit -p $(redis-server-pid) -l 70
```

```
ps aux | grep redis
cpulimit -p $(redis-server-pid) -l 40
```


## Start destination migration script
Run destination script first.
```
cd KV_Migration/cpp/server/Fulva
bash run_dst.sh
```


## Run YCSB Clients
```
./ycsb-fulva -run -db KV -P workloads/workloadc -P Fulva/kv_migration.properties -p threadcount=8 -s > result/fulva-c-16GB-100%.txt
```

## Start source migration push server

After running client of about 200 seconds, in source server:
```
cd KV_Migration/cpp/server/Fulva
bash run_src_push.sh
```
