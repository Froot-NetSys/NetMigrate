# Run Migration  

## Limit Source redis CPU
For parameter 70% and 40% source redis CPU limit:
use this:
```
ps aux | grep redis
cpulimit -p 1234 -l 70
```

```
ps aux | grep redis
cpulimit -p 1234 -l 40
```

## Start redis-server 
In destination:
```
ps aux | grep redis # check existing redis-server
sudo kill -9 xxxx (redis pid)
redis-server --protected-mode no --port 7380 --save "" --appendonly no&
```

## Restart Switchd and controller

## Start source priority pull server
```
cd KV_Migration/cpp/server/NetMigrate
bash run_src_pull.sh
```
Note: We can only run source pull to test PriorityPull implementation/performance.


## Start destination migration script
Run destination script first.
```
cd KV_Migration/cpp/server/NetMigrate
bash run_dst.sh
```

## Start source migration push server

```
cd KV_Migration/cpp/server/NetMigrate
bash run_src_push.sh
```


## Run YCSB Clients Immediately
```
./ycsb-kv_migration -run -db KV -P workloads/workloadc -P kv_migration/kv_migration.properties -p threadcount=8 -s > result/NetMigrate-c-16GB-100%.txt
```


