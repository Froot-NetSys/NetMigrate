# Run Migration  

## Start redis-server 
In destination:
```
ps aux | grep redis # check existing redis-server
sudo kill -9 xxxx (redis pid)
redis-server --protected-mode no --port 7380 --save "" --appendonly no&
```

## Restart Switchd and controller

## Start source priority pull 
```
cd NetMigrate/cpp/server/NetMigrate
bash run_src_pull.sh
```

## Start destination migration agent
Run destination script first.
```
cd NetMigrate/cpp/server/NetMigrate
bash run_dst.sh
```

## Start source migration push to migrate data

```
cd NetMigrate/cpp/server/NetMigrate
bash run_src_push.sh
```
## Limit Source Redis CPU
If limit source Redis CPU to mimic load-balancing scenario, e.g., 70% and 40% source redis CPU limit:

use this:
```
ps aux | grep redis
cpulimit -p 1234 -l 70
```

```
ps aux | grep redis
cpulimit -p 1234 -l 40
```


## Run YCSB Clients Immediately
```
cd NetMigrate/cpp/YCSB-client
./ycsb-kv_migration -run -db KV -P workloads/workloadc -P kv_migration/kv_migration.properties -p threadcount=8 -s > result/NetMigrate-c-16GB-100%.txt
```


