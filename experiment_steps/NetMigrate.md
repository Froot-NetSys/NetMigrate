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
In another terminal in the source server:
```
cd NetMigrate/cpp/server/NetMigrate
bash run_src_push.sh
```

## Run YCSB Clients Immediately
(because we start migration after 200 sec in NetMigrate server agent code.)
```
cd NetMigrate/cpp/YCSB-client
./ycsb-kv_migration -run -db KV -P workloads/workloadb -P kv_migration/kv_migration.properties -p threadcount=8 -s > ~/result/NetMigrate-b-100%.txt
```

After migration finishes, you will get NetMigrate throughput figure(Figure 4(d) in the paper) and latency figures (Figure 5(d) and 6(d) in the paper).

Throughput:

<p align="center">
  <img src="./figures/netmigrate-b-100.png" width="500">
</p>

Median latency:

<p align="center">
  <img src="./figures/netmigrate-5-100-50.png" width="500">
</p>

99%-tail latency:

<p align="center">
  <img src="./figures/netmigrate-5-100-99.png" width="500">
</p>

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




