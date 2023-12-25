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
In source server:
```
cd NetMigrate/cpp/server/NetMigrate
bash run_src_pull.sh
```

The parameters in ```run_src_pull.sh```, ```run_dst.sh```, ```run_src_push.sh``` mean 
```
Usage: NetMigrate server_type[destination, source_pull, source_push] src_redis_ip [src_redis_port,...] src_migration_agent_start_port dst_redis_ip [dst_redis_port,...] dst_migration_agent_start_port src_trans_ip dst_trans_ip migr_thread_num migr_pkt_thread_num req_thread_num redis_cli_scale_num
```

## Start destination migration agent
In destination server:
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
(because we start migration after 300 sec in NetMigrate server agent code.)
In client server:
```
cd NetMigrate/cpp/YCSB-client
./ycsb-kv_migration -run -db KV -P workloads/workloadb -P kv_migration/kv_migration.properties -p threadcount=8 -s > ~/result/netmigrate-b-100%.txt
```

After migration finishes, you will get NetMigrate throughput figure and latency figures (below as an example).
You can draw from the raw data output ```netmigrate-b-100.txt``` by the client using ```./figures/draw.py```.

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




