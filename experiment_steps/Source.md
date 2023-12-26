# Run Migration  

## Start redis-server 
In destination server:
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

In another terminal in destination server:
```
cd NetMigrate/cpp/server/Source-protocol/server_agent/
bash start_dst_server_agent.sh
```



## Start source migration script
In source server:
```
cd NetMigrate/cpp/server/Source-protocol/
bash source_migr.sh
```
The parameters in ```source_migr.sh``` and ```destination_migr.sh``` mean:
```
Usage: Source_Migration server_type[destination, source] src_trans_ip dst_trans_ip src_redis_port dst_redis_port src_migration_agent_start_port dst_migration_agent_start_port src_kv_migr_start_port dst_kv_migr_start_port migr_thread_num migr_pkt_thread_num req_thread_num redis_scale_num client_start_port
```

## Run YCSB Clients Immetiately
In client server:

```
cd NetMigrate/cpp/YCSB-client
./ycsb-source -run -db KV -P workloads/workloadb  -P Source/run.properties -p threadcount=8 -s > ~/result/source-b-100%.txt
```

After migration finishes, you will get the raw data output ```~/result/source-b-100.txt``` in client server. You can draw throughput and latency figures from it using ```$NetMigrate/experiment_steps/figures/draw.py```. The trend in the figures will be similar as the below examples.


Throughput:

<p align="center">
  <img src="./figures/source-b-100.png" width="500">
</p>

Median latency:

<p align="center">
  <img src="./figures/source-5-100-50.png" width="500">
</p>

99%-tail latency:

<p align="center">
  <img src="./figures/source-5-100-99.png" width="500">
</p>


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

