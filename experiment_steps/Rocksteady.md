# Run Migration  

## Start Destination Redis-server 
In destination:
```
ps aux | grep redis # check existing redis-server
sudo kill -9 xxxx (redis pid)
redis-server --protected-mode no --port 7380 --save "" --appendonly no&
```


## Start source priority pull 
In source server:
```
cd NetMigrate/cpp/server/Rocksteady
bash run_src_pull.sh
```

## Start destination migration agent
In destination server:
```
cd NetMigrate/cpp/server/Rocksteady
bash run_dst.sh
```


## Run YCSB Clients
In client server:
```
cd NetMigrate/cpp/YCSB-client
./ycsb-rocksteady -run -db KV -P workloads/workloadb -P Rocksteady/run.properties -p threadcount=8 -s > ~/result/rocksteady-b-100%.txt
```

## Start source migration push to migrate data

After running client for a while (e.g., ~200 seconds), in another terminal in source server:
```
cd NetMigrate/cpp/server/Rocksteady
bash run_src_push.sh
```

You will get Rocksteady throughput figure(Figure 4(a) in the paper) and latency figures (Figure 5(a) and 6(a) in the paper).

Throughput:

<p align="center">
  <img src="./figures/rocksteady-b-100.png" width="500">
</p>

Median latency:

<p align="center">
  <img src="./figures/rocksteady-5-100-50.png" width="500">
</p>

99%-tail latency:

<p align="center">
  <img src="./figures/rocksteady-5-100-99.png" width="500">
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



