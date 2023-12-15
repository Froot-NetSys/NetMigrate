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


## Start source priority pull server

```
cd KV_Migration/cpp/server/Rocksteady
bash run_src_pull.sh
```
Note: We can only run source pull to test PriorityPull implementation/performance.


## Start destination migration script
Run destination script first.
```
cd KV_Migration/cpp/server/Rocksteady
bash run_dst.sh
```


## Run YCSB Clients
```
./ycsb-rocksteady -run -db KV -P workloads/workloadc -P Rocksteady/run.properties -p threadcount=8 -s > result/rocksteady-c-16GB-100%.txt
```

## Start source migration push server

After running client of at least 200 seconds, in source server:
```
cd KV_Migration/cpp/server/Rocksteady
bash run_src_push.sh
```



