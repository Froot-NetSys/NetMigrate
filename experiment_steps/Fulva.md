# Run Migration  

## Start Source Redis-server 
In source (netx7):
```
ps aux | grep redis # check existing redis-server
sudo kill -9 xxxx (redis pid)
cd ~; redis-server --protected-mode no --port 6380 --save "" --appendonly no &
```

## Start Destination redis-server 
In destination (netx5):
```
ps aux | grep redis # check existing redis-server
sudo kill -9 xxxx (redis pid)
redis-server --protected-mode no --port 7380 --save "" --appendonly no&
```

## Start source priority pull server
In source server (netx7):
```
cd $NetMigrate/cpp/server/Fulva
bash run_src_pull.sh
```

## Start destination migration agent
In destination server (netx5): 
```
cd $NetMigrate/cpp/server/Fulva
bash run_dst.sh
```


## Run YCSB Clients
In client server (netx4):
```
cd $NetMigrate/cpp/YCSB-client
./ycsb-fulva -run -db KV -P workloads/workloadb -P Fulva/kv_migration.properties -p threadcount=8 -s > ~/result/fulva-b-100.txt
```

## Start source migration push to migrate data

After running client of about 200 seconds, in another terminal in source server (netx7):
```
cd $NetMigrate/cpp/server/Fulva
bash run_src_push.sh
```

After migration finishes, you will get the raw data output ```~/result/fulva-b-100.txt``` in client server. You can draw throughput and latency figures from it using ```$NetMigrate/experiment_steps/figures/draw.py```. The trend in the figures will be similar as the below examples.
```
cp ~/result/fulva-b-100.txt $NetMigrate/experiment_steps/figures/
cd $NetMigrate/experiment_steps/figures/
mkdir latency_fig
mkdir thorughput_fig
python3 draw.py fulva b 100
```


Throughput:

<p align="center">
  <img src="./figures/fulva-b-100.png" width="500">
</p>

Median latency:

<p align="center">
  <img src="./figures/fulva-5-100-50.png" width="500">
</p>

99%-tail latency:

<p align="center">
  <img src="./figures/fulva-5-100-99.png" width="500">
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

