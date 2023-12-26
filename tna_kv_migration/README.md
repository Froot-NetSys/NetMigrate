The experiments are done with bf-sde-9.4.0 and python2.

# P4Studio setup
Set the environmen variables:
```
source ~/.bashrc
```
or type the following commands:
```
export NetMigrate=/home/zeying/NetMigrate
export SDE=/home/zhuolong/mysde/bf-sde-9.4.0
export SDE_INSTALL=$SDE/install
export PATH=$SDE_INSTALL/bin:$PATH
```
# KV_Migration
## Compile the kv_migration P4 program to be loaded into the switch
```
cd $SDE
./p4_build.sh $NetMigrate/tna_kv_migration/migration/kv_migration.p4
```

## Run switch program
**Terminal 1:** Running switchd first in one terminal and keep this terminal:
```
cd $SDE/
./run_switchd.sh -p kv_migration
```
After *run_switchd.sh* finishes (in tens of seconds), it will enter the "bf-shell" mode. One can type commands in the shell. We will use this shell to add ports interactively:
```
ucli
pm
port-add -/- 40G NONE
port-enb -/-
```

**Terminal 2:** Keep Terminal 1 running. Open a new terminal to login the switch and run the following commands:
```
cd $NetMigrate/tna_kv_migration/migration/controller
python kv_controller.py
```



## Manually set up ARP in end-hosts
We need to manually add static ARP entries in 3 servers.
```
sudo arp -s remote_ip remote_mac
```
In our testbed, set ARP entries in next4, next5, next7 servers:
```
cd ~/fast_ae
bash set_arp.sh
```
