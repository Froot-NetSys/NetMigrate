The experiments are done with bf-sde-9.4.0 and python2.

# P4Studio setup
```
export SDE=path/to/bf-sde-9.4.0
export SDE_INSTALL=$SDE/install
export PATH=$SDE_INSTALL/bin:$PATH
```
# KV_Migration
## Compile and install kv_migration project
```
cd $SDE
./p4_build.sh $NetMigrate/tna_kv_migration/migration/kv_migration.p4
```

## Run switch
**Terminal 1** Running switchd first in one terminal and keep this terminal:
```
cd $SDE/
./run_switchd.sh -p kv_migration
#After bf-shell starts, add ports in switchd terminal(interactive mode):
ucli
pm
port-add -/- 40G NONE
port-enb -/-
```

**Terminal 2**Running controller in another terminal:
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
sudo arp -s 10.1.1.4 3c:fd:fe:aa:46:68
sudo arp -s 10.1.1.5 3c:fd:fe:ab:de:f0
sudo arp -s 10.1.1.7 3c:fd:fe:ab:e0:50
```
