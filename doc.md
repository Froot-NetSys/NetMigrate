# Overview

# Parameters
## Switch  
* Key size: 4B, value size: 64B (but not limited to 64B)
* 4 key-value pairs in one multi-op query packet.
* Bucket id in switch: 32 bits  
* Version: 8 bits (to merge newer version of double read and distinguish reply from PriorityPulls and from successful local read)   
* MG ports range: 0xbee0 - 0xbeef, 48864-48879

# Design
## Membership table routing logic for Single READ:
```
  BF = 0, CBF = 0, not start migration; no false positive => direct to source  
  BF = 0, CBF = 1, bucket under migration => both source and destination; false positive of CBF : not start migration => direct to source   
  BF = 1, CBF = 0, complete migration => direct to destination; false positive of BF: not start migration => Priority Pull  
  BF = 1, CBF = 1, not possible;   
    false positives:  
      BF wrong, CBF correct, under migration => both source and destination  
      BF wrong, CBF false positive, not start migration => direct to source  
      BF correct, CBF false positive, complete migration => direct to destination  
      BF correct, CBF correct, not possible => none  
```

## Membership table routing logic for Single WRITE:
```
  BF = 0, CBF = 0, not start migration; no false positive => direct to source  
  BF = 0, CBF = 1, bucket under migration => direct to destination; false positive of CBF : not start migration => direct to source   
  BF = 1, CBF = 0, complete migration => direct to destination; false positive of BF: not start migration => Priority Pull  
  BF = 1, CBF = 1, not possible; => direct to destination   
    false positives:  
      BF wrong, CBF correct, under migration => both source and destination  
      BF wrong, CBF false positive, not start migration => direct to source  
      BF correct, CBF false positive, complete migration => direct to destination  
      BF correct, CBF correct, not possible => none  
```

## Proof of consistency guarantee of READ and WRITE
state machine


## Version control
version in read / multi-read reply is 8 bits (3 bits used currently).
```
Init at switch: 0xFF
001 / 000: whether read is sucessful (1) or not (0)
010 / 000: from destination (1) or source (0)
100 / 000: whether a reply for double read (1) or not (0)
```
3 cases:
1. only destinaiton return with successful read from destination Redis or from PriorityPull => version = 011 / 010
2. only source return => version = 001 / 000
3. both source and destination return (triggered by double read) => source reply version = 101 / 100; destination reply version = 111 / 110



# KVS Migration Packet Format
## Migration Control Packet 

### Migration Init
```
+-----------+-----------+-----------+----------------+--------+--------------+-----------+--------------+-----------+--------------+
|    ETH    |    IP     |    UDP    |    MG_INIT     |   SEQ  |  MG_DB_PORT  | MG_SRC_IP | MG_SRC_PORT  | MG_DST_IP | MG_DST_PORT  |
+-----------+-----------+-----------+----------------+--------+--------------+-----------+--------------+-----------+--------------+
```
### Migration Terminate
```
+-----------+-----------+-----------+----------------+--------+--------------+-----------+--------------+-----------+--------------+
|    ETH    |    IP     |    UDP    |     MG_TERM    |   SEQ  |  MG_DB_PORT  | MG_SRC_IP | MG_SRC_PORT  | MG_DST_IP | MG_DST_PORT  |
+-----------+-----------+-----------+----------------+--------+--------------+-----------+--------------+-----------+--------------+
```
### Migration Init REPLY
```
+-----------+-----------+-----------+---------------+--------+--------------+
|    ETH    |    IP     |    UDP    | MG_INIT_REPLY |   SEQ  |  MG_DB_PORT  |
+-----------+-----------+-----------+---------------+--------+--------------+
```
### Migration Terminate REPLY
```
+-----------+-----------+-----------+--------------------+--------+--------------+
|    ETH    |    IP     |    UDP    | MG_TERMINATE_REPLY |   SEQ  |  MG_DB_PORT  |
+-----------+-----------+-----------+--------------------+--------+--------------+
```
### Migration Group Start
```
+-----------+-----------+-----------+----------------+--------+--------------+------------+
|    ETH    |    IP     |    UDP    | MG_GROUP_START |   SEQ  |  MG_DB_PORT  | GROUP_ID   |
+-----------+-----------+-----------+----------------+--------+--------------+------------+
```

### Migration Group Complete
```
+-----------+-----------+-----------+-------------------+--------+--------------+------------+
|    ETH    |    IP     |    UDP    | MG_GROUP_COMPLETE |   SEQ  |  MG_DB_PORT  | GROUP_ID   |
+-----------+-----------+-----------+-------------------+--------+--------------+------------+
```

### Migration GROUP START REPLY
```
+-----------+-----------+-----------+----------------------+--------+--------------+
|    ETH    |    IP     |    UDP    | MG_GROUP_START_REPLY |   SEQ  |  MG_DB_PORT  |
+-----------+-----------+-----------+----------------------+--------+--------------+
```

### Migration GROUP COMPLETE REPLY
```
+-----------+-----------+-----------+-------------------------+--------+--------------+
|    ETH    |    IP     |    UDP    | MG_GROUP_COMPLETE_REPLY |   SEQ  |  MG_DB_PORT  |
+-----------+-----------+-----------+-------------------------+--------+--------------+
```

### Migrate Data 
```
+-----------+-----------+-----------+----------------+--------+--------------+---------+-----------+-----------+--------+-----------+-------------+
|    ETH    |    IP     |    UDP    |     MG_MIGR    |   SEQ  |  MG_DB_PORT  | KV_NUM  |   KEY_1   |  VALUE_1  |  ...   |   KEY_n   |   VALUE_n   |
+-----------+-----------+-----------+----------------+--------+--------------+---------+-----------+-----------+--------+-----------+-------------+
```

### Migrate Data REPLY
```
+-----------+-----------+-----------+------------------+--------+--------------+
|    ETH    |    IP     |    UDP    | MG_MIGRATE_REPLY |   SEQ  |  MG_DB_PORT  |
+-----------+-----------+-----------+------------------+--------+--------------+
```


## Client Request Packet 

### Single READ
```
+-----------+-----------+-----------+----------------+--------+-------------+-----------+---------+-----------+
|    ETH    |    IP     |    UDP    |    MG_READ     |   SEQ  | MG_DB_PORT  |   BITMAP  |   VER   |    KEY    |
+-----------+-----------+-----------+----------------+--------+-------------+-----------+---------+-----------+
```

### Single WRITE
```
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+-------------------+
|    ETH    |    IP     |    UDP    |    MG_WRITE    |   SEQ  | MG_DB_PORT  |   BITMAP  |    KEY    |        VALUE      |
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+-------------------+
```

### Single DELETE
```
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+
|    ETH    |    IP     |    UDP    |   MG_DELETE    |   SEQ  | MG_DB_PORT  |   BITMAP  |    KEY    |
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+
```

### Multi-READ
```
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+-----------+-----------+-----------+
|    ETH    |    IP     |    UDP    | MG_MULTI_READ  |   SEQ  | MG_DB_PORT  |   BITMAP  |   KEY_1   |   KEY_2   |   KEY_3   |   KEY_4   |
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+-----------+-----------+-----------+
```

### Multi-WRITE
```
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+-------------------+-----------+-------------------+-----------+-------------------+-----------+-------------------+
|    ETH    |    IP     |    UDP    | MG_MULTI_WRITE |   SEQ  | MG_DB_PORT  |   BITMAP  |   KEY_1   |      VALUE_1      |   KEY_2   |      VALUE_2      |   KEY_3   |      VALUE_3      |   KEY_4   |      VALUE_4      |
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+-------------------+-----------+-------------------+-----------+-------------------+-----------+-------------------+
```

### Multi-DELETE
```
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+-----------+-----------+-----------+
|    ETH    |    IP     |    UDP    | MG_MULTI_DELETE|   SEQ  | MG_DB_PORT  |   BITMAP  |   KEY_1   |   KEY_2   |   KEY_3   |   KEY_4   |
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+-----------+-----------+-----------+
```

### Single READ REPLY
```
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+
|    ETH    |    IP     |    UDP    |  MG_READ_REPLY |   SEQ  | MG_DB_PORT  |  VERSION  |   VALUE   |
+-----------+-----------+-----------+----------------+--------+-------------+-----------+-----------+
```

### Single WRITE REPLY
```
+-----------+-----------+-----------+----------------+--------+-------------+-----------+
|    ETH    |    IP     |    UDP    | MG_WRITE_REPLY |   SEQ  | MG_DB_PORT  |   VER     |
+-----------+-----------+-----------+----------------+--------+-------------+-----------+
```
### Single DELETE REPLY
```
+-----------+-----------+-----------+-----------------+--------+-------------+-----------+
|    ETH    |    IP     |    UDP    | MG_DELETE_REPLY |   SEQ  | MG_DB_PORT  |   VER     |
+-----------+-----------+-----------+-----------------+--------+-------------+-----------+
```
