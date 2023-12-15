#pragma once

// Client requests op
#define _MG_READ                    0x01
#define _MG_WRITE                   0x02
#define _MG_DELETE                  0x03
#define _MG_READ_REPLY              0x04
#define _MG_WRITE_REPLY             0x05
#define _MG_DELETE_REPLY            0x06

#define _MG_MULTI_READ              0x07
#define _MG_MULTI_WRITE             0x08
#define _MG_MULTI_DELETE            0x09
#define _MG_MULTI_READ_REPLY        0x0A
#define _MG_MULTI_WRITE_REPLY       0x0B
#define _MG_MULTI_DELETE_REPLY      0x0C

// Server Migration op
#define _MG_INIT                    0x10
#define _MG_INIT_REPLY              0x11
#define _MG_MIGRATE                 0x12
#define _MG_MIGRATE_REPLY           0x13
#define _MG_TERMINATE               0x14
#define _MG_TERMINATE_REPLY         0x15

#define _MG_MIGRATE_GROUP_START     0x16
#define _MG_MIGRATE_GROUP_START_REPLY 0x17
#define _MG_MIGRATE_GROUP_COMPLETE  0x18  // Source send this reply when a group finishes migration
#define _MG_MIGRATE_GROUP_COMPLETE_REPLY 0x19

// Other Cache op
#define _CACHE_HOT_READ                0x20

#define _CACHE_COUNT_CLEAR             0x21

// Fulva
#define FULVA_MCR_BEGIN                0x30
#define FULVA_MCR_BEGIN_REPLY          0x31
#define FULVA_MG_END                   0x32
#define FULVA_MG_END_REPLY             0x33

// Internal Cache States
#define _CACHE_MISS                    0
#define _CACHE_HIT                     1
#define _CACHE_EXIT                    3

#define _CACHE_VALID                   1
#define _CACHE_INVALID                 0

// Migration Parameters
#define _MG_MAX_MIGRATION_PAIR_ENTRIES  128
#define _MG_MAX_BLOOM_FILTER_ENTRIES    262144  
#define _MG_BLOOM_FILTER_WIDTH          18
#define _MG_COUNTING_BLOOM_FILTER_WIDTH 16
#define _MG_COUNTING_BLOOM_FILTER_ENTRIES 65536
#define _MG_GROUP_ID_WIDTH              32

// Cache Parameters for one cache register
#define _CACHE_MAX_CACHE_ENTRIES       65536
#define _CACHE_VAL_REGISTER_SIZE       32768
#define _CACHE_VAL_REGISTER_SIZE_2     65536
#define _CACHE_VAL_REGISTER_SIZE_3     98304
#define _CACHE_VAL_REGISTER_SIZE_4     131072

#define _MG_PORT                       8888

