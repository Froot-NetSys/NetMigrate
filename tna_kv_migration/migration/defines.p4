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


// Migration Parameters
#define _MG_MAX_MIGRATION_PAIR_ENTRIES  32
#define _MG_MAX_BLOOM_FILTER_ENTRIES    32768  
#define _MG_BLOOM_FILTER_WIDTH          15
#define _MG_COUNTING_BLOOM_FILTER_WIDTH 4 // 18
#define _MG_COUNTING_BLOOM_FILTER_ENTRIES 16 // 262144 
#define _MG_GROUP_ID_WIDTH              32
#define _MG_REPLY_FILTER_WIDTH          32
#define _MG_REPLY_FILTER_ENTRIES        32w65536


#define UDP_MG_PORT_BASE               0xbe00
#define UDP_MG_PORT_MASK               0xff00
#define RECIRC_PORT                    68
