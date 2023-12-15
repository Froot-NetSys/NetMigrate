#pragma once

#include "constants.h"
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>

#define MTU 1500

#define OP_SIZE 1
#define SEQ_SIZE 8
#define PORT_SIZE 2
#define HDR_SIZE 11
#define KEY_SIZE 4
#define VAL_SIZE 64 // Value size can be tuned larger
#define VER_SIZE 1
#define ADDR_SIZE 4
#define ID_SIZE 4
#define NUM_SIZE 1

#define PAYLOAD_SIZE 1428
#define MAX_KV_NUM (PAYLOAD_SIZE / (KEY_SIZE + VAL_SIZE)) // 21

#define BITMAP_SIZE 1

#define MAX_HASH_NUM 160
#define KEY_PAYLOAD_SIZE (MAX_HASH_NUM * KEY_SIZE) // 640

#define FULVA_MAX_MIGR_THREAD_NUM 32

#define htonll(x) ((1==htonl(1)) ? (x) : \
    ((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
#define ntohll(x) ((1==ntohl(1)) ? (x) : \
    ((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))


inline uint32_t ipv4Addr_to_i32(std::string ip) {
    std::stringstream ss(ip);
    std::vector<uint32_t> vec;
    for (int i; ss >> i; ) {
        vec.push_back(i);
        if (ss.peek() == '.') 
            ss.ignore();
    }
    uint32_t res = 0;
    for (size_t i = 0; i < vec.size(); i++) {
        res = res * 256 + vec[i];
    }
    return res;
}


struct __attribute__((packed)) mg_hdr_t {
    uint8_t op;
    uint64_t seq;
    uint16_t dbPort;
};

// migration control packets
// Packets: migration_init, migration_terminate
struct __attribute__((packed)) mg_ctrl {
    uint32_t srcAddr;
    uint16_t srcPort;
    uint32_t dstAddr;
    uint16_t dstPort;
    uint8_t size_exp;
    uint8_t remain_exp;
};

// Packets: migration_group_start, migration_group_complete
struct __attribute__((packed)) mg_group_ctrl {
    uint32_t group_id;
};

// Packets: migration_init_reply, migration_terminate_reply, 
//          migration_group_start_reply, migration_group_complete_reply
//          migrate_data_reply
struct __attribute__((packed)) mg_ctrl_reply {

};

struct __attribute__((packed)) kv_pair {
    char key[KEY_SIZE];
    char val[VAL_SIZE];
};

struct __attribute__((packed)) ver_val_pair {
    uint8_t ver;
    char val[VAL_SIZE];
};

struct __attribute__((packed)) ver_key_pair {
    uint8_t ver;
    char key[KEY_SIZE];
};


// Packets: migrate_data
struct __attribute__((packed)) mg_data {
    uint8_t kv_num;
    struct kv_pair kv_payload[MAX_KV_NUM];
}; 


// Client Requests
struct __attribute__((packed)) mg_read {
    uint8_t bitmap;
    uint8_t ver;
    char key[KEY_SIZE];
};

struct __attribute__((packed)) mg_write {
    uint8_t bitmap;
    char key[KEY_SIZE];
    char val[VAL_SIZE];
};

struct __attribute__((packed)) mg_delete {
    uint8_t bitmap;
    char key[KEY_SIZE];
};

struct __attribute__((packed)) mg_read_reply {
    uint8_t bitmap;
    uint8_t ver;
    char val[VAL_SIZE]; // treat as payload in switch
};

struct __attribute__((packed)) mg_write_reply {
    uint8_t bitmap;
    uint8_t ver;
};

struct __attribute__((packed)) mg_delete_reply {
    uint8_t bitmap;
    uint8_t ver;
};

struct __attribute__((packed)) mg_multi_read {
    uint8_t bitmap;
    struct ver_key_pair ver_key[4];
};

struct __attribute__((packed)) mg_multi_write {
    uint8_t bitmap;
    char keys[4 * KEY_SIZE];
    char vals[4 * VAL_SIZE]; // treat as payload in switch
};

struct __attribute__((packed)) mg_multi_delete {
    uint8_t bitmap;
    char keys[4 * KEY_SIZE];
};

// Only used for without switch migrate protocols
struct __attribute__((packed)) mg_multi_read_reply {
    uint8_t bitmap;
    struct ver_val_pair ver_val[4];
};

// Only used for without switch migrate protocols
struct __attribute__((packed)) mg_multi_write_reply {
    uint8_t bitmap;
    uint8_t ver[4];
};

// Only used for without switch migrate protocols
struct __attribute__((packed)) mg_multi_delete_reply {
    uint8_t bitmap;
    uint8_t ver[4];
};

// for Fulva SPH udpate
struct __attribute__((packed)) sph_metadata {
    uint8_t num;
    char keys[MAX_HASH_NUM * KEY_SIZE]; // Note: we cannot get key hash values from destination agent so our sampling pull uses keys 
};



inline void SerializeMgHdr(char * buf, const struct mg_hdr_t * mg_hdr) {
    uint64_t seq = htonll(mg_hdr->seq);
    uint16_t dbPort = htons(mg_hdr->dbPort);
    memcpy(buf, &mg_hdr->op, OP_SIZE);
    memcpy(buf + OP_SIZE, &seq, SEQ_SIZE);
    memcpy(buf + OP_SIZE + SEQ_SIZE, &dbPort, PORT_SIZE);
}

inline void DeserializeMgHdr(const char * buf, struct mg_hdr_t * mg_hdr) { 
    memcpy(&mg_hdr->op, buf, OP_SIZE);
    memcpy(&mg_hdr->seq, buf + OP_SIZE, SEQ_SIZE);
    memcpy(&mg_hdr->dbPort, buf + OP_SIZE + SEQ_SIZE, PORT_SIZE);
    mg_hdr->seq = ntohll(mg_hdr->seq);
    mg_hdr->dbPort = ntohs(mg_hdr->dbPort);
}

// Migration Control / Data
// Assume mg_hdr is extracted before
inline void SerializeMgCtrlPkt(char * buf, const struct mg_ctrl * pkt) {
    uint32_t srcAddr = htonl(pkt->srcAddr);
    uint16_t srcPort = htons(pkt->srcPort);
    uint32_t dstAddr = htonl(pkt->dstAddr);
    uint16_t dstPort = htons(pkt->dstPort);
    memcpy(buf + HDR_SIZE, &srcAddr, ADDR_SIZE);
    memcpy(buf + HDR_SIZE + ADDR_SIZE, &srcPort, PORT_SIZE);
    memcpy(buf + HDR_SIZE + ADDR_SIZE + PORT_SIZE, &dstAddr, ADDR_SIZE);
    memcpy(buf + HDR_SIZE + ADDR_SIZE + PORT_SIZE + ADDR_SIZE, &dstPort, PORT_SIZE);
    memcpy(buf + HDR_SIZE + ADDR_SIZE + PORT_SIZE + ADDR_SIZE + PORT_SIZE, &pkt->size_exp, 1);
    memcpy(buf + HDR_SIZE + ADDR_SIZE + PORT_SIZE + ADDR_SIZE + PORT_SIZE + 1, &pkt->remain_exp, 1);
}

inline void DeserializeMgCtrlPkt(const char * buf, struct mg_ctrl * pkt) {
    memcpy(&pkt->srcAddr, buf + HDR_SIZE, ADDR_SIZE);
    memcpy(&pkt->srcPort, buf + HDR_SIZE + ADDR_SIZE, PORT_SIZE);
    memcpy(&pkt->dstAddr, buf + HDR_SIZE + ADDR_SIZE + PORT_SIZE, ADDR_SIZE);
    memcpy(&pkt->dstPort, buf + HDR_SIZE + ADDR_SIZE + PORT_SIZE + ADDR_SIZE, PORT_SIZE);
    memcpy(&pkt->size_exp, buf + HDR_SIZE + ADDR_SIZE + PORT_SIZE + ADDR_SIZE + PORT_SIZE, 1);
    memcpy(&pkt->remain_exp, buf + HDR_SIZE + ADDR_SIZE + PORT_SIZE + ADDR_SIZE + PORT_SIZE + 1, 1);
    pkt->srcAddr = ntohl(pkt->srcAddr);
    pkt->srcPort = ntohs(pkt->srcPort);
    pkt->dstAddr = ntohl(pkt->dstAddr);
    pkt->dstPort = ntohs(pkt->dstPort);
}

inline void SerializeMgGroupCtrlPkt(char * buf, const struct mg_group_ctrl * pkt) {
    uint32_t group_id = htonl(pkt->group_id);
    memcpy(buf + HDR_SIZE, &group_id, ID_SIZE);
}

inline void DeserializeMgGroupCtrlPkt(const char * buf, struct mg_group_ctrl * pkt) {
    memcpy(&pkt->group_id, buf + HDR_SIZE, ID_SIZE);
    pkt->group_id = ntohl(pkt->group_id);
}

// MgDataPkt no need to be parsed by switch, thus bypass byte order changing
inline void SerializeMgDataPkt(char * buf, const struct mg_data * pkt) { 
    memcpy(buf + HDR_SIZE, &pkt->kv_num, NUM_SIZE);
    memcpy(buf + HDR_SIZE + NUM_SIZE, pkt->kv_payload, PAYLOAD_SIZE); 
}

inline void DeserializeMgDataPkt(const char * buf, struct mg_data * pkt) {
    memcpy(&pkt->kv_num, buf + HDR_SIZE, NUM_SIZE);
    memcpy(pkt->kv_payload, buf + HDR_SIZE + NUM_SIZE, PAYLOAD_SIZE);
}


// Client Requests
// Assume mg_hdr is extracted before
inline void SerializeReadPkt(char * buf, const struct mg_read * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    memcpy(buf + HDR_SIZE + BITMAP_SIZE, &pkt->ver, VER_SIZE);
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, pkt->key, KEY_SIZE);
}

inline void DeserializeReadPkt(const char * buf, struct mg_read * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    memcpy(&pkt->ver, buf + HDR_SIZE + BITMAP_SIZE, VER_SIZE);
    memcpy(pkt->key, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, KEY_SIZE);
}

inline void SerializeReadReplyPkt(char * buf, const struct mg_read_reply * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    memcpy(buf + HDR_SIZE + BITMAP_SIZE, &pkt->ver, VER_SIZE);
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, pkt->val, VAL_SIZE);
}

inline void DeserializeReadReplyPkt(const char * buf, struct mg_read_reply * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    memcpy(&pkt->ver, buf + HDR_SIZE + BITMAP_SIZE, VER_SIZE);
    memcpy(pkt->val, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, VAL_SIZE);
}

// For Fulva MCR updates from destination to client
inline void SerializeReadReplyMCRPkt(char * buf, const uint32_t * mcr, const size_t mcr_size) {
    uint32_t mcr_tmp[FULVA_MAX_MIGR_THREAD_NUM];
    for (size_t i = 0; i < mcr_size; i++) {
        mcr_tmp[i] = htonl(mcr[i]);
    }
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE + VAL_SIZE, mcr_tmp, 4 * mcr_size);
}

inline void DeserializeReadReplyMCRPkt(const char * buf, uint32_t * mcr, const size_t mcr_size) {
    memcpy(mcr, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE + VAL_SIZE, 4 * mcr_size);
    for (size_t i = 0; i < mcr_size; i++) {
        mcr[i] = ntohl(mcr[i]);
    }
}

// For Fulva SPH updates from destination to client, no need to parse by switch
inline void SerializeReadReplySPHPkt(char * buf, const struct sph_metadata * metadata) {
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE + VAL_SIZE, metadata, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void DeserializeReadReplySPHPkt(const char * buf, struct sph_metadata * metadata) {
    memcpy(metadata, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE + VAL_SIZE, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void SerializeWritePkt(char * buf, const struct mg_write * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    memcpy(buf + HDR_SIZE + BITMAP_SIZE, pkt->key, KEY_SIZE);
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + KEY_SIZE, pkt->val, VAL_SIZE);
}

inline void DeserializeWritePkt(const char * buf, struct mg_write * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    memcpy(pkt->key, buf + HDR_SIZE + BITMAP_SIZE, KEY_SIZE);
    memcpy(pkt->val, buf + HDR_SIZE + BITMAP_SIZE + KEY_SIZE, VAL_SIZE);
}

inline void SerializeWriteReplyPkt(char * buf, const struct mg_write_reply * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    memcpy(buf + HDR_SIZE + BITMAP_SIZE, &pkt->ver, VER_SIZE);
}

inline void DeserializeWriteReplyPkt(const char * buf, struct mg_write_reply * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    memcpy(&pkt->ver, buf + HDR_SIZE + BITMAP_SIZE, VER_SIZE);
}

// For Fulva MCR updates from destination to client
inline void SerializeWriteReplyMCRPkt(char * buf, const uint32_t * mcr, const size_t mcr_size) {
    uint32_t mcr_tmp[FULVA_MAX_MIGR_THREAD_NUM];
    for (size_t i = 0; i < mcr_size; i++) {
        mcr_tmp[i] = htonl(mcr[i]);
    }
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, mcr_tmp, mcr_size * 4);
}

inline void DeserializeWriteReplyMCRPkt(const char * buf, uint32_t * mcr, const size_t mcr_size) {
    memcpy(mcr, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, mcr_size * 4);
    for (size_t i = 0; i < mcr_size; i++) {
        mcr[i] = ntohl(mcr[i]);
    }
}

// For Fulva SPH updates from destination to client, no need to parse by switch
inline void SerializeWriteReplySPHPkt(char * buf, const struct sph_metadata * metadata) {
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, metadata, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void DeserializeWriteReplySPHPkt(const char * buf, struct sph_metadata * metadata) {
    memcpy(metadata, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void SerializeDeletePkt(char * buf, const struct mg_delete * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    memcpy(buf + HDR_SIZE + BITMAP_SIZE, pkt->key, KEY_SIZE);
}

inline void DeserializeDeletePkt(const char * buf, struct mg_delete * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    memcpy(pkt->key, buf + HDR_SIZE + BITMAP_SIZE, KEY_SIZE);
}

inline void SerializeDeleteReplyPkt(char * buf, const struct mg_delete_reply * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE); 
    memcpy(buf + HDR_SIZE + BITMAP_SIZE, &pkt->ver, VER_SIZE);
}

inline void DeserializeDeleteReplyPkt(const char * buf, struct mg_delete_reply * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    memcpy(&pkt->ver, buf + HDR_SIZE + BITMAP_SIZE, VER_SIZE);
}

// For Fulva MCR updates from destination to client
inline void SerializeDeleteReplyMCRPkt(char * buf, const uint32_t * mcr, const size_t mcr_size) {
    uint32_t mcr_tmp[FULVA_MAX_MIGR_THREAD_NUM];
    for (size_t i = 0; i < mcr_size; i++) {
        mcr_tmp[i] = htonl(mcr[i]);
    }
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, mcr_tmp, mcr_size * 4);
}

inline void DeserializeDeleteReplyMCRPkt(const char * buf, uint32_t * mcr, const size_t mcr_size) {
    memcpy(mcr, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, mcr_size * 4);
    for (size_t i = 0; i < mcr_size; i++) {
        mcr[i] = ntohl(mcr[i]);
    }
}

// For Fulva SPH updates from destination to client, no need to parse by switch
inline void SerializeDeleteReplySPHPkt(char * buf, const struct sph_metadata * metadata) {
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, metadata, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void DeserializeDeleteReplySPHPkt(const char * buf, struct sph_metadata * metadata) {
    memcpy(metadata, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void SerializeMultiReadPkt(char * buf, const struct mg_multi_read * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + KEY_SIZE) * i, &pkt->ver_key[i].ver, VER_SIZE);
        memcpy(buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + KEY_SIZE) * i + VER_SIZE, pkt->ver_key[i].key, KEY_SIZE);
    }
}

inline void DeserializeMultiReadPkt(const char * buf, struct mg_multi_read * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(&pkt->ver_key[i].ver, buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + KEY_SIZE) * i, VER_SIZE);
        memcpy(pkt->ver_key[i].key, buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + KEY_SIZE) * i + VER_SIZE, KEY_SIZE);
    }
}

inline void SerializeMultiReadReplyPkt(char * buf, const struct mg_multi_read_reply * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + VAL_SIZE) * i, &pkt->ver_val[i].ver, VER_SIZE);
        memcpy(buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + VAL_SIZE) * i + VER_SIZE, pkt->ver_val[i].val, VAL_SIZE);
    }
}

inline void DeserializeMultiReadReplyPkt(const char * buf, struct mg_multi_read_reply * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(&pkt->ver_val[i].ver, buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + VAL_SIZE) * i, VER_SIZE);
        memcpy(pkt->ver_val[i].val, buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + VAL_SIZE) * i + VER_SIZE, VAL_SIZE);
    }
}


// For Fulva MCR updates from destination to client
inline void SerializeMultiReadReplyMCRPkt(char * buf, const uint32_t * mcr, const size_t mcr_size) {
    uint32_t mcr_tmp[FULVA_MAX_MIGR_THREAD_NUM];
    for (size_t i = 0; i < mcr_size; i++) {
        mcr_tmp[i] = htonl(mcr[i]);
    }
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + VAL_SIZE)*4, mcr_tmp, mcr_size * 4);
}

inline void DeserializeMultiReadReplyMCRPkt(const char * buf, uint32_t * mcr, const size_t mcr_size) {
    memcpy(mcr, buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + VAL_SIZE)*4, mcr_size * 4);
    for (size_t i = 0; i < mcr_size; i++) {
        mcr[i] = ntohl(mcr[i]);
    }
}

// For Fulva SPH updates from destination to client, no need to parse by switch
inline void SerializeMultiReadReplySPHPkt(char * buf, const struct sph_metadata * metadata) {
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + VAL_SIZE)*4, metadata, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void DeserializeMultiReadReplySPHPkt(const char * buf, struct sph_metadata * metadata) {
    memcpy(metadata, buf + HDR_SIZE + BITMAP_SIZE + (VER_SIZE + VAL_SIZE)*4, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void SerializeMultiWritePkt(char * buf, const struct mg_multi_write * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(buf + HDR_SIZE + BITMAP_SIZE + KEY_SIZE * i, &pkt->keys[i * KEY_SIZE], KEY_SIZE);
    }
    for (int i = 0; i < 4; i++) {
        memcpy(buf + HDR_SIZE + BITMAP_SIZE + KEY_SIZE * 4 + VAL_SIZE * i, &pkt->vals[i * VAL_SIZE], VAL_SIZE);
    }
}

inline void DeserializeMultiWritePkt(const char * buf, struct mg_multi_write * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE , BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(&pkt->keys[i * KEY_SIZE], buf + HDR_SIZE + BITMAP_SIZE + KEY_SIZE * i, KEY_SIZE);
    }
    for (int i = 0; i < 4; i++) {
        memcpy(&pkt->vals[i * VAL_SIZE], buf + HDR_SIZE + BITMAP_SIZE + KEY_SIZE * 4 + VAL_SIZE * i, VAL_SIZE);
    }
}


inline void SerializeMultiWriteReplyPkt(char * buf, const struct mg_multi_write_reply * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE * i, &pkt->ver[i], VER_SIZE);
    }
}

inline void DeserializeMultiWriteReplyPkt(const char * buf, struct mg_multi_write_reply * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(&pkt->ver[i], buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE * i, VER_SIZE);
    }
}

// For Fulva MCR updates from destination to client
inline void SerializeMultiWriteReplyMCRPkt(char * buf, const uint32_t * mcr, const size_t mcr_size) {
    uint32_t mcr_tmp[FULVA_MAX_MIGR_THREAD_NUM];
    for (size_t i = 0; i < mcr_size; i++) {
        mcr_tmp[i] = htonl(mcr[i]);
    }
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE*4, mcr_tmp, mcr_size * 4);
}

inline void DeserializeMultiWriteReplyMCRPkt(const char * buf, uint32_t * mcr, const size_t mcr_size) {
    memcpy(mcr, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE*4, mcr_size * 4);
    for (size_t i = 0; i < mcr_size; i++) {
        mcr[i] = ntohl(mcr[i]);
    }
}

// For Fulva SPH updates from destination to client, no need to parse by switch
inline void SerializeMultiWriteReplySPHPkt(char * buf, const struct sph_metadata * metadata) {
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE*4, metadata, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void DeserializeMultiWriteReplySPHPkt(const char * buf, struct sph_metadata * metadata) {
    memcpy(metadata, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE*4, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void SerializeMultiDeletePkt(char * buf, const struct mg_multi_delete * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(buf + HDR_SIZE + BITMAP_SIZE + KEY_SIZE * i, &pkt->keys[i * KEY_SIZE], KEY_SIZE);
    }
}

inline void DeserializeMultiDeletePkt(const char * buf, struct mg_multi_delete * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(&pkt->keys[i * KEY_SIZE], buf + HDR_SIZE + BITMAP_SIZE + KEY_SIZE * i, KEY_SIZE);
    }
}

inline void SerializeMultiDeleteReplyPkt(char * buf, const struct mg_multi_delete_reply * pkt) {
    memcpy(buf + HDR_SIZE, &pkt->bitmap, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE * i, &pkt->ver[i], VER_SIZE);
    }
}

inline void DeserializeMultiDeleteReplyPkt(const char * buf, struct mg_multi_delete_reply * pkt) {
    memcpy(&pkt->bitmap, buf + HDR_SIZE, BITMAP_SIZE);
    for (int i = 0; i < 4; i++) {
        memcpy(&pkt->ver[i], buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE * i, VER_SIZE);
    }
}

// For Fulva MCR updates from destination to client
inline void SerializeMultiDeleteReplyMCRPkt(char * buf, const uint32_t * mcr, const size_t mcr_size) {
    uint32_t mcp_tmp[FULVA_MAX_MIGR_THREAD_NUM];
    for (size_t i = 0; i < mcr_size; i++) {
        mcp_tmp[i] = htonl(mcr[i]);
    }
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE*4, mcp_tmp, mcr_size * 4);
}

inline void DeserializeMultiDeleteReplyMCRPkt(const char * buf, uint32_t * mcr, const size_t mcr_size) {
    memcpy(mcr, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE*4, mcr_size * 4);
    for (size_t i = 0; i < mcr_size; i++) {
        mcr[i] = ntohl(mcr[i]);
    }
}

// For Fulva SPH updates from destination to client, no need to parse by switch
inline void SerializeMultiDeleteReplySPHPkt(char * buf, const struct sph_metadata * metadata) {
    memcpy(buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE*4, metadata, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void DeserializeMultiDeleteReplySPHPkt(const char * buf, struct sph_metadata * metadata) {
    memcpy(metadata, buf + HDR_SIZE + BITMAP_SIZE + VER_SIZE*4, NUM_SIZE + KEY_PAYLOAD_SIZE); 
}

inline void SerializeMCRBeginPkt(char * buf, uint32_t * mcr, const size_t mcr_size, const uint32_t remain_size_exp) {
    uint32_t mcr_tmp[FULVA_MAX_MIGR_THREAD_NUM];
    for (size_t i = 0; i < mcr_size; i++) {
        mcr_tmp[i] = htonl(mcr[i]);
    }
    memcpy(buf + HDR_SIZE, mcr_tmp, mcr_size * 4);
    uint32_t sz_exp = htonl(remain_size_exp);
    memcpy(buf + HDR_SIZE + mcr_size * 4, &sz_exp, 4);
}

inline void DeserializeMCRBeginPkt(const char * buf, uint32_t * mcr, const size_t mcr_size, uint32_t * remain_size_exp) {
    memcpy(mcr, buf + HDR_SIZE, mcr_size * 4);
    for (size_t i = 0; i < mcr_size; i++) {
        mcr[i] = ntohl(mcr[i]);
    }
    memcpy(remain_size_exp, buf + HDR_SIZE + mcr_size * 4, 4);
    *remain_size_exp = ntohl(*remain_size_exp);
}

inline void SerializeKey(const char * key, char * key_pkt) {
    memcpy(key_pkt, key, KEY_SIZE);
}

inline void DeserializeKey(char * key, char * key_pkt) {
    memcpy(key, key_pkt, KEY_SIZE);
    key[KEY_SIZE] = 0;
}

inline void SerializeVal(const char * val, char * val_pkt) {
    memcpy(val_pkt, val, VAL_SIZE);
}

inline void DeserializeVal(char * val, char * val_pkt) {
    memcpy(val, val_pkt, VAL_SIZE);
    val[VAL_SIZE] = 0;
}
