#ifndef _MIGRATION_HEADERS_
#define _MIGRATION_HEADERS_

typedef bit<48> mac_addr_t;
typedef bit<32> ipv4_addr_t;

typedef bit<16> ether_type_t;
const ether_type_t ETHERTYPE_IPV4 = 16w0x0800;

typedef bit<8> ip_protocol_t;
const ip_protocol_t IP_PROTOCOLS_TCP = 6;
const ip_protocol_t IP_PROTOCOLS_UDP = 17;

#define _MG_KEY_WIDTH   32  
#define _MG_SEQ_WIDTH   64
#define _MG_FID_WIDTH   16


header ethernet_t {
    mac_addr_t    dstAddr;
    mac_addr_t    srcAddr;
    ether_type_t    etherType;
}

header ipv4_t {
    bit<4> version;
    bit<4> ihl;
    bit<8> diffserv;
    bit<16> totalLen;
    bit<16> identification;
    bit<3> flags;
    bit<13> fragOffset;
    bit<8> ttl;
    bit<8> protocol;
    bit<16> hdrChecksum;
    ipv4_addr_t srcAddr;
    ipv4_addr_t dstAddr;
}

header tcp_t {
    bit<16> srcPort;
    bit<16> dstPort;
    bit<32> seqNo;
    bit<32> ackNo;
    bit<4> dataOffset;
    bit<3> res;
    bit<3> ecn;
    bit<6> ctrl;
    bit<16> window;
    bit<16> checksum;
    bit<16> urgentPtr;
}

header udp_t {
    bit<16> srcPort;
    bit<16> dstPort;
    bit<16> udpLength;
    bit<16> checksum;
}

header mg_hdr_t {
    bit<8> op;
    bit<64> seq;
    bit<16> dbPort;
}

header mg_pair_hdr_t {
    ipv4_addr_t srcAddr;
    bit<16> srcPort;
    ipv4_addr_t dstAddr;
    bit<16> dstPort;
}

header mg_dict_mask_h {
    bit<8> size_exp;
    bit<8> remain_exp;
}

header mg_bitmap_hdr_t {
    bit<8> t;
}

header mg_key_s1_t {
    bit<_MG_KEY_WIDTH> key;
}

header mg_key_s2_t {
    bit<_MG_KEY_WIDTH> key;
}

header mg_key_s3_t {
    bit<_MG_KEY_WIDTH> key;
}

header mg_key_s4_t {
    bit<_MG_KEY_WIDTH> key;
}

// version bits are used for merging two double read packets
header mg_version_s1_t {
    bit<8> ver;
}

header mg_version_s2_t {
    bit<8> ver;
}

header mg_version_s3_t {
    bit<8> ver;
}

header mg_version_s4_t {
    bit<8> ver;
}

header mg_bridged_metadata_t {
    pkt_type_t pkt_type;
    bit<8> do_egr_mirroring;  //  Enable egress mirroring
    bit<8> redirect_to_dst;
    mac_addr_t eth_dst_addr;
    ipv4_addr_t ip_dst_addr;
    bit<16> db_dst_port;
}

header mirror_I2E_h {
    pkt_type_t pkt_type;
    mac_addr_t eth_dst_addr;
    ipv4_addr_t ip_dst_addr;
    bit<16> db_dst_port;
}

header mirror_E2E_h {
    pkt_type_t pkt_type;
    bit<8> redirect_to_dst;
    mac_addr_t eth_dst_addr;
    ipv4_addr_t ip_dst_addr;
    bit<16> db_dst_port;
}

header mg_group_id_t {
    bit<_MG_GROUP_ID_WIDTH> group_id;
}

struct migration_header_t {
    mg_bridged_metadata_t bridged_md;
    mirror_I2E_h mirror_I2E_md;
    mirror_E2E_h mirror_E2E_md;
    ethernet_t ethernet;
    ipv4_t ipv4;
    tcp_t tcp;
    udp_t udp;
    mg_hdr_t mg_hdr;  
    mg_pair_hdr_t mg_pair_hdr;
    mg_dict_mask_h mg_dict_mask;
    mg_group_id_t mg_group_id;
    mg_bitmap_hdr_t mg_bitmap_hdr;
    mg_version_s1_t mg_ver_s1;
    mg_key_s1_t mg_key_s1;
    mg_version_s2_t mg_ver_s2;
    mg_key_s2_t mg_key_s2;
    mg_version_s3_t mg_ver_s3;
    mg_key_s3_t mg_key_s3;
    mg_version_s4_t mg_ver_s4;
    mg_key_s4_t mg_key_s4;
}


#endif