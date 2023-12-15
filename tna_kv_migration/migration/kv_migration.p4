/* -*- P4_16 -*- */

/*
    Copyright (C) 2023 Zeying Zhu, University of Maryland, College Park
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.
    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/


#include <core.p4>
#if __TARGET_TOFINO__ == 2
#include <t2na.p4>
#else
#include <tna.p4>
#endif

#include "defines.p4"

#if __TARGET_TOFINO__ == 1
typedef bit<3> mirror_type_t;
#else
typedef bit<4> mirror_type_t;
#endif
const mirror_type_t MIRROR_TYPE_I2E = 1;
const mirror_type_t MIRROR_TYPE_E2E = 2;

typedef bit<8> pkt_type_t;
const pkt_type_t PKT_TYPE_NORMAL = 1;
const pkt_type_t PKT_TYPE_MIRROR_I2E = 2;
const pkt_type_t PKT_TYPE_MIRROR_E2E = 3;

#ifdef USE_STAGE
#define STAGE(n) @stage(n)
#define STAGE_BF_1 3
#define STAGE_BF_2 4
#define STAGE_BF_3 5
#define STAGE_BF_4 6
#define STAGE_CBF_1 3
#define STAGE_CBF_2 4
#define STAGE_CBF_3 5
#define STAGE_CBF_4 6
#define STAGE_SIGNATIRE 7
#define STAGE_FILTER_1 8
#define STAGE_FILTER_2 9
#else
#define STAGE(n)
#endif

#include "headers.p4"

struct digest_migration_pair_t {
    bit<8> op;
    ipv4_addr_t srcAddr;
    bit<16> srcPort;
    ipv4_addr_t dstAddr;
    bit<16> dstPort;
    bit<8> dict_size_exp;
    bit<8> dict_remain_exp;
}

struct digest_req_t {
    bit<8> op;
    ipv4_addr_t ipAddr;
    bit<16> dbPort;
    bit<16> UDP_dst_port;
}

struct mg_dst_pair {
    mac_addr_t eth_addr;
    ipv4_addr_t ip_addr;
    bit<16> db_port;
}

struct mg_reply_pair {
    bit<32> signature; // signature[31:16] = fid, signature[15:0] = seq[15:0]
    bit<32> ts; // timestamp
}

struct hash_value_t {
    bit<_MG_BLOOM_FILTER_WIDTH> bf_1;
    bit<_MG_BLOOM_FILTER_WIDTH> bf_2;
    bit<_MG_BLOOM_FILTER_WIDTH> bf_3;
    bit<_MG_BLOOM_FILTER_WIDTH> bf_4;
    bit<_MG_COUNTING_BLOOM_FILTER_WIDTH> cbf_1;
    bit<_MG_COUNTING_BLOOM_FILTER_WIDTH> cbf_2;
    bit<_MG_COUNTING_BLOOM_FILTER_WIDTH> cbf_3;
    bit<_MG_COUNTING_BLOOM_FILTER_WIDTH> cbf_4;
    bit<_MG_REPLY_FILTER_WIDTH> rf_1;
    bit<_MG_REPLY_FILTER_WIDTH> rf_2;
    bit<32> rf_signature;
}

struct mg_metadata_t {
    mg_dst_pair dst_pair;
    bit<32> bucket_id_s1; 
    ipv4_addr_t bucket_id_addr;
    bit<16> bucket_id_port;
    hash_value_t hash_values;
    bit<8> dict_size_exp;
    bit<8> dict_remain_exp;
    bit<1> bf_1;
    bit<1> bf_2;
    bit<1> bf_3;
    bit<1> bf_4;
    int<8> cbf_1;
    int<8> cbf_2;
    int<8> cbf_3;
    int<8> cbf_4;
    bit<32> rf_set_success;
    bit<32> rf_match_success;
    bit<1> cbf_false_positive;
    bit<8> redirect_to_dst;
}


struct mg_ingress_metadata_t {
    MirrorId_t igr_mir_ses;
    pkt_type_t pkt_type;
    mac_addr_t eth_dst_addr;
    ipv4_addr_t ip_dst_addr;
    bit<16> db_dst_port;
    // bit<16> udp_checksum_tmp;
}

struct migration_pair {
    ipv4_addr_t srcAddr;
    ipv4_addr_t dstAddr;
}

struct mg_egress_metadata_t {   
    MirrorId_t egr_mir_ses;
    pkt_type_t pkt_type;
    bit<8> redirect_to_dst;
    mac_addr_t eth_dst_addr;
    ipv4_addr_t ip_dst_addr;
    bit<16> db_dst_port;
    // bit<16> udp_checksum_tmp;
}

#include "parsers.p4"
#include "routing.p4"

control KVMigrationIngress(
        inout migration_header_t hdr, 
        inout mg_ingress_metadata_t mg_ig_md,
        in ingress_intrinsic_metadata_t ig_intr_md,
        in ingress_intrinsic_metadata_from_parser_t ig_prsr_md,
        inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
        inout ingress_intrinsic_metadata_for_tm_t ig_intr_md_for_tm) {

    mg_metadata_t mg_md;

    IngressRouting() ingress_routing;

    CRCPolynomial<bit<64>>(64w0xad93d23594c935a9,
                           true,
                           false,
                           false,
                           64w0x0000000000000000,
                           64w0x0000000000000000
                           ) poly;
    Hash<bit<64>>(HashAlgorithm_t.CUSTOM, poly) bucket_id_hash;

    // caculate bucket id
    action get_bucket_id_s1() {
        mg_md.bucket_id_s1 = bucket_id_hash.get(hdr.mg_key_s1.key)[31:0];
    }

    action read_bucket_id() {
        mg_md.bucket_id_s1 = hdr.mg_group_id.group_id;
    }

    action nop() {
        ;
    }

    table mg_get_bucket_id {
        key = { hdr.mg_hdr.op : exact; }
        actions = { get_bucket_id_s1; read_bucket_id; nop;}
        size = 9;
        const entries = {
            (_MG_MIGRATE_GROUP_START) : read_bucket_id;
            (_MG_MIGRATE_GROUP_COMPLETE) : read_bucket_id;
            (_MG_READ) : get_bucket_id_s1;
            (_MG_WRITE) : get_bucket_id_s1;
            (_MG_DELETE) : get_bucket_id_s1;
            (_MG_MULTI_READ) : get_bucket_id_s1;
            (_MG_MULTI_WRITE) : get_bucket_id_s1;
            (_MG_MULTI_DELETE) : get_bucket_id_s1;
        }
        const default_action = nop;
    }

    // caculate bucket id
    action set_bucket_id_addr_port_1() {
        mg_md.bucket_id_addr = mg_md.dst_pair.ip_addr;
        mg_md.bucket_id_port = mg_md.dst_pair.db_port;
    }

    action set_bucket_id_addr_port_2() {
        mg_md.bucket_id_addr = hdr.ipv4.dstAddr;
        mg_md.bucket_id_port = hdr.mg_hdr.dbPort;
    }

    table mg_set_bucket_id_addr_port {
        key = { hdr.mg_hdr.op : exact; }
        actions = { set_bucket_id_addr_port_1; set_bucket_id_addr_port_2; nop;}
        size = 9;
        const entries = {
            (_MG_MIGRATE_GROUP_START) : set_bucket_id_addr_port_2;
            (_MG_MIGRATE_GROUP_COMPLETE) : set_bucket_id_addr_port_2;
            (_MG_READ) : set_bucket_id_addr_port_1;
            (_MG_WRITE) : set_bucket_id_addr_port_1;
            (_MG_DELETE) : set_bucket_id_addr_port_1;
            (_MG_MULTI_READ) : set_bucket_id_addr_port_1;
            (_MG_MULTI_WRITE) : set_bucket_id_addr_port_1;
            (_MG_MULTI_DELETE) : set_bucket_id_addr_port_1;
        }
        const default_action = nop;
    }

    action update_dict_mask_1() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x1;
    }
    action update_dict_mask_2() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x3;
    }

    action update_dict_mask_3() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x7;
    }

    action update_dict_mask_4() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0xF;
    }

    action update_dict_mask_5() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x1F;
    }

    action update_dict_mask_6() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x3F;
    }

    action update_dict_mask_7() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x7F;
    }

    action update_dict_mask_8() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0xFF;
    }

    action update_dict_mask_9() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x1FF;
    }

    action update_dict_mask_10() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x3FF;
    }

    action update_dict_mask_11() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x7FF;
    }

    action update_dict_mask_12() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0xFFF;
    }

    action update_dict_mask_13() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x1FFF;
    }

    action update_dict_mask_14() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x3FFF;
    }

    action update_dict_mask_15() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x7FFF;
    }
    action update_dict_mask_16() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0xFFFF;
    }

    action update_dict_mask_17() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x1FFFF;
    }

    action update_dict_mask_18() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x3FFFF;
    }

    action update_dict_mask_19() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x7FFFF;
    }

    action update_dict_mask_20() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0xFFFFF;
    }

    action update_dict_mask_21() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x1FFFFF;
    }

    action update_dict_mask_22() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x3FFFFF;
    }

    action update_dict_mask_23() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x7FFFFF;
    }

    action update_dict_mask_24() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0xFFFFFF;
    }

    action update_dict_mask_25() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x1FFFFFF;
    }

    action update_dict_mask_26() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x3FFFFFF;
    }

    action update_dict_mask_27() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x7FFFFFF;
    }

    action update_dict_mask_28() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0xFFFFFFF;
    }

    action update_dict_mask_29() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x1FFFFFFF;
    }

    action update_dict_mask_30() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x3FFFFFFF;
    }

    action update_dict_mask_31() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0x7FFFFFFF;
    }

    action update_dict_mask_32() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 & 0xFFFFFFFF;
    }


    table mg_update_dict_mask {
        key = { mg_md.dict_size_exp : exact; }
        actions = { update_dict_mask_1; 
                    update_dict_mask_2;
                    update_dict_mask_3;
                    update_dict_mask_4;
                    update_dict_mask_5;
                    update_dict_mask_6;
                    update_dict_mask_7;
                    update_dict_mask_8;
                    update_dict_mask_9;
                    update_dict_mask_10;
                    update_dict_mask_11;
                    update_dict_mask_12;
                    update_dict_mask_13; 
                    update_dict_mask_14;
                    update_dict_mask_15;
                    update_dict_mask_16;
                    update_dict_mask_17;
                    update_dict_mask_18;
                    update_dict_mask_19;
                    update_dict_mask_20;
                    update_dict_mask_21;
                    update_dict_mask_22;
                    update_dict_mask_23;
                    update_dict_mask_24;
                    update_dict_mask_25; 
                    update_dict_mask_26;
                    update_dict_mask_27;
                    update_dict_mask_28;
                    update_dict_mask_29;
                    update_dict_mask_30;
                    update_dict_mask_31;
                    update_dict_mask_32;
                    nop; }
        size = 33;
        const entries = {
            (8w1) : update_dict_mask_1;
            (8w2) : update_dict_mask_2;
            (8w3) : update_dict_mask_3;
            (8w4) : update_dict_mask_4;
            (8w5) : update_dict_mask_5;
            (8w6) : update_dict_mask_6;
            (8w7) : update_dict_mask_7;
            (8w8) : update_dict_mask_8;
            (8w9) : update_dict_mask_9;
            (8w10) : update_dict_mask_10;
            (8w11) : update_dict_mask_11;
            (8w12) : update_dict_mask_12;
            (8w13) : update_dict_mask_13;
            (8w14) : update_dict_mask_14;
            (8w15) : update_dict_mask_15;
            (8w16) : update_dict_mask_16;
            (8w17) : update_dict_mask_17;
            (8w18) : update_dict_mask_18;
            (8w19) : update_dict_mask_19;
            (8w20) : update_dict_mask_20;
            (8w21) : update_dict_mask_21;
            (8w22) : update_dict_mask_22;
            (8w23) : update_dict_mask_23;
            (8w24) : update_dict_mask_24;
            (8w25) : update_dict_mask_25;
            (8w26) : update_dict_mask_26;
            (8w27) : update_dict_mask_27;
            (8w28) : update_dict_mask_28;
            (8w29) : update_dict_mask_29;
            (8w30) : update_dict_mask_30;
            (8w31) : update_dict_mask_31;
            (8w32) : update_dict_mask_32;
        }   
        const default_action = nop;
    }

    action update_group_id_1() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 1;
    }
    action update_group_id_2() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 2;
    }

    action update_group_id_3() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 3;
    }

    action update_group_id_4() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 4;
    }

    action update_group_id_5() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 5;
    }

    action update_group_id_6() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 6;
    }

    action update_group_id_7() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 7;
    }

    action update_group_id_8() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 8;
    }

    action update_group_id_9() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 9;
    }

    action update_group_id_10() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 10;
    }

    action update_group_id_11() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 11;
    }

    action update_group_id_12() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 12;
    }

    action update_group_id_13() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 13;
    }

    action update_group_id_14() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 14;
    }

    action update_group_id_15() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 15;
    }
    action update_group_id_16() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 16;
    }

    action update_group_id_17() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 17;
    }

    action update_group_id_18() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 18;
    }

    action update_group_id_19() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 19;
    }

    action update_group_id_20() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 20;
    }

    action update_group_id_21() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 21;
    }

    action update_group_id_22() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 22;
    }

    action update_group_id_23() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 23;
    }

    action update_group_id_24() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 24;
    }

    action update_group_id_25() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 25;
    }

    action update_group_id_26() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 26;
    }

    action update_group_id_27() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 27;
    }

    action update_group_id_28() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 28;
    }

    action update_group_id_29() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 29;
    }

    action update_group_id_30() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 30;
    }

    action update_group_id_31() {
        mg_md.bucket_id_s1 = mg_md.bucket_id_s1 >> 31;
    }

    table mg_update_dict_group_id {
        key = { mg_md.dict_remain_exp : exact;}
        actions = { update_group_id_1; 
                    update_group_id_2;
                    update_group_id_3;
                    update_group_id_4;
                    update_group_id_5;
                    update_group_id_6;
                    update_group_id_7;
                    update_group_id_8;
                    update_group_id_9;
                    update_group_id_10;
                    update_group_id_11;
                    update_group_id_12;
                    update_group_id_13; 
                    update_group_id_14;
                    update_group_id_15;
                    update_group_id_16;
                    update_group_id_17;
                    update_group_id_18;
                    update_group_id_19;
                    update_group_id_20;
                    update_group_id_21;
                    update_group_id_22;
                    update_group_id_23;
                    update_group_id_24;
                    update_group_id_25; 
                    update_group_id_26;
                    update_group_id_27;
                    update_group_id_28;
                    update_group_id_29;
                    update_group_id_30;
                    update_group_id_31;
                    nop; }
        size = 32;
        const entries = {
            (8w1) : update_group_id_1;
            (8w2) : update_group_id_2;
            (8w3) : update_group_id_3;
            (8w4) : update_group_id_4;
            (8w5) : update_group_id_5;
            (8w6) : update_group_id_6;
            (8w7) : update_group_id_7;
            (8w8) : update_group_id_8;
            (8w9) : update_group_id_9;
            (8w10) : update_group_id_10;
            (8w11) : update_group_id_11;
            (8w12) : update_group_id_12;
            (8w13) : update_group_id_13;
            (8w14) : update_group_id_14;
            (8w15) : update_group_id_15;
            (8w16) : update_group_id_16;
            (8w17) : update_group_id_17;
            (8w18) : update_group_id_18;
            (8w19) : update_group_id_19;
            (8w20) : update_group_id_20;
            (8w21) : update_group_id_21;
            (8w22) : update_group_id_22;
            (8w23) : update_group_id_23;
            (8w24) : update_group_id_24;
            (8w25) : update_group_id_25;
            (8w26) : update_group_id_26;
            (8w27) : update_group_id_27;
            (8w28) : update_group_id_28;
            (8w29) : update_group_id_29;
            (8w30) : update_group_id_30;
            (8w31) : update_group_id_31;
        }   
        const default_action = nop;  
    } 

    CRCPolynomial<bit<32>>(32w0x1EDC6F41,
                           true,
                           true,
                           false,
                           32w0x00000000,
                           32w0xFFFFFFFF
                           ) poly1; // crc_32c

    CRCPolynomial<bit<32>>(32w0x04C11DB7,
                           true,
                           true,
                           false,
                           32w0x00000000,
                           32w0xFFFFFFFF
                           ) poly2; // crc32
    
    CRCPolynomial<bit<32>>(32w0x04C11DB7,
                           false,
                           true,
                           false,
                           32w0x00000000,
                           32w0xFFFFFFFF
                           ) poly3; // crc_32_bzip2
    
    CRCPolynomial<bit<32>>(32w0x04C11DB7,
                           false,
                           true,
                           false,
                           32w0xFFFFFFFF,
                           32w0x00000000
                           ) poly4; // crc_32_mpeg

    Hash<bit<_MG_BLOOM_FILTER_WIDTH>>(HashAlgorithm_t.CUSTOM, poly1) bf_hash_1;
    Hash<bit<_MG_BLOOM_FILTER_WIDTH>>(HashAlgorithm_t.CUSTOM, poly2) bf_hash_2;
    Hash<bit<_MG_BLOOM_FILTER_WIDTH>>(HashAlgorithm_t.CUSTOM, poly3) bf_hash_3;
    Hash<bit<_MG_BLOOM_FILTER_WIDTH>>(HashAlgorithm_t.CUSTOM, poly4) bf_hash_4;

    Hash<bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>>(HashAlgorithm_t.CUSTOM, poly1) cbf_hash_1;
    Hash<bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>>(HashAlgorithm_t.CUSTOM, poly2) cbf_hash_2;
    Hash<bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>>(HashAlgorithm_t.CUSTOM, poly3) cbf_hash_3;
    Hash<bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>>(HashAlgorithm_t.CUSTOM, poly4) cbf_hash_4;

    Hash<bit<16>>(HashAlgorithm_t.CUSTOM, poly1) rf_hash_1;
    Hash<bit<16>>(HashAlgorithm_t.CUSTOM, poly2) rf_hash_2;

    action get_hash_bf_1() {
        mg_md.hash_values.bf_1 = bf_hash_1.get({mg_md.bucket_id_addr, mg_md.bucket_id_port, mg_md.bucket_id_s1});
        mg_md.hash_values.cbf_1 = mg_md.hash_values.bf_1[17:0];
    }

    action get_hash_bf_2() {
        mg_md.hash_values.bf_2 = bf_hash_2.get({mg_md.bucket_id_addr, mg_md.bucket_id_port, mg_md.bucket_id_s1});
        mg_md.hash_values.cbf_2 = mg_md.hash_values.bf_2[17:0];
    }

    action get_hash_bf_3() {
        mg_md.hash_values.bf_3 = bf_hash_3.get({mg_md.bucket_id_addr, mg_md.bucket_id_port, mg_md.bucket_id_s1});
        mg_md.hash_values.cbf_3 = mg_md.hash_values.bf_3[17:0];
    }

    action get_hash_bf_4() {
        mg_md.hash_values.bf_4 = bf_hash_4.get({mg_md.bucket_id_addr, mg_md.bucket_id_port, mg_md.bucket_id_s1});
        mg_md.hash_values.cbf_4 = mg_md.hash_values.bf_4[17:0];
    }

    action get_hash_rf_1() {
        mg_md.hash_values.rf_1[15:0] = rf_hash_1.get({
            4w0,
            hdr.ipv4.dstAddr, hdr.mg_hdr.dbPort,
            hdr.mg_hdr.seq,
            4w0});
        mg_md.hash_values.rf_1[31:16] = 0;
    }

    action get_hash_rf_2() {
        mg_md.hash_values.rf_2[15:0] = rf_hash_2.get({
            2w1,
            hdr.ipv4.dstAddr, hdr.mg_hdr.dbPort,
            2w1,
            hdr.mg_hdr.seq,
            2w1});
        mg_md.hash_values.rf_1[31:16] = 0;
    }

    Hash<bit<32>>(HashAlgorithm_t.CRC32) crc32_sig;
        
    action get_pkt_signature(){
        mg_md.hash_values.rf_signature=crc32_sig.get({hdr.mg_hdr.seq, hdr.ipv4.dstAddr, hdr.udp.dstPort});
    }


    // Migration pair table

    action read_mg_dst_addr(mac_addr_t eth_dstAddr, ipv4_addr_t ip_dstAddr, bit<16> db_dstPort, bit<8> size_exp, bit<8> remain_exp) {
        mg_md.dst_pair.eth_addr = eth_dstAddr;
        mg_md.dst_pair.ip_addr = ip_dstAddr;
        mg_md.dst_pair.db_port = db_dstPort;
        mg_md.dict_size_exp = size_exp;
        mg_md.dict_remain_exp = remain_exp;
    }

    action set_default_dst_addr() {
        mg_md.dst_pair.eth_addr = 0xFFFFFFFFFFFF;
        mg_md.dst_pair.ip_addr = 0xFFFFFFFF;
        mg_md.dst_pair.db_port = 0xFFFF;
        mg_md.dict_size_exp = 0;
        mg_md.dict_remain_exp = 0;
    }

    table mg_migration_pair_lookup {
        key = { 
            hdr.ipv4.dstAddr : exact; 
            hdr.mg_hdr.dbPort : exact; 
        }
        actions = { read_mg_dst_addr; set_default_dst_addr; }
        default_action = set_default_dst_addr;
        size = 32;
    }

    action set_migration_pair_digest_type() {
        ig_dprsr_md.digest_type = 1;
    }

    // Membership tables (BFs and CBFs)
    Register<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>>(_MG_MAX_BLOOM_FILTER_ENTRIES, 0) mg_bloom_filter_reg_1;
    Register<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>>(_MG_MAX_BLOOM_FILTER_ENTRIES, 0) mg_bloom_filter_reg_2;
    Register<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>>(_MG_MAX_BLOOM_FILTER_ENTRIES, 0) mg_bloom_filter_reg_3;
    Register<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>>(_MG_MAX_BLOOM_FILTER_ENTRIES, 0) mg_bloom_filter_reg_4;

    RegisterAction<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>, bit<1>>(mg_bloom_filter_reg_1) bloom_filter_read_reg_action_1 = {
        void apply(inout bit<1> value, out bit<1> read_value) {
            read_value = value;
        }
    };
  
    RegisterAction<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>, bit<1>>(mg_bloom_filter_reg_2) bloom_filter_read_reg_action_2 = {
        void apply(inout bit<1> value, out bit<1> read_value) {
            read_value = value;
        }
    };

    RegisterAction<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>, bit<1>>(mg_bloom_filter_reg_3) bloom_filter_read_reg_action_3 = {
        void apply(inout bit<1> value, out bit<1> read_value) {
            read_value = value;
        }
    };

    RegisterAction<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>, bit<1>>(mg_bloom_filter_reg_4) bloom_filter_read_reg_action_4 = {
        void apply(inout bit<1> value, out bit<1> read_value) {
            read_value = value;
        }
    };

    RegisterAction<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>, bit<1>>(mg_bloom_filter_reg_1) bloom_filter_write_reg_action_1 = {
        void apply(inout bit<1> value, out bit<1> read_value) {
            value = 1;
            read_value = value;
        }
    };
  
    RegisterAction<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>, bit<1>>(mg_bloom_filter_reg_2) bloom_filter_write_reg_action_2 = {
        void apply(inout bit<1> value, out bit<1> read_value) {
            value = 1;
            read_value = value;
        }
    };

    RegisterAction<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>, bit<1>>(mg_bloom_filter_reg_3) bloom_filter_write_reg_action_3 = {
        void apply(inout bit<1> value, out bit<1> read_value) {
            value = 1;
            read_value = value;
        }
    };

    RegisterAction<bit<1>, bit<_MG_BLOOM_FILTER_WIDTH>, bit<1>>(mg_bloom_filter_reg_4) bloom_filter_write_reg_action_4 = {
        void apply(inout bit<1> value, out bit<1> read_value) {
            value = 1;
            read_value = value;
        }
    };

    action bloom_filter_read_r1() {
        mg_md.bf_1 = bloom_filter_read_reg_action_1.execute(mg_md.hash_values.bf_1);
    }

    action bloom_filter_read_r2() {
        mg_md.bf_2 = bloom_filter_read_reg_action_2.execute(mg_md.hash_values.bf_2);                
    }

    action bloom_filter_read_r3() {
        mg_md.bf_3 = bloom_filter_read_reg_action_3.execute(mg_md.hash_values.bf_3);
    }

    action bloom_filter_read_r4() {
        mg_md.bf_4 = bloom_filter_read_reg_action_4.execute(mg_md.hash_values.bf_4);
    }
    
    action bloom_filter_write_r1() {
        mg_md.bf_1 = bloom_filter_write_reg_action_1.execute(mg_md.hash_values.bf_1);
    }

    action bloom_filter_write_r2() {
        mg_md.bf_2 = bloom_filter_write_reg_action_2.execute(mg_md.hash_values.bf_2);
    }

    action bloom_filter_write_r3() {
        mg_md.bf_3 = bloom_filter_write_reg_action_3.execute(mg_md.hash_values.bf_3);
    }

    action bloom_filter_write_r4() {
        mg_md.bf_4 = bloom_filter_write_reg_action_4.execute(mg_md.hash_values.bf_4);
    }

    Register<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>>(_MG_COUNTING_BLOOM_FILTER_ENTRIES, 0) mg_counting_bloom_filter_reg_1;
    Register<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>>(_MG_COUNTING_BLOOM_FILTER_ENTRIES, 0) mg_counting_bloom_filter_reg_2;
    Register<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>>(_MG_COUNTING_BLOOM_FILTER_ENTRIES, 0) mg_counting_bloom_filter_reg_3;
    Register<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>>(_MG_COUNTING_BLOOM_FILTER_ENTRIES, 0) mg_counting_bloom_filter_reg_4;

    RegisterAction<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>, int<8>>(mg_counting_bloom_filter_reg_1) counting_bloom_filter_write_reg_action_1 = {
        void apply(inout int<8> value, out int<8> read_value) {
            if (hdr.mg_hdr.op == _MG_MIGRATE_GROUP_START)
                value = value + 1;
            else 
                value = value - 1;
            read_value = value;
        }
    };

    RegisterAction<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>, int<8>>(mg_counting_bloom_filter_reg_2) counting_bloom_filter_write_reg_action_2 = {
        void apply(inout int<8> value, out int<8> read_value) {
            if (hdr.mg_hdr.op == _MG_MIGRATE_GROUP_START)
                value = value + 1;
            else 
                value = value - 1;
            read_value = value;
        }
    };

    RegisterAction<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>, int<8>>(mg_counting_bloom_filter_reg_3) counting_bloom_filter_write_reg_action_3 = {
        void apply(inout int<8> value, out int<8> read_value) {
            if (hdr.mg_hdr.op == _MG_MIGRATE_GROUP_START)
                value = value + 1;
            else 
                value = value - 1;
            read_value = value;
        }
    };

    RegisterAction<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>, int<8>>(mg_counting_bloom_filter_reg_4) counting_bloom_filter_write_reg_action_4 = {
        void apply(inout int<8> value, out int<8> read_value) {
            if (hdr.mg_hdr.op == _MG_MIGRATE_GROUP_START)
                value = value + 1;
            else 
                value = value - 1;
            read_value = value;
        }
    };

    RegisterAction<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>, int<8>>(mg_counting_bloom_filter_reg_1) counting_bloom_filter_read_reg_action_1 = {
        void apply(inout int<8> value, out int<8> read_value) {
            read_value = value;
        }
    };

    RegisterAction<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>, int<8>>(mg_counting_bloom_filter_reg_2) counting_bloom_filter_read_reg_action_2 = {
        void apply(inout int<8> value, out int<8> read_value) {
            read_value = value;
        }
    };

    RegisterAction<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>, int<8>>(mg_counting_bloom_filter_reg_3) counting_bloom_filter_read_reg_action_3 = {
        void apply(inout int<8> value, out int<8> read_value) {
            read_value = value;
        }
    };

    RegisterAction<int<8>, bit<_MG_COUNTING_BLOOM_FILTER_WIDTH>, int<8>>(mg_counting_bloom_filter_reg_4) counting_bloom_filter_read_reg_action_4 = {
        void apply(inout int<8> value, out int<8> read_value) {
            read_value = value;
        }
    };

    action counting_bloom_filter_read_r1() {
        mg_md.cbf_1 = counting_bloom_filter_read_reg_action_1.execute(mg_md.hash_values.cbf_1);
    }

    action counting_bloom_filter_read_r2() {
        mg_md.cbf_2 = counting_bloom_filter_read_reg_action_2.execute(mg_md.hash_values.cbf_2);
    }

    action counting_bloom_filter_read_r3() {
        mg_md.cbf_3 = counting_bloom_filter_read_reg_action_3.execute(mg_md.hash_values.cbf_3);
    }

    action counting_bloom_filter_read_r4() {
        mg_md.cbf_4 = counting_bloom_filter_read_reg_action_4.execute(mg_md.hash_values.cbf_4);
    }

    action counting_bloom_filter_write_r1() {
        mg_md.cbf_1 = counting_bloom_filter_write_reg_action_1.execute(mg_md.hash_values.cbf_1);
    }

    action counting_bloom_filter_write_r2() {
        mg_md.cbf_2 = counting_bloom_filter_write_reg_action_2.execute(mg_md.hash_values.cbf_2);
    }

    action counting_bloom_filter_write_r3() {
        mg_md.cbf_3 = counting_bloom_filter_write_reg_action_3.execute(mg_md.hash_values.cbf_3);
    }

    action counting_bloom_filter_write_r4() {
        mg_md.cbf_4 = counting_bloom_filter_write_reg_action_4.execute(mg_md.hash_values.cbf_4);
    }

    // reply filtering registers and actions
    #define TIMESTAMP ig_intr_md.ingress_mac_tstamp[31:0]
    #define TS_EXPIRE_THRESHOLD (200*1000*1000) 
    #define TS_LEGITIMATE_THRESHOLD (200*1000*1000) 

    #define current_entry_matched (in_value.signature==mg_md.hash_values.rf_signature)  
    #define timestamp_legitimate  ((TIMESTAMP-in_value.ts)<TS_LEGITIMATE_THRESHOLD)

    Register<mg_reply_pair, bit<_MG_REPLY_FILTER_WIDTH>>(_MG_REPLY_FILTER_ENTRIES) mg_reply_filter_reg_1;
    Register<mg_reply_pair, bit<_MG_REPLY_FILTER_WIDTH>>(_MG_REPLY_FILTER_ENTRIES) mg_reply_filter_reg_2;

    RegisterAction<mg_reply_pair, bit<_MG_REPLY_FILTER_WIDTH>, bit<32>>(mg_reply_filter_reg_1) set_reply_filter_reg_action_1 = {
        void apply(inout mg_reply_pair value, out bit<32> set_success) {
            set_success = 0;
            mg_reply_pair in_value;
            in_value = value;

            bool existing_timestamp_is_old = ((TIMESTAMP-in_value.ts)>TS_EXPIRE_THRESHOLD);
            bool current_entry_empty = (in_value.signature==0);
            
            if(existing_timestamp_is_old || current_entry_empty) {
                value.signature = mg_md.hash_values.rf_signature;
                value.ts = TIMESTAMP;
                set_success = 1;
            }
        }
    };

    RegisterAction<mg_reply_pair, bit<_MG_REPLY_FILTER_WIDTH>, bit<32>>(mg_reply_filter_reg_2) set_reply_filter_reg_action_2 = {
        void apply(inout mg_reply_pair value, out bit<32> set_success) {
            set_success = 0;
            mg_reply_pair in_value;
            in_value = value;

            bool existing_timestamp_is_old = ((TIMESTAMP-in_value.ts)>TS_EXPIRE_THRESHOLD);
            bool current_entry_empty = (in_value.signature==0);

            if(existing_timestamp_is_old || current_entry_empty) {
                value.signature = mg_md.hash_values.rf_signature;
                value.ts = TIMESTAMP;
                set_success = 1;
            }
        }
    };


    RegisterAction<mg_reply_pair, bit<_MG_REPLY_FILTER_WIDTH>, bit<32>>(mg_reply_filter_reg_1) read_match_reply_filter_reg_action_1 = {
        void apply(inout mg_reply_pair value, out bit<32> match_success) {
            match_success = 0;
            mg_reply_pair in_value;
            in_value = value;
            if (timestamp_legitimate && current_entry_matched) {
                value.signature = 0;
                value.ts = 0;
                match_success = 1;
            }
        }
    };

    RegisterAction<mg_reply_pair, bit<_MG_REPLY_FILTER_WIDTH>, bit<32>>(mg_reply_filter_reg_2) read_match_reply_filter_reg_action_2 = {
        void apply(inout mg_reply_pair value, out bit<32> match_success) {
            match_success = 0;
            mg_reply_pair in_value;
            in_value = value;
            if (timestamp_legitimate && current_entry_matched) {
                value.signature = 0;
                value.ts = 0;
                match_success = 1;
            }
        }
    };

    /*
    action reply_filter_set_1() {
        mg_md.rf_set_success = set_reply_filter_reg_action_1.execute(mg_md.hash_values.rf_1);
    }

    action reply_filter_set_2() {
        mg_md.rf_set_success = set_reply_filter_reg_action_2.execute(mg_md.hash_values.rf_2);
    }
    
    action reply_filter_read_match_1() {
        mg_md.rf_match_success = read_match_reply_filter_reg_action_1.execute(mg_md.hash_values.rf_1);
    }

    action reply_filter_read_match_2() {
        mg_md.rf_match_success = read_match_reply_filter_reg_action_2.execute(mg_md.hash_values.rf_2);
    }
    */
   
    apply {
        // if (hdr.mg_hdr.isValid()) {
            // if (hdr.udp.isValid()) 
                hdr.udp.checksum = 0; // not use udp checksum under ipv4
            mg_migration_pair_lookup.apply();
            mg_get_bucket_id.apply();
            mg_set_bucket_id_addr_port.apply();
            if (hdr.mg_hdr.op == _MG_READ || hdr.mg_hdr.op == _MG_WRITE || hdr.mg_hdr.op == _MG_DELETE \
                || hdr.mg_hdr.op == _MG_MULTI_READ || hdr.mg_hdr.op == _MG_MULTI_WRITE || hdr.mg_hdr.op == _MG_MULTI_DELETE) {
                mg_update_dict_mask.apply();
                mg_update_dict_group_id.apply();
            }
            get_hash_bf_1();
            get_hash_bf_2();
            get_hash_bf_3();
            get_hash_bf_4();
        // }

        // Migration Control
        if (hdr.mg_hdr.op == _MG_INIT || hdr.mg_hdr.op == _MG_TERMINATE) {
            // Controller set or delete migration_pair entry
            set_migration_pair_digest_type();
            // reply packets will be generated by destination; destination proxy also holds migration pair table
        }
        else if ( hdr.mg_hdr.op == _MG_MIGRATE_GROUP_START || hdr.mg_hdr.op == _MG_MIGRATE_GROUP_COMPLETE) { 
            // Migration a group start or complete: update membership in cbf; foward to source server
            STAGE(STAGE_CBF_1) {
                counting_bloom_filter_write_r1();
            }
            STAGE(STAGE_CBF_2) {
                counting_bloom_filter_write_r2();
            }
            STAGE(STAGE_CBF_3) {
                counting_bloom_filter_write_r3();
            }
            STAGE(STAGE_CBF_4) {
                counting_bloom_filter_write_r4();
            }
            if (hdr.mg_hdr.op == _MG_MIGRATE_GROUP_COMPLETE) {
                STAGE(STAGE_BF_1) {
                    bloom_filter_write_r1();
                }
                STAGE(STAGE_BF_2) {
                    bloom_filter_write_r2();
                }
                STAGE(STAGE_BF_3) {
                    bloom_filter_write_r3();
                }
                STAGE(STAGE_BF_4) {
                    bloom_filter_write_r4();
                }
            }
        }
        else if (hdr.mg_hdr.op == _MG_READ || hdr.mg_hdr.op == _MG_WRITE || hdr.mg_hdr.op == _MG_DELETE || \
                 hdr.mg_hdr.op == _MG_MULTI_READ || hdr.mg_hdr.op == _MG_MULTI_WRITE || hdr.mg_hdr.op == _MG_MULTI_DELETE) {
            // Look up membership tables
            STAGE(STAGE_BF_1) {
                bloom_filter_read_r1();
            }
            STAGE(STAGE_BF_2) {
                bloom_filter_read_r2();
            }
            STAGE(STAGE_BF_3) {
                bloom_filter_read_r3();
            }
            STAGE(STAGE_BF_4) {
                bloom_filter_read_r4();
            }
            STAGE(STAGE_CBF_1) {
                counting_bloom_filter_read_r1();
            }
            STAGE(STAGE_CBF_2) {
                counting_bloom_filter_read_r2();
            }
            STAGE(STAGE_CBF_3) {
                counting_bloom_filter_read_r3();
            }
            STAGE(STAGE_CBF_4) {
                counting_bloom_filter_read_r4();
            }
        }
	/*
        else if (hdr.mg_hdr.op == _MG_READ_REPLY) { // control for double read replies
            get_pkt_signature();
            get_hash_rf_1();
            get_hash_rf_2();
            mg_md.rf_match_success = 0;
            if (hdr.mg_ver_s1.ver == 0x7) { // double read reply, destination success
                STAGE(STAGE_FILTER_1) {
                    reply_filter_set_1();
                }
                
                if (mg_md.rf_set_success == 0) {
                    STAGE(STAGE_FILTER_2) {
                        reply_filter_set_2();
                    }
                } 
            }
            else if (hdr.mg_ver_s1.ver & 0x4 > 0) { // double read reply from source, potentially can be dropped
                STAGE(STAGE_FILTER_1) {
                    reply_filter_read_match_1();
                }
                if (mg_md.rf_match_success == 0) {
                    STAGE(STAGE_FILTER_2) {
                        reply_filter_read_match_2();
                    }
                }
            }
        }
	*/

        ingress_routing.apply(hdr, ig_intr_md_for_tm, ig_dprsr_md, mg_md, mg_ig_md);
    }
}



control KVMigrationEgress(
        inout migration_header_t hdr,
        inout mg_egress_metadata_t mg_eg_md,
        in egress_intrinsic_metadata_t eg_intr_md,
        in egress_intrinsic_metadata_from_parser_t eg_intr_from_prsr,
        inout egress_intrinsic_metadata_for_deparser_t eg_dprsr_md,
        inout egress_intrinsic_metadata_for_output_port_t eg_intr_md_for_oport) {
        
    action udpate_dst_header() {
        hdr.ethernet.dstAddr = hdr.mirror_I2E_md.eth_dst_addr;
        hdr.ipv4.dstAddr = hdr.mirror_I2E_md.ip_dst_addr;
        hdr.mg_hdr.dbPort = hdr.mirror_I2E_md.db_dst_port;
    }

    action set_recirculate_and_E2E_mirror(MirrorId_t egr_ses) {
        mg_eg_md.egr_mir_ses = egr_ses; // set to recirculation port (68)
        mg_eg_md.pkt_type = PKT_TYPE_MIRROR_E2E;
        eg_dprsr_md.mirror_type = MIRROR_TYPE_E2E;
        // set original addr and port
        mg_eg_md.redirect_to_dst = hdr.bridged_md.redirect_to_dst;
        mg_eg_md.eth_dst_addr = hdr.ethernet.dstAddr;
        mg_eg_md.ip_dst_addr = hdr.ipv4.dstAddr;
        mg_eg_md.db_dst_port = hdr.mg_hdr.dbPort;
    }

    action nop() {
        ;
    }

    table mg_recirculate_and_E2E_mirror {
        key = { hdr.bridged_md.do_egr_mirroring : exact; }
        actions = { set_recirculate_and_E2E_mirror; nop; }
        size = 2;
        default_action = nop;
    }

    action update_dst_header_redirect() {
        hdr.ethernet.dstAddr = hdr.bridged_md.eth_dst_addr;
        hdr.ipv4.dstAddr = hdr.bridged_md.ip_dst_addr;
        hdr.mg_hdr.dbPort = hdr.bridged_md.db_dst_port;
    }

    action copy_ver_key_s2() {
        hdr.mg_ver_s1.ver = hdr.mg_ver_s2.ver;
        hdr.mg_key_s1.key = hdr.mg_key_s2.key;
    }

    action copy_ver_key_s3() {
        hdr.mg_ver_s1.ver = hdr.mg_ver_s3.ver;
        hdr.mg_key_s1.key = hdr.mg_key_s3.key;
    }

    action copy_ver_key_s4() {
        hdr.mg_ver_s1.ver = hdr.mg_ver_s4.ver;
        hdr.mg_key_s1.key = hdr.mg_key_s4.key;
    }

    action copy_key_s2() {
        hdr.mg_key_s1.key = hdr.mg_key_s2.key;
    }

    action copy_key_s3() {
        hdr.mg_key_s1.key = hdr.mg_key_s3.key;
    }

    action copy_key_s4() {
        hdr.mg_key_s1.key = hdr.mg_key_s4.key;
    }

    table update_ver_key_s1 {
        key = { hdr.mg_bitmap_hdr.t : ternary; 
                hdr.mg_hdr.op : exact; }
        actions = { copy_ver_key_s2;
                    copy_ver_key_s3;
                    copy_ver_key_s4;
                    copy_key_s2;
                    copy_key_s3;
                    copy_key_s4;
                    @defaultonly nop; }
        size = 10;
        const entries = {
            (0x2 &&& 0x3, _MG_MULTI_READ) : copy_ver_key_s2;
            (0x4 &&& 0x7, _MG_MULTI_READ) : copy_ver_key_s3;
            (0x8 &&& 0xF, _MG_MULTI_READ) : copy_ver_key_s4;
            (0x2 &&& 0x3, _MG_MULTI_WRITE) : copy_key_s2;
            (0x4 &&& 0x7, _MG_MULTI_WRITE) : copy_key_s3;
            (0x8 &&& 0xF, _MG_MULTI_WRITE) : copy_key_s4;
            (0x2 &&& 0x3, _MG_MULTI_DELETE) : copy_key_s2;
            (0x4 &&& 0x7, _MG_MULTI_DELETE) : copy_key_s3;
            (0x8 &&& 0xF, _MG_MULTI_DELETE) : copy_key_s4;
        }
        const default_action = nop;
    }

    action update_dst_header_back() {
        hdr.ethernet.dstAddr = hdr.mirror_E2E_md.eth_dst_addr;
        hdr.ipv4.dstAddr = hdr.mirror_E2E_md.ip_dst_addr;
        hdr.mg_hdr.dbPort = hdr.mirror_E2E_md.db_dst_port;
    }
    
    apply {
        if (hdr.udp.isValid()) 
            hdr.udp.checksum = 0;
            
        if (hdr.mirror_I2E_md.isValid() && hdr.mirror_I2E_md.pkt_type == PKT_TYPE_MIRROR_I2E) {
            udpate_dst_header();
        }
        else if (hdr.mirror_E2E_md.isValid() && hdr.mirror_E2E_md.pkt_type == PKT_TYPE_MIRROR_E2E) {
            // update key, val to s1 field
            update_ver_key_s1.apply();
            if (hdr.mirror_E2E_md.redirect_to_dst == 1) {
                update_dst_header_back();
            }
        }
        else if (hdr.bridged_md.isValid()) {
            mg_recirculate_and_E2E_mirror.apply();
            if (hdr.bridged_md.redirect_to_dst == 1) {
                update_dst_header_redirect();
            }
        }
    }
}

Pipeline(KVMigrationIngressParser(),
         KVMigrationIngress(),
         KVMigrationIngressDeparser(),
         KVMigrationEgressParser(),
         KVMigrationEgress(),
         KVMigrationEgressDeparser()) pipe;

Switch(pipe) main;
