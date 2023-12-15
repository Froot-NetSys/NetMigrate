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

parser TofinoEgressParser(
        packet_in pkt,
        out egress_intrinsic_metadata_t eg_intr_md) {
    state start {
        pkt.extract(eg_intr_md);
        transition accept;
    }
}


// ---------------------------------------------------------------------------
// Ingress parser
// ---------------------------------------------------------------------------
parser KVMigrationIngressParser(
        packet_in pkt,
        out migration_header_t hdr,
        out mg_ingress_metadata_t mg_ig_md,
        out ingress_intrinsic_metadata_t ig_intr_md
        ) {

    Checksum() ipv4_checksum;
    // Checksum() udp_checksum;

    state start {
        pkt.extract(ig_intr_md);
        transition select(ig_intr_md.resubmit_flag) {
            1 : parse_resubmit;
            0 : parse_port_metadata;
        }
    }

    state parse_resubmit {
        transition accept;
    }

    state parse_port_metadata {
        pkt.advance(PORT_METADATA_SIZE);
        transition select(ig_intr_md.ingress_port) {
            68 : parse_recirculate;
            default: parse_ethernet;
        }
    }

    state parse_recirculate {
        transition parse_ethernet;
    }

    state parse_ethernet {
        pkt.extract(hdr.ethernet);
        transition select (hdr.ethernet.etherType) {
            ETHERTYPE_IPV4 : parse_ipv4;
            default: reject;
        }
    }

    state parse_ipv4 {
        pkt.extract(hdr.ipv4);
        ipv4_checksum.add(hdr.ipv4);
        // udp_checksum.subtract({hdr.ipv4.srcAddr, hdr.ipv4.dstAddr});
        transition select (hdr.ipv4.protocol) {
            IP_PROTOCOLS_TCP : parse_tcp;
            IP_PROTOCOLS_UDP : parse_udp;
            default : accept;
        }
    }

    state parse_tcp {
        pkt.extract(hdr.tcp);
        transition accept;
    }

    state parse_udp {
        pkt.extract(hdr.udp);
        // udp_checksum.subtract({hdr.udp.checksum});
        // udp_checksum.subtract({hdr.udp.srcPort, hdr.udp.dstPort});
        // udp_checksum.subtract_all_and_deposit(mg_ig_md.udp_checksum_tmp);
        transition select (hdr.udp.dstPort) {
            UDP_MG_PORT_BASE &&& UDP_MG_PORT_MASK : parse_migration;
            default: accept;
        }
    }

    state parse_migration {
        pkt.extract(hdr.mg_hdr);
        transition select (hdr.mg_hdr.op) {
            _MG_INIT : parse_migrate_init;
            _MG_MIGRATE: accept;
            _MG_TERMINATE : parse_migrate_termination;
            _MG_MIGRATE_REPLY : accept;
            _MG_INIT_REPLY : accept;
            _MG_TERMINATE_REPLY : accept;
            _MG_MIGRATE_GROUP_START: parse_mg_group;
            _MG_MIGRATE_GROUP_START_REPLY: accept;
            _MG_MIGRATE_GROUP_COMPLETE: parse_mg_group;
            _MG_MIGRATE_GROUP_COMPLETE_REPLY: accept;
            _MG_READ : parse_mg_read;
            _MG_WRITE : parse_mg_write;
            _MG_DELETE : parse_mg_delete;
            _MG_READ_REPLY : parse_mg_read_reply;
            _MG_WRITE_REPLY : accept;
            _MG_DELETE_REPLY : accept;
            _MG_MULTI_READ : parse_mg_multi_read;
            _MG_MULTI_WRITE : parse_mg_multi_write;
            _MG_MULTI_DELETE : parse_mg_multi_delete;
            _MG_MULTI_READ_REPLY : accept;
            _MG_MULTI_WRITE_REPLY : accept;
            _MG_MULTI_DELETE_REPLY : accept;
            default : reject; 
        }
    }

    state parse_mg_group {
        pkt.extract(hdr.mg_group_id);
        transition accept;
    }

    state parse_migrate_init {
        pkt.extract(hdr.mg_pair_hdr);
        pkt.extract(hdr.mg_dict_mask);
        transition accept;
    }

    state parse_migrate_termination {
        pkt.extract(hdr.mg_pair_hdr);
        pkt.extract(hdr.mg_dict_mask);
        transition accept;
    }

    state parse_mg_read {
        pkt.extract(hdr.mg_bitmap_hdr);
        pkt.extract(hdr.mg_ver_s1);
        pkt.extract(hdr.mg_key_s1);
        transition accept;
    }

    state parse_mg_write {
        pkt.extract(hdr.mg_bitmap_hdr);
        pkt.extract(hdr.mg_key_s1);
        transition accept;
    }

    state parse_mg_delete {
        pkt.extract(hdr.mg_bitmap_hdr);
        pkt.extract(hdr.mg_key_s1);
        transition accept;
    }

    state parse_mg_multi_read {
        pkt.extract(hdr.mg_bitmap_hdr);
        transition select (hdr.mg_bitmap_hdr.t) {
            0x1 &&& 0xF: parse_mg_extract_ver_key_1;
            0x2 &&& 0xE: parse_mg_extract_ver_key_2;
            0x4 &&& 0xC: parse_mg_extract_ver_key_3;
            0x8 &&& 0x8: parse_mg_extract_ver_key_4;
            default: reject;
        }
    }

    state parse_mg_multi_delete {
        pkt.extract(hdr.mg_bitmap_hdr);
        transition select (hdr.mg_bitmap_hdr.t) {
            0x1 &&& 0xF: parse_mg_extract_key_1;
            0x2 &&& 0xE: parse_mg_extract_key_2;
            0x4 &&& 0xC: parse_mg_extract_key_3;
            0x8 &&& 0x8: parse_mg_extract_key_4;
            default: reject;
        }
    }


    state parse_mg_multi_write {
        pkt.extract(hdr.mg_bitmap_hdr);
        transition select (hdr.mg_bitmap_hdr.t) {
            0x1 &&& 0xF: parse_mg_extract_key_1;
            0x2 &&& 0xE: parse_mg_extract_key_2;
            0x4 &&& 0xC: parse_mg_extract_key_3;
            0x8 &&& 0x8: parse_mg_extract_key_4;
            default: reject;
        }
    }

    state parse_mg_extract_ver_key_1 {
        pkt.extract(hdr.mg_ver_s1);
        pkt.extract(hdr.mg_key_s1);
        transition accept;
    }

    state parse_mg_extract_ver_key_2 {
        pkt.extract(hdr.mg_ver_s1);
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_ver_s2);
        pkt.extract(hdr.mg_key_s2);
        transition accept;
    }

    state parse_mg_extract_ver_key_3 {
        pkt.extract(hdr.mg_ver_s1);
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_ver_s2);
        pkt.extract(hdr.mg_key_s2);
        pkt.extract(hdr.mg_ver_s3);
        pkt.extract(hdr.mg_key_s3);
        transition accept;
    }

    state parse_mg_extract_ver_key_4 {
        pkt.extract(hdr.mg_ver_s1);
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_ver_s2);
        pkt.extract(hdr.mg_key_s2);
        pkt.extract(hdr.mg_ver_s3);
        pkt.extract(hdr.mg_key_s3);
        pkt.extract(hdr.mg_ver_s4);
        pkt.extract(hdr.mg_key_s4);
        transition accept;
    }

    state parse_mg_extract_key_1 {
        pkt.extract(hdr.mg_key_s1);
        transition accept;
    }

    state parse_mg_extract_key_2 {
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_key_s2);
        transition accept;
    }

    state parse_mg_extract_key_3 {
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_key_s2);
        pkt.extract(hdr.mg_key_s3);
        transition accept;
    }

    state parse_mg_extract_key_4 {
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_key_s2);
        pkt.extract(hdr.mg_key_s3);
        pkt.extract(hdr.mg_key_s4);
        transition accept;
    }

    state parse_mg_read_reply {
        pkt.extract(hdr.mg_bitmap_hdr);
        pkt.extract(hdr.mg_ver_s1);
        transition accept;
    }


    // Note: we removed multi-op reply packets
}


// ---------------------------------------------------------------------------
// Ingress Deparser
// ---------------------------------------------------------------------------

control KVMigrationIngressDeparser(
        packet_out pkt,
        inout migration_header_t hdr,
        in mg_ingress_metadata_t mg_ig_md,
        in ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md
        ) {
            
    Mirror() mirror;
    Digest<digest_migration_pair_t>() digest_migration_pair;
    Digest<digest_req_t>() digest_req;
    Checksum() ipv4_checksum;
    // Checksum() udp_checksum;
    
    apply {
        
        if (ig_dprsr_md.digest_type == 1) {
            digest_migration_pair.pack({hdr.mg_hdr.op, hdr.mg_pair_hdr.srcAddr, hdr.mg_pair_hdr.srcPort, hdr.mg_pair_hdr.dstAddr, \
                                        hdr.mg_pair_hdr.dstPort, hdr.mg_dict_mask.size_exp, hdr.mg_dict_mask.remain_exp});
        } 
        
        if (ig_dprsr_md.mirror_type == MIRROR_TYPE_I2E) {
            mirror.emit<mirror_I2E_h>(mg_ig_md.igr_mir_ses, {mg_ig_md.pkt_type, mg_ig_md.eth_dst_addr, mg_ig_md.ip_dst_addr, mg_ig_md.db_dst_port});
        }

        if (hdr.ipv4.isValid()) {
            hdr.ipv4.hdrChecksum = ipv4_checksum.update({
                hdr.ipv4.version, 
                hdr.ipv4.ihl, 
                hdr.ipv4.diffserv,
                hdr.ipv4.totalLen,
                hdr.ipv4.identification,
                hdr.ipv4.flags, 
                hdr.ipv4.fragOffset,
                hdr.ipv4.ttl, 
                hdr.ipv4.protocol,
                hdr.ipv4.srcAddr,
                hdr.ipv4.dstAddr});
        }
/*
        if (hdr.ipv4.isValid() && hdr.udp.isValid()) {
            hdr.udp.checksum = udp_checksum.update(data = {
                hdr.ipv4.srcAddr,
                hdr.ipv4.dstAddr,
                hdr.udp.srcPort,
                hdr.udp.dstPort,
                mg_ig_md.udp_checksum_tmp
            }, zeros_as_ones = true);
        }
*/
        pkt.emit(hdr.bridged_md);
        pkt.emit(hdr.ethernet);
        pkt.emit(hdr.ipv4);
        pkt.emit(hdr.tcp);
        pkt.emit(hdr.udp);
        pkt.emit(hdr.mg_hdr);
        pkt.emit(hdr.mg_pair_hdr);
        pkt.emit(hdr.mg_dict_mask);
        pkt.emit(hdr.mg_group_id);
        pkt.emit(hdr.mg_bitmap_hdr);
        pkt.emit(hdr.mg_ver_s1);
        pkt.emit(hdr.mg_key_s1);
        pkt.emit(hdr.mg_ver_s2);
        pkt.emit(hdr.mg_key_s2);    
        pkt.emit(hdr.mg_ver_s3);
        pkt.emit(hdr.mg_key_s3);     
        pkt.emit(hdr.mg_ver_s4);
        pkt.emit(hdr.mg_key_s4);
    }
}


// ---------------------------------------------------------------------------
// Egress parser
// ---------------------------------------------------------------------------

parser KVMigrationEgressParser(
        packet_in pkt,
        out migration_header_t hdr,
        out mg_egress_metadata_t mg_eg_md,
        out egress_intrinsic_metadata_t eg_intr_md
        ) {

    TofinoEgressParser() tofino_parser;
    Checksum() ipv4_checksum;
    // Checksum() udp_checksum;

    state start {
        tofino_parser.apply(pkt, eg_intr_md);
        transition parse_metadata;
    }

    state parse_metadata {
        mirror_E2E_h mirror_md = pkt.lookahead<mirror_E2E_h>();
        transition select(mirror_md.pkt_type) {
            PKT_TYPE_MIRROR_I2E : parse_I2E_mirror_md;
            PKT_TYPE_MIRROR_E2E : parse_E2E_mirror_md;
            PKT_TYPE_NORMAL: parse_briged_md;
            default: accept;
        }
    }
    
    state parse_I2E_mirror_md {
        pkt.extract(hdr.mirror_I2E_md);
        transition parse_ethernet;
    }

    state parse_E2E_mirror_md {
        pkt.extract(hdr.mirror_E2E_md);
        transition parse_ethernet;
    }

    state parse_briged_md {
        pkt.extract(hdr.bridged_md);
        transition parse_ethernet;
    }

    state parse_ethernet {
        pkt.extract(hdr.ethernet);
        transition select (hdr.ethernet.etherType) {
            ETHERTYPE_IPV4 : parse_ipv4;
            default: reject;
        }
    }

    state parse_ipv4 {
        pkt.extract(hdr.ipv4);
        ipv4_checksum.add(hdr.ipv4);
        // udp_checksum.subtract({hdr.ipv4.srcAddr, hdr.ipv4.dstAddr});
        transition select (hdr.ipv4.protocol) {
            IP_PROTOCOLS_TCP : parse_tcp;
            IP_PROTOCOLS_UDP : parse_udp;
            default : accept;
        }
    }

    state parse_tcp {
        pkt.extract(hdr.tcp);
        transition accept;
    }

    state parse_udp {
        pkt.extract(hdr.udp);
        // udp_checksum.subtract({hdr.udp.checksum});
        // udp_checksum.subtract({hdr.udp.srcPort, hdr.udp.dstPort});
        // udp_checksum.subtract_all_and_deposit(mg_eg_md.udp_checksum_tmp);
        transition select (hdr.udp.dstPort) {
            UDP_MG_PORT_BASE &&& UDP_MG_PORT_MASK : parse_migration;
            default: accept;
        }
    }

    state parse_migration {
        pkt.extract(hdr.mg_hdr);
        transition select (hdr.mg_hdr.op) {
            _MG_INIT : accept;
            _MG_MIGRATE: accept;
            _MG_TERMINATE : accept;
            _MG_MIGRATE_REPLY : accept;
            _MG_INIT_REPLY : accept;
            _MG_TERMINATE_REPLY : accept;
            _MG_MIGRATE_GROUP_START: accept;
            _MG_MIGRATE_GROUP_START_REPLY: accept;
            _MG_MIGRATE_GROUP_COMPLETE: accept;
            _MG_MIGRATE_GROUP_COMPLETE_REPLY: accept;
            _MG_READ : accept;
            _MG_WRITE : accept;
            _MG_DELETE : accept;
            _MG_READ_REPLY : accept;
            _MG_WRITE_REPLY : accept;
            _MG_DELETE_REPLY : accept;
            _MG_MULTI_READ : parse_mg_multi_read;
            _MG_MULTI_WRITE : parse_mg_multi_write;
            _MG_MULTI_DELETE : parse_mg_multi_delete;
            _MG_MULTI_READ_REPLY : accept;
            _MG_MULTI_WRITE_REPLY : accept;
            _MG_MULTI_DELETE_REPLY : accept;
            default : reject; 
        }
    }

    state parse_mg_multi_read {
        pkt.extract(hdr.mg_bitmap_hdr);
        transition select (hdr.mg_bitmap_hdr.t) {
            0x1 &&& 0xF: parse_mg_extract_ver_key_1;
            0x2 &&& 0xE: parse_mg_extract_ver_key_2;
            0x4 &&& 0xC: parse_mg_extract_ver_key_3;
            0x8 &&& 0x8: parse_mg_extract_ver_key_4;
            default: reject;
        }
    }

    state parse_mg_multi_delete {
        pkt.extract(hdr.mg_bitmap_hdr);
        transition select (hdr.mg_bitmap_hdr.t) {
            0x1 &&& 0xF: parse_mg_extract_key_1;
            0x2 &&& 0xE: parse_mg_extract_key_2;
            0x4 &&& 0xC: parse_mg_extract_key_3;
            0x8 &&& 0x8: parse_mg_extract_key_4;
            default: reject;
        }
    }

    state parse_mg_multi_write {
        pkt.extract(hdr.mg_bitmap_hdr);
        transition select (hdr.mg_bitmap_hdr.t) {
            0x1 &&& 0xF: parse_mg_extract_key_1;
            0x2 &&& 0xE: parse_mg_extract_key_2;
            0x4 &&& 0xC: parse_mg_extract_key_3;
            0x8 &&& 0x8: parse_mg_extract_key_4;
            default: reject;
        }
    }

    state parse_mg_extract_ver_key_1 {
        pkt.extract(hdr.mg_ver_s1);
        pkt.extract(hdr.mg_key_s1);
        transition accept;
    }

    state parse_mg_extract_ver_key_2 {
        pkt.extract(hdr.mg_ver_s1);
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_ver_s2);
        pkt.extract(hdr.mg_key_s2);
        transition accept;
    }

    state parse_mg_extract_ver_key_3 {
        pkt.extract(hdr.mg_ver_s1);
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_ver_s2);
        pkt.extract(hdr.mg_key_s2);
        pkt.extract(hdr.mg_ver_s3);
        pkt.extract(hdr.mg_key_s3);
        transition accept;
    }

    state parse_mg_extract_ver_key_4 {
        pkt.extract(hdr.mg_ver_s1);
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_ver_s2);
        pkt.extract(hdr.mg_key_s2);
        pkt.extract(hdr.mg_ver_s3);
        pkt.extract(hdr.mg_key_s3);
        pkt.extract(hdr.mg_ver_s4);
        pkt.extract(hdr.mg_key_s4);
        transition accept;
    }

    state parse_mg_extract_key_1 {
        pkt.extract(hdr.mg_key_s1);
        transition accept;
    }

    state parse_mg_extract_key_2 {
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_key_s2);
        transition accept;
    }

    state parse_mg_extract_key_3 {
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_key_s2);
        pkt.extract(hdr.mg_key_s3);
        transition accept;
    }

    state parse_mg_extract_key_4 {
        pkt.extract(hdr.mg_key_s1);
        pkt.extract(hdr.mg_key_s2);
        pkt.extract(hdr.mg_key_s3);
        pkt.extract(hdr.mg_key_s4);
        transition accept;
    }
}

// ---------------------------------------------------------------------------
// Egress Deparser
// ---------------------------------------------------------------------------

control KVMigrationEgressDeparser(
        packet_out pkt,
        inout migration_header_t hdr,
        in mg_egress_metadata_t mg_eg_md,
        in egress_intrinsic_metadata_for_deparser_t eg_dprsr_md
        ) {
    
    Mirror() mirror;
    Checksum() ipv4_checksum;
    // Checksum() udp_checksum;

    apply {

        if (eg_dprsr_md.mirror_type == MIRROR_TYPE_E2E) {
            mirror.emit<mirror_E2E_h>(mg_eg_md.egr_mir_ses, {mg_eg_md.pkt_type, mg_eg_md.redirect_to_dst, mg_eg_md.eth_dst_addr, mg_eg_md.ip_dst_addr, mg_eg_md.db_dst_port});
        }

        if (hdr.ipv4.isValid()) {
            hdr.ipv4.hdrChecksum = ipv4_checksum.update({
                hdr.ipv4.version, 
                hdr.ipv4.ihl, 
                hdr.ipv4.diffserv,
                hdr.ipv4.totalLen,
                hdr.ipv4.identification,
                hdr.ipv4.flags, 
                hdr.ipv4.fragOffset,
                hdr.ipv4.ttl, 
                hdr.ipv4.protocol,
                hdr.ipv4.srcAddr,
                hdr.ipv4.dstAddr});
        }
/*
        if (hdr.ipv4.isValid() && hdr.udp.isValid()) {
            hdr.udp.checksum = udp_checksum.update(data = {
                hdr.ipv4.srcAddr,
                hdr.ipv4.dstAddr,
                hdr.udp.srcPort,
                hdr.udp.dstPort,
                mg_eg_md.udp_checksum_tmp
            }, zeros_as_ones = true);
        }
*/

        pkt.emit(hdr.ethernet);
        pkt.emit(hdr.ipv4);
        pkt.emit(hdr.tcp);
        pkt.emit(hdr.udp);
        pkt.emit(hdr.mg_hdr);
        pkt.emit(hdr.mg_pair_hdr);
        pkt.emit(hdr.mg_dict_mask);
        pkt.emit(hdr.mg_group_id);
        pkt.emit(hdr.mg_bitmap_hdr);
        pkt.emit(hdr.mg_ver_s1);
        pkt.emit(hdr.mg_key_s1);
        pkt.emit(hdr.mg_ver_s2);
        pkt.emit(hdr.mg_key_s2);
        pkt.emit(hdr.mg_ver_s3);
        pkt.emit(hdr.mg_key_s3);
        pkt.emit(hdr.mg_ver_s4);
        pkt.emit(hdr.mg_key_s4);
    }
}