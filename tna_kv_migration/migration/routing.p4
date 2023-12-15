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

control IngressRouting(
    inout migration_header_t hdr,
    inout ingress_intrinsic_metadata_for_tm_t ig_intr_md_for_tm,
    inout ingress_intrinsic_metadata_for_deparser_t ig_dprsr_md,
    inout mg_metadata_t mg_md,
    inout mg_ingress_metadata_t mg_ig_md) {
    
    action set_normal_pkt(bit<8> do_egr_mirroring) {
        hdr.bridged_md.setValid();
        hdr.bridged_md.pkt_type = PKT_TYPE_NORMAL;
        hdr.bridged_md.do_egr_mirroring = do_egr_mirroring;
        hdr.bridged_md.redirect_to_dst = mg_md.redirect_to_dst;
        hdr.bridged_md.eth_dst_addr = mg_md.dst_pair.eth_addr;
        hdr.bridged_md.ip_dst_addr = mg_md.dst_pair.ip_addr;
        hdr.bridged_md.db_dst_port = mg_md.dst_pair.db_port;
    }

    table mg_set_normal_pkt {
        key = { hdr.mg_hdr.isValid() : exact;
                hdr.mg_hdr.op: exact;
                hdr.mg_bitmap_hdr.t: range; }
        actions = { set_normal_pkt; }
        size = 4;
        const entries = {
            (true, _MG_MULTI_READ, 1..15) : set_normal_pkt(0x1);
            (true, _MG_MULTI_DELETE, 1..15) : set_normal_pkt(0x1);
            (true, _MG_MULTI_WRITE, 1..15) : set_normal_pkt(0x1);
        }
        const default_action = set_normal_pkt(0x0);
    }

    // ipv4 routing
    action set_egress(bit<9> egress_spec) {
        ig_intr_md_for_tm.ucast_egress_port = egress_spec;
    }

    action ndrop() {
        ig_dprsr_md.drop_ctl[0:0] = 0;
    }
    
    table ipv4_routing {
        key = { hdr.ipv4.dstAddr : lpm; }
        actions = { set_egress; ndrop; }
        size = 16;
    }
    
    // redirect to destination routing
    // triggered by READ 
    action redirect_to_destination(bit<9> egress_spec) {
        ig_intr_md_for_tm.ucast_egress_port = egress_spec;
        mg_md.redirect_to_dst = 1; // postpone to egress to change destination ip and dbPort
        /*
        // for debug use:
        hdr.ethernet.dstAddr = mg_md.dst_pair.eth_addr;
        hdr.ipv4.dstAddr = mg_md.dst_pair.ip_addr;
        hdr.mg_hdr.dbPort = mg_md.dst_pair.db_port;
        */
    }

    action no_update_dst_header_back() {
        mg_md.redirect_to_dst = 0;
    }

    table mg_redirect_to_destination {
        key = { 
            mg_md.dst_pair.ip_addr : lpm;
        }
        actions = { redirect_to_destination; @defaultonly no_update_dst_header_back;}
        size = 16;
    }

    // mirror to destination routing
    // triggered by WRITE, MULTI_WRITE, READ, MULTI_READ
    action set_mirror_fwd(MirrorId_t ing_ses) {
        mg_ig_md.igr_mir_ses = ing_ses;
        mg_ig_md.pkt_type = PKT_TYPE_MIRROR_I2E;
        mg_ig_md.eth_dst_addr = mg_md.dst_pair.eth_addr;
        mg_ig_md.ip_dst_addr = mg_md.dst_pair.ip_addr;
        mg_ig_md.db_dst_port = mg_md.dst_pair.db_port;
        // We force source server and destination server use the same UDP port, only modify dbPort here

        // Set mirror type
        ig_dprsr_md.mirror_type = MIRROR_TYPE_I2E;
        hdr.mg_ver_s1.ver[2:2] = 1;

        /*
        // for debug use
        ig_dprsr_md.digest_type = 1;
        hdr.mg_pair_hdr.setValid();
        hdr.mg_pair_hdr.srcAddr = mg_md.dst_pair.eth_addr[47:16];
        hdr.mg_pair_hdr.srcPort = mg_md.dst_pair.eth_addr[15:0];
        hdr.mg_pair_hdr.dstAddr = mg_md.dst_pair.ip_addr;
        hdr.mg_pair_hdr.dstPort = hdr.udp.dstPort;
        */
    }

    table mg_mirror_fwd {
        key = { 
            hdr.ipv4.dstAddr : exact;
        }
        actions = { set_mirror_fwd; }
        size = 16;
    }

    action set_false_positive_bit() {
        mg_md.cbf_false_positive = 1;
    }
    
    action clear_false_positive_bit() {
        mg_md.cbf_false_positive = 0;
    }

    table mg_false_positive_handler {
        key = { mg_md.bucket_id_s1 : exact; }
        actions = { set_false_positive_bit; clear_false_positive_bit; }
        default_action = clear_false_positive_bit;
        size = 256;
    }

    action drop() {
        ig_dprsr_md.drop_ctl[0:0] = 0;
    }

    /*
    table mg_drop {
        key = { hdr.mg_hdr.isValid() : exact;
                hdr.mg_hdr.op : exact;
                mg_md.rf_match_success : exact; }
        actions = { @defaultonly ndrop; drop; }
        size = 2;
        const entries = {
            (true, _MG_READ_REPLY, 1) : drop();
        }
        const default_action = ndrop();
    }
    */

    action remove_bit_1() {
        hdr.mg_bitmap_hdr.t[0:0] = 0;
    }

    action remove_bit_2() {
        hdr.mg_bitmap_hdr.t[1:1] = 0;
    }

    action remove_bit_3() {
        hdr.mg_bitmap_hdr.t[2:2] = 0;
    }

    action remove_bit_4() {
        hdr.mg_bitmap_hdr.t[3:3] = 0;
    }

    table update_bitmap_hdr {
        key = { hdr.mg_hdr.isValid() : exact;
                hdr.mg_hdr.op : exact;
                hdr.mg_bitmap_hdr.t : ternary; }
        actions = { remove_bit_1; remove_bit_2; remove_bit_3; remove_bit_4; @defaultonly ndrop; }
        size = 13;
        const entries = {
            (true, _MG_MULTI_READ, 0x1 &&& 0x1) : remove_bit_1;
            (true, _MG_MULTI_READ, 0x2 &&& 0x3) : remove_bit_2;
            (true, _MG_MULTI_READ, 0x4 &&& 0x7) : remove_bit_3;
            (true, _MG_MULTI_READ, 0x8 &&& 0xF) : remove_bit_4;
            (true, _MG_MULTI_DELETE, 0x1 &&& 0x1) : remove_bit_1;
            (true, _MG_MULTI_DELETE, 0x2 &&& 0x3) : remove_bit_2;
            (true, _MG_MULTI_DELETE, 0x4 &&& 0x7) : remove_bit_3;
            (true, _MG_MULTI_DELETE, 0x8 &&& 0xF) : remove_bit_4;
            (true, _MG_MULTI_WRITE, 0x1 &&& 0x1) : remove_bit_1;
            (true, _MG_MULTI_WRITE, 0x2 &&& 0x3) : remove_bit_2;
            (true, _MG_MULTI_WRITE, 0x4 &&& 0x7) : remove_bit_3;
            (true, _MG_MULTI_WRITE, 0x8 &&& 0xF) : remove_bit_4;
        }
        const default_action = ndrop;
    }
    
    apply {
        ipv4_routing.apply();

        /*
        // debug use only
        if (hdr.mg_hdr.op == _MG_READ) {
            mg_redirect_to_destination.apply();
        }
        else if (hdr.mg_hdr.op == _MG_WRITE) {
            mg_mirror_fwd.apply();
        }
        */
    
        if ((mg_md.dst_pair.ip_addr != 0xFFFFFFFF && mg_md.dst_pair.db_port != 0xFFFF) && \
            (hdr.mg_hdr.op == _MG_READ || hdr.mg_hdr.op == _MG_WRITE || hdr.mg_hdr.op == _MG_DELETE || \
             hdr.mg_hdr.op == _MG_MULTI_READ || hdr.mg_hdr.op == _MG_MULTI_WRITE || hdr.mg_hdr.op == _MG_MULTI_DELETE)) {
                // send to the ''right'' server; not related to the cache logic
                // mg_false_positive_handler.apply();
                 if (mg_md.cbf_1 == 0 || mg_md.cbf_2 == 0 || mg_md.cbf_3 == 0 || mg_md.cbf_4 == 0) {
                    if (mg_md.bf_1 == 1 && mg_md.bf_2 == 1 && mg_md.bf_3 == 1 && mg_md.bf_4 == 1) {
                 // if (mg_md.cbf_1 == 0 || mg_md.cbf_2 == 0 || mg_md.cbf_3 == 0) {
		 //   if (mg_md.bf_1 == 1 && mg_md.bf_2 == 1 && mg_md.bf_3 == 1) {
			// entire group completes migration to destination
                        // false positive: entire group has not started migration
                        mg_redirect_to_destination.apply();
                    }
                    else {
                        // entire group not start migration
                        // no false positive or false negative
                    }
                }
                else {
                    // all three cases are possible or group under migration
                    // group under migration => false negative: entire group has not started migration
                    // use double read; write redirect to destination
                    if (hdr.mg_hdr.op == _MG_READ || hdr.mg_hdr.op == _MG_MULTI_READ) {
                        mg_mirror_fwd.apply();
                    }
                    else 
                        mg_redirect_to_destination.apply();
                }
            // remove lowest 1 in bitmap for multi_key operations
            update_bitmap_hdr.apply();
        }
    

        // mg_drop.apply();
        
        mg_set_normal_pkt.apply();
        

    }
}
