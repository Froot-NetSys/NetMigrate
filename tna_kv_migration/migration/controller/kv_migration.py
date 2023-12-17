import logging

from control import Control
from common import *


class KVMigration(Control):

    def __init__(self, target, gc, bfrt_info):
        super(KVMigration, self).__init__(target, gc)

        self.log = logging.getLogger(__name__)
        self.bfrt_info = bfrt_info

        self.tables = [
            # Tables with size > 1
            bfrt_info.table_get("KVMigrationIngress.mg_migration_pair_lookup"),
            bfrt_info.table_get("KVMigrationEgress.mg_recirculate_and_E2E_mirror")
        ]
        self.migration_pair_table = self.tables[0] # index might be modified
        self.recirculate_and_e2e_mirror_table = self.tables[1]
        self.mirror_cfg_table = bfrt_info.table_get("$mirror.cfg")

        self.registers = [
            bfrt_info.table_get("KVMigrationIngress.mg_bloom_filter_reg_1"),
            bfrt_info.table_get("KVMigrationIngress.mg_bloom_filter_reg_2"),
            bfrt_info.table_get("KVMigrationIngress.mg_bloom_filter_reg_3"),
            bfrt_info.table_get("KVMigrationIngress.mg_bloom_filter_reg_4"),
            bfrt_info.table_get("KVMigrationIngress.mg_counting_bloom_filter_reg_1"),
            bfrt_info.table_get("KVMigrationIngress.mg_counting_bloom_filter_reg_2"),
            bfrt_info.table_get("KVMigrationIngress.mg_counting_bloom_filter_reg_3"),
            bfrt_info.table_get("KVMigrationIngress.mg_counting_bloom_filter_reg_4"),
            # bfrt_info.table_get("KVMigrationIngress.mg_reply_filter_reg_1"),
            # bfrt_info.table_get("KVMigrationIngress.mg_reply_filter_reg_2")
        ]

        self.register_name = [
            "KVMigrationIngress.mg_bloom_filter_reg_1",
            "KVMigrationIngress.mg_bloom_filter_reg_2",
            "KVMigrationIngress.mg_bloom_filter_reg_3",
            "KVMigrationIngress.mg_bloom_filter_reg_4",
            "KVMigrationIngress.mg_counting_bloom_filter_reg_1",
            "KVMigrationIngress.mg_counting_bloom_filter_reg_2",
            "KVMigrationIngress.mg_counting_bloom_filter_reg_3",
            "KVMigrationIngress.mg_counting_bloom_filter_reg_4",
            "KVMigrationIngress.mg_reply_filter_reg_1",
            "KVMigrationIngress.mg_reply_filter_reg_2"
        ]
        

        # Set default entries
        action_data = self.migration_pair_table.make_data([], action_name = "KVMigrationIngress.set_default_dst_addr")
        self.migration_pair_table.default_entry_set( target = target, data = action_data )

        action_data = self.recirculate_and_e2e_mirror_table.make_data([], action_name = "KVMigrationEgress.nop")
        self.recirculate_and_e2e_mirror_table.default_entry_set( target = target, data = action_data )

        
        # Clear register
        self._clear_registers()

    def write_registers(self, reg, name, value, size):
        for i in range(size):
            reg.entry_add(self.target, [reg.make_key([self.gc.KeyTuple('$REGISTER_INDEX', i)])],
                [reg.make_data([self.gc.DataTuple(name, value)])])

    def reset_bloom_filters(self):
        for i in range(0, 4):
            self.write_registers(self.registers[i], self.register_name[i] + ".f1", 0, MigrationParameters.MG_MAX_BLOOM_FILTER_ENTRIES)
        for i in range(4, 8):
            self.write_registers(self.registers[i], self.register_name[i] + ".f1", 0, MigrationParameters.MG_COUNTING_BLOOM_FILTER_ENTRIES)
        
        # for i in range(8,10):
        #     self.write_registers(self.registers[i], self.register_name[i] + ".f1", 0, MigrationParameters.MG_REPLY_FILTER_SIZE)

    # Debug only
    def show_bloom_filters(self):
        for reg_idx in range(0, 8):
            # register "get" ==> debug bf entry update
            reg = self.registers[reg_idx]
            if reg_idx < 4:
                length = MigrationParameters.MG_MAX_BLOOM_FILTER_ENTRIES
            else:
                length = MigrationParameters.MG_COUNTING_BLOOM_FILTER_ENTRIES
            resp = reg.entry_get(self.target, 
                                [reg.make_key([self.gc.KeyTuple('$REGISTER_INDEX', i)]) for i in range(0, length)],
                                flags={"from_hw": True})
            values = []
            idx = 0
            for data, _ in resp:
                data_dict = data.to_dict()
                read_value_0 = data_dict[self.register_name[reg_idx]+".f1"][0]
                read_value_1 = data_dict[self.register_name[reg_idx]+".f1"][1]
                if read_value_0 != 0 or read_value_1 != 0:
                    print idx, data_dict
                idx += 1

        print "End of scanning bloom filters."
        

    def add_mirror_cfg_entry(self, sid, port, str_val="EGRESS"):
        self.mirror_cfg_table.entry_add(
            self.target,
            [self.mirror_cfg_table.make_key([self.gc.KeyTuple('$sid', sid)])],
            [self.mirror_cfg_table.make_data([self.gc.DataTuple('$direction', str_val=str_val),
                                              self.gc.DataTuple('$ucast_egress_port', port),
                                              self.gc.DataTuple('$ucast_egress_port_valid', bool_val=True),
                                              self.gc.DataTuple('$session_enable', bool_val=True)],
                                              '$normal')]
        )
        self.log.info("Using session %d for port %d", sid, port)

    def del_mirror_cfg_entry(self, sid):

        self.mirror_cfg_table.entry_del(
            self.target,
            [self.mirror_cfg_table.make_key([self.gc.KeyTuple('$sid', sid)])])
        self.log.info("Deleting session %d", sid)

    def add_recirculate_and_e2e_mirror_entry(self, sid):
        self.recirculate_and_e2e_mirror_table.entry_add(self.target,
            [self.recirculate_and_e2e_mirror_table.make_key([self.gc.KeyTuple("hdr.bridged_md.do_egr_mirroring", 1)])],
            [self.recirculate_and_e2e_mirror_table.make_data([self.gc.DataTuple("egr_ses", sid)],
                                                              action_name = "KVMigrationEgress.set_recirculate_and_E2E_mirror")]
        )

    def add_migration_pair(self, ip_srcAddr, db_srcPort, eth_dstAddr, ip_dstAddr, db_dstPort, size_exp, remain_exp):
        self.migration_pair_table.entry_add(self.target, 
            [self.migration_pair_table.make_key([self.gc.KeyTuple("hdr.ipv4.dstAddr", ip_srcAddr), self.gc.KeyTuple("hdr.mg_hdr.dbPort", db_srcPort)])],
            [self.migration_pair_table.make_data([self.gc.DataTuple("eth_dstAddr", eth_dstAddr),
                                                self.gc.DataTuple("ip_dstAddr", ip_dstAddr), 
                                                self.gc.DataTuple("db_dstPort", db_dstPort),
                                                self.gc.DataTuple("size_exp", size_exp),
                                                self.gc.DataTuple("remain_exp", remain_exp)], 
                                                action_name = "KVMigrationIngress.read_mg_dst_addr")]
        )
    
    def del_migration_pair(self, ip_srcAddr, db_srcPort):
        self.migration_pair_table.entry_del(self.target, 
            [self.migration_pair_table.make_key([self.gc.KeyTuple("hdr.ipv4.dstAddr", ip_srcAddr), self.gc.KeyTuple("hdr.mg_hdr.dbPort", db_srcPort)])]
        )
    
    def hdl_digest_migration_pair(self, interface):
        # Digest
        self.learn_filter = self.bfrt_info.learn_get("digest_migration_pair")
        self.digest = interface.digest_get()
        data_list = self.learn_filter.make_data_list(self.digest)
        data_dict = data_list[0].to_dict()
        recv_op = data_dict["op"]
        recv_src_addr = data_dict["srcAddr"]
        recv_src_port = data_dict["srcPort"]
        recv_dst_addr = data_dict["dstAddr"]
        recv_dst_port = data_dict["dstPort"]
        size_exp = data_dict["dict_size_exp"]
        remain_exp = data_dict["dict_remain_exp"]
        
        return recv_op, recv_src_addr, recv_src_port, recv_dst_addr, recv_dst_port, size_exp, remain_exp
    
    def hdl_digest_req(self, interface):
        learn_filter = self.bfrt_info.learn_get("digest_req")
        digest = interface.digest_get()
        data_list = learn_filter.make_data_list(digest)
        data_dict = data_list[0].to_dict()
        op = data_dict["op"]
        ipAddr = data_dict["ipAddr"]
        dbPort = data_dict["dbPort"]
        UDP_dst_port = data_dict["UDP_dst_port"]

        return op, ipAddr, dbPort, UDP_dst_port

    def _clear_registers(self):
        if self.registers is not None:
            for register in self.registers:
                register.entry_del(self.target)

    def _clear(self):
        # Clear tables
        super(KVMigration, self)._clear()
        
        # Clear registers
        self._clear_registers()
