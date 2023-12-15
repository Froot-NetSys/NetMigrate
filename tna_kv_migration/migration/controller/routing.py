#  Copyright (C) 2023 Zeying Zhu, University of Maryland, College Park
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.

import logging

from control import Control


class Routing(Control):

    def __init__(self, target, gc, bfrt_info):
        # Set up base class
        super(Routing, self).__init__(target, gc)

        self.log = logging.getLogger(__name__)

        self.tables = [
            bfrt_info.table_get("KVMigrationIngress.ingress_routing.ipv4_routing"),
            bfrt_info.table_get("KVMigrationIngress.ingress_routing.mg_redirect_to_destination"),
            bfrt_info.table_get("KVMigrationIngress.ingress_routing.mg_mirror_fwd"),
            # bfrt_info.table_get("KVMigrationIngress.ingress_routing.mg_false_positive_handler"),
        ]

        self.ipv4_routing = self.tables[0]
        self.mg_redirect_to_destination = self.tables[1]
        self.mg_mirror_fwd = self.tables[2]
        # self.mg_false_positive_handler = self.tables[3]
        self.mirror_cfg_table = bfrt_info.table_get("$mirror.cfg")
        
        # Clear tables
        self._clear()

    
    def config_default_entries(self):
        action_data = self.ipv4_routing.make_data([], action_name = "KVMigrationIngress.ingress_routing.ndrop")
        self.ipv4_routing.default_entry_set( target = self.target, data = action_data )

        action_data = self.mg_redirect_to_destination.make_data([], action_name = "KVMigrationIngress.ingress_routing.no_update_dst_header_back")
        self.mg_redirect_to_destination.default_entry_set( target = self.target, data = action_data )
    
    def add_mirror_cfg_entry(self, sid, port, str_val="INGRESS"):
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

    def add_mirror_fwd_entry(self, src_addr, ing_sid=0):

        self.log.info("Programming mirror forwarding table ...")
    
        self.mg_mirror_fwd.entry_add(
            self.target,
            [self.mg_mirror_fwd.make_key([self.gc.KeyTuple('hdr.ipv4.dstAddr', src_addr)])],
            [self.mg_mirror_fwd.make_data([self.gc.DataTuple('ing_ses', ing_sid)],
                                           'KVMigrationIngress.ingress_routing.set_mirror_fwd')]
        )
    
    def del_mirror_fwd_entry(self, src_addr):

        self.mg_mirror_fwd.entry_del(
            self.target,
            [self.mg_mirror_fwd.make_key([self.gc.KeyTuple('hdr.ipv4.dstAddr', src_addr)])])

    def add_ipv4_routing_entry(self, dst_addr, port):

        self.ipv4_routing.entry_add(
            self.target,
            [self.ipv4_routing.make_key([self.gc.KeyTuple('hdr.ipv4.dstAddr', dst_addr, prefix_len = 32)])],
            [self.ipv4_routing.make_data([self.gc.DataTuple('egress_spec', port)], 
                                                            "KVMigrationIngress.ingress_routing.set_egress")]
        )

    def add_ipv4_routing_entries(self, entry_list):
        ''' Add MIB entries.

            Keyword arguments:
                entry_list -- a list of tuples: (ip_address, dev_port)
        '''
        for (ip_address, dev_port) in entry_list:
            self.add_ipv4_routing_entry(ip_address, dev_port)

    def add_redirect_to_destination_entry(self, ip_dst_addr, port):

        self.mg_redirect_to_destination.entry_add(
            self.target,
            [self.mg_redirect_to_destination.make_key([self.gc.KeyTuple('mg_md.dst_pair.ip_addr', ip_dst_addr, prefix_len = 32)])],
            [self.mg_redirect_to_destination.make_data([self.gc.DataTuple('egress_spec', port)], 
                                                            "KVMigrationIngress.ingress_routing.redirect_to_destination")]
        )
    
    def del_redirect_to_destination_entry(self, ip_dst_addr):

        self.mg_redirect_to_destination.entry_del(
            self.target,
            [self.mg_redirect_to_destination.make_key([self.gc.KeyTuple('mg_md.dst_pair.ip_addr', ip_dst_addr, prefix_len = 32)])])
    
    

    def _clear(self):
        super(Routing, self)._clear()

