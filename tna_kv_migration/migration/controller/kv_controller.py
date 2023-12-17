import os
import sys
import glob
import yaml
import signal
import threading
import argparse
import logging



# Add BF Python to search path
bfrt_location = '{}/lib/python*/site-packages/tofino'.format(
    os.environ['SDE_INSTALL'])
sys.path.append(glob.glob(bfrt_location)[0])
import bfrt_grpc.client as gc
from thriftutils import *

from ports import Ports
from routing import Routing
from kv_migration import KVMigration
from common import *


def sigint_handler(signal_received, frame):
    print 'SIGINT or CTRL-C detected. Stopping controller.' 
        
    # Flush log, stdout, stderr
    sys.stdout.flush()
    sys.stderr.flush()
    logging.shutdown()

    # Exit
    os.kill(os.getpid(), signal.SIGTERM)

def uint_to_i48(u):
    if u > 0x7FFFFFFFFFFF:
        u -= 0x1000000000000
    return u

def macAddr_to_i48(addr):
    byte_array = [int(b, 16) for b in addr.split(":")]
    res = 0
    for b in byte_array:
        res = res * 256 + b
    return uint_to_i48(res)

class KVMigrationController(object):

    def __init__(self):
        super(KVMigrationController, self).__init__()

        self.log = logging.getLogger(__name__)
        self.log.info('kv_migration Controller')

        # CPU PCIe port
        self.cpu_port = 64 
        self.recirculate_port = 68

    def critical_error(self, msg):
        self.log.critical(msg)
        # print msg, file=sys.stderr
        sys.stderr.write(msg)
        logging.shutdown()
        #sys.exit(1)
        os.kill(os.getpid(), signal.SIGTERM)

    def setup(self,
              program,
              switch_mac,
              switch_ip,
              bfrt_ip,
              bfrt_port,
              ports_file):

        # Device 0
        self.dev = 0
        # Target all pipes
        self.target = gc.Target(self.dev, pipe_id=0xFFFF)

        # Connect to BFRT server
        try:
            self.interface = gc.ClientInterface('{}:{}'.format(bfrt_ip, bfrt_port),
                                           client_id=0,
                                           device_id=self.dev)
        except RuntimeError as re:
            msg = re.args[0] % re.args[1]
            self.critical_error(msg)
        else:
            self.log.info('Connected to BFRT server {}:{}'.format(
                bfrt_ip, bfrt_port))

        try:
            self.interface.bind_pipeline_config(program)
        except gc.BfruntimeForwardingRpcException:
            self.critical_error('P4 program {} not found!'.format(program))

        try:
            # Get all tables for program
            self.bfrt_info = self.interface.bfrt_info_get(program)

            # Ports table
            self.ports = Ports(self.target, gc, self.bfrt_info)

            # Setup tables
            # Migration and Serving Request tables
            self.kv_migration = KVMigration(self.target, gc, self.bfrt_info)
            # self.kv_migration.reset_bloom_filters()
            
            # routing tables
            self.routing = Routing(self.target, gc, self.bfrt_info)
            self.routing.config_default_entries()

            self.mib = {}
            self.arp = {}
            self.sids = {}
            self.sid_list = set()
            self.global_sid = 1
            self.redirect_to_dst_list = set()
            self.mg_pair_list = set()

            # Setup recirculation and E2E mirror
            sid = self.get_avail_sid()
            self.sid_list.add(sid)
            self.kv_migration.add_mirror_cfg_entry(sid, self.recirculate_port, "EGRESS")
            self.kv_migration.add_recirculate_and_e2e_mirror_entry(sid)
            
            # Enable ports
            success, ports = self.load_ports_file(ports_file)
            if not success:
                self.critical_error(ports)

            # Set switch addresses
            self.set_switch_mac_and_ip(switch_mac, switch_ip)

        except KeyboardInterrupt:
            self.critical_error('Stopping controller.')
        except Exception as e:
            self.log.exception(e)
            self.critical_error('Unexpected error. Stopping controller.')

    def load_ports_file(self, ports_file):
        ''' Load ports yaml file and enable front panel ports.
            Keyword arguments:
                ports_file -- yaml file name
            Returns:
                (success flag, list of ports or error message)
        '''

        ports = []

        with open(ports_file) as f:
            yaml_ports = yaml.safe_load(f)

            for port, value in yaml_ports['ports'].items():

                re_match = front_panel_regex.match(port)
                if not re_match:
                    return (False, 'Invalid port {}'.format(port))

                fp_port = int(re_match.group(1))
                fp_lane = int(re_match.group(2))

                # Convert all keys to lowercase
                value = {k.lower(): v for k, v in value.items()}

                if 'speed' in value:
                    try:
                        speed = int(value['speed'].upper().replace('G',
                                                                   '').strip())
                    except ValueError:
                        return (False, 'Invalid speed for port {}'.format(port))

                    if speed not in [10, 25, 40, 50, 100]:
                        return (
                            False,
                            'Port {} speed must be one of 10G,25G,40G,50G,100G'.
                            format(port))
                else:
                    speed = 100

                if 'fec' in value:
                    fec = value['fec'].lower().strip()
                    if fec not in ['none', 'fc', 'rs']:
                        return (False,
                                'Port {} fec must be one of none, fc, rs'.
                                format(port))
                else:
                    fec = 'none'

                if 'autoneg' in value:
                    an = value['autoneg'].lower().strip()
                    if an not in ['default', 'enable', 'disable']:
                        return (
                            False,
                            'Port {} autoneg must be one of default, enable, disable'
                            .format(port))
                else:
                    an = 'default'

                if 'mac' not in value:
                    return (False,
                            'Missing MAC address for port {}'.format(port))

                if 'ip' not in value:
                    return (False,
                            'Missing IP address for port {}'.format(port))

                success, dev_port = self.ports.get_dev_port(fp_port, fp_lane)
                if success:
                    self.mib[ipv4Addr_to_i32(value['ip'])] = dev_port
                    self.arp[ipv4Addr_to_i32(value['ip'])] = macAddr_to_i48(value['mac'])
                else:
                    return (False, dev_port)

                ports.append((fp_port, fp_lane, speed, fec, an))

        # Add ports -- using bfshell now
        # success, error_msg = self.ports.add_ports(ports)
        # if not success:
        #     return (False, error_msg)

        # Add forwarding entries
        print "MIB table:"
        print self.mib
        print "ARP table:" 
        print self.arp
        self.routing.add_ipv4_routing_entries(self.mib.items())

        return (True, ports)
        
    def set_switch_mac_and_ip(self, switch_mac, switch_ip):
        ''' Set switch MAC and IP '''
        self.switch_mac = switch_mac.upper()
        self.switch_ip = switch_ip

    def get_switch_mac_and_ip(self):
        ''' Get switch MAC and IP '''
        return self.switch_mac, self.switch_ip

    def get_avail_sid(self):
        while self.global_sid in self.sid_list:    
            self.global_sid = (self.global_sid + 1) % 1023 + 1

        if self.global_sid not in self.sid_list:
            return self.global_sid
        else:
            self.critical_error('mirror session id used out.')
            return None

    def hdl_digest(self):
        while True:
            try: 
                recv_op, recv_src_addr, recv_src_port, recv_dst_addr, recv_dst_port, size_exp, remain_exp = self.kv_migration.hdl_digest_migration_pair(self.interface)

                print recv_op, recv_src_addr, recv_src_port, recv_dst_addr, recv_dst_port, size_exp, remain_exp

                if recv_op == MigrationOp.MG_INIT:
                    self.log.info("{}: configure table entries based on MG_INIT packets...".format("hdl_digest"))
                    if (recv_src_addr, recv_src_port) not in self.mg_pair_list:
                        self.mg_pair_list.add((recv_src_addr, recv_src_port))
                        self.kv_migration.add_migration_pair(recv_src_addr, recv_src_port, self.arp[recv_dst_addr], recv_dst_addr, recv_dst_port, size_exp, remain_exp)

                    dev_port = self.mib[recv_dst_addr]
                    
                    if recv_src_addr not in self.sids.keys():    
                        sid = self.get_avail_sid()
                        self.sids[recv_src_addr] = sid
                        self.sid_list.add(sid)
                        self.routing.add_mirror_cfg_entry(sid, dev_port)
                        self.routing.add_mirror_fwd_entry(recv_src_addr, sid)
                    
                    if recv_dst_addr not in self.redirect_to_dst_list:
                        self.redirect_to_dst_list.add(recv_dst_addr)
                        self.routing.add_redirect_to_destination_entry(recv_dst_addr, dev_port)

                elif recv_op == MigrationOp.MG_TERMINATE:
                    self.log.info("{}: configure table entries based on MG_TERMINATE packets...".format("hdl_digest"))
                    # self.routing.del_redirect_to_destination_entry(recv_dst_addr)
                    # self.kv_migration.del_migration_pair(recv_src_addr, recv_src_port)
                    # Do not delete migration pair and redirect entry until client updates its routing to destination
                    
                    if recv_src_addr in self.sids.keys():
                        self.routing.del_mirror_fwd_entry(recv_src_addr)
                        self.routing.del_mirror_cfg_entry(self.sids[recv_src_addr])
                        self.sid_list.remove(self.sids[recv_src_addr])
                        self.sids.pop(recv_src_addr, None)
                    
                    
            except RuntimeError:
                pass

            # self.kv_migration.show_bloom_filters() # debug use

    def run(self):
        
        try:
            print 'Starting controller.'
            # Thread for receiving digest from data plane
            # t = threading.Thread(target = self.hdl_digest)
            # t.daemon = True
            # t.start()

            signal.signal(signal.SIGINT, sigint_handler)
            self.hdl_digest()

        except Exception as e:
            self.log.exception(e)

if __name__ == '__main__':

    # Parse arguments
    argparser = argparse.ArgumentParser(description='kv_migration controller.')
    argparser.add_argument('--program',
                           type=str,
                           default='kv_migration',
                           help='P4 program name. Default: kv_migration')
    argparser.add_argument(
        '--bfrt-ip',
        type=str,
        default='127.0.0.1',
        help='Name/address of the BFRuntime server. Default: 127.0.0.1')
    argparser.add_argument('--bfrt-port',
                           type=int,
                           default=50052,
                           help='Port of the BFRuntime server. Default: 50052')
    argparser.add_argument(
        '--switch-mac',
        type=str,
        default='6c:ec:5a:3c:5f:af',
        help='MAC address of the switch. Default: 6c:ec:5a:3c:5f:af')
    argparser.add_argument('--switch-ip',
                           type=str,
                           default='10.89.10.67',
                           help='IP address of the switch. Default: 10.89.10.67')
    argparser.add_argument(
        '--ports',
        type=str,
        default='ports.yaml',
        help=
        'YAML file describing machines connected to ports. Default: ports.yaml')
    argparser.add_argument('--log-level',
                           default='INFO',
                           choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
                           help='Default: INFO')

    args = argparser.parse_args()

    # Configure logging
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        sys.exit('Invalid log level: {}'.format(args.log_level))

    logformat = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename='kv_migration.log',
                        filemode='w',
                        level=numeric_level,
                        format=logformat,
                        datefmt='%H:%M:%S')

    args.switch_mac = args.switch_mac.strip().upper()
    args.switch_ip = args.switch_ip.strip()
    args.bfrt_ip = args.bfrt_ip.strip()

    if not mac_address_regex.match(args.switch_mac):
        sys.exit('Invalid Switch MAC address')
    if not validate_ip(args.switch_ip):
        sys.exit('Invalid Switch IP address')
    if not validate_ip(args.bfrt_ip):
        sys.exit('Invalid BFRuntime server IP address')

    ctrl = KVMigrationController()
    ctrl.setup(args.program, args.switch_mac, args.switch_ip, args.bfrt_ip,
               args.bfrt_port, args.ports)

    # Start controller
    ctrl.run()
