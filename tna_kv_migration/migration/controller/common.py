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


import re
import socket
from enum import IntEnum

# From configuration.p4
max_num_queue_pairs_per_worker = 512

# Regexes
mac_address_regex = re.compile(':'.join(['[0-9a-fA-F]{2}'] * 6))
front_panel_regex = re.compile('([0-9]+)/([0-9]+)$')


# IPv4 validation
def validate_ip(value):
    ''' Validate IP address string '''
    try:
        socket.inet_aton(value)
        return True
    except:
        return False


# Enums
class ClientReqOp(IntEnum):
    MG_READ                    = 0x01
    MG_WRITE                   = 0x02
    MG_DELETE                  = 0x03
    MG_READ_REPLY              = 0x04
    MG_WRITE_REPLY             = 0x05
    MG_DELETE_REPLY            = 0x06

    MG_MULTI_READ              = 0x07
    MG_MULTI_WRITE             = 0x08
    MG_MULTI_DELETE            = 0x09
    MG_MULTI_READ_REPLY        = 0x0A
    MG_MULTI_WRITE_REPLY       = 0x0B
    MG_MULTI_DELETE_REPLY      = 0x0C


class MigrationOp(IntEnum):
    MG_INIT                         =  0x10
    MG_INIT_REPLY                   =  0x11
    MG_MIGRATE                      =  0x12
    MG_MIGRATE_REPLY                =  0x13
    MG_TERMINATE                    =  0x14
    MG_TERMINATE_REPLY              =  0x15

    MG_MIGRATE_GROUP_START          = 0x16
    MG_MIGRATE_GROUP_START_REPLY    = 0x17
    MG_MIGRATE_GROUP_COMPLETE       = 0x18  # Source send this reply when a group finishes migration
    MG_MIGRATE_GROUP_COMPLETE_REPLY = 0x19

class Constants(IntEnum):
    MG_PORT = 48879 # usable: 48864-48879

class MigrationParameters(IntEnum):
    MG_MAX_MIGRATION_PAIR_ENTRIES  = 128
    MG_MAX_BLOOM_FILTER_ENTRIES    = 262144  
    MG_BLOOM_FILTER_WIDTH          = 18
    MG_COUNTING_BLOOM_FILTER_WIDTH = 16
    MG_COUNTING_BLOOM_FILTER_ENTRIES = 65536
    MG_GROUP_ID_WIDTH              = 32 # this is only for group_id
    MG_REPLY_FILTER_SIZE           = 65536