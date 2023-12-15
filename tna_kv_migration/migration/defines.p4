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
#define _MG_MAX_BLOOM_FILTER_ENTRIES    1048576  
#define _MG_BLOOM_FILTER_WIDTH          20
#define _MG_COUNTING_BLOOM_FILTER_WIDTH 18
#define _MG_COUNTING_BLOOM_FILTER_ENTRIES 262144 
#define _MG_GROUP_ID_WIDTH              32
#define _MG_REPLY_FILTER_WIDTH          32
#define _MG_REPLY_FILTER_ENTRIES        32w65536


#define UDP_MG_PORT_BASE               0xbe00
#define UDP_MG_PORT_MASK               0xff00
#define RECIRC_PORT                    68
