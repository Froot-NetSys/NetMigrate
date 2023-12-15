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

class Control(object):

    def __init__(self, target, gc):

        # Get logging, client, and global program info
        self.log = logging.getLogger(__name__)
        self.gc = gc
        self.target = target

        # Child classes must set tables
        self.tables = None

    def _clear(self):
        ''' Remove all existing entries in the tables '''
        if self.tables is not None:
            for table in self.tables:
                table.entry_del(self.target)
                table.default_entry_reset(self.target)