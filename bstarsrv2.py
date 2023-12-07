"""
Binary Star server, using bstar reactor

Author: Min RK <benjaminrk@gmail.com>
"""

import sys

import zmq

from bstar import BinaryStar

from server import *
from bstar_utils import *



def main():
    # Arguments can be either of:
    #     -p  primary server, at tcp://localhost:5001
    #     -b  backup server, at tcp://localhost:5002
    if '-p' in sys.argv:
        hash_table = Router(address=ROUTER_BIND_ADDRESS, replica_count=24)
        star = BinaryStar(True, ROUTER_ADDRESS_LOCAL_PEER, ROUTER_ADDRESS_REMOTE_PEER, hash_table)
        star.register_voter()
    elif '-b' in sys.argv:
        hash_table = Router(address=ROUTER_BACKUP_BIND_ADDRESS, replica_count=24)
        star = BinaryStar(False, ROUTER_BACKUP_ADDRESS_LOCAL_PEER, ROUTER_BACKUP_ADDRESS_REMOTE_PEER, hash_table)
        star.register_voter()
    else:
        print("Usage: bstarsrv2.py { -p | -b }\n")
        return

    star.start()

if __name__ == '__main__':
    main()