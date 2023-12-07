# States we can be in at any point in time
STATE_PRIMARY = 1          # Primary, waiting for peer to connect
STATE_BACKUP = 2           # Backup, waiting for peer to connect
STATE_ACTIVE = 3           # Active - accepting connections
STATE_PASSIVE = 4          # Passive - not accepting connections

# Events, which start with the states our peer can be in
PEER_PRIMARY = 1           # HA peer is pending primary
PEER_BACKUP = 2            # HA peer is pending backup
PEER_ACTIVE = 3            # HA peer is active
PEER_PASSIVE = 4           # HA peer is passive
CLIENT_REQUEST = 5         # Client makes request

# We send state information every this often
# If peer doesn't respond in two heartbeats, it is 'dead'
HEARTBEAT_BSTAR = 1000          # In msecs

ROUTER_ADDRESS_LOCAL_PEER = "tcp://*:5556"
ROUTER_ADDRESS_REMOTE_PEER = "tcp://localhost:5557"
ROUTER_BACKUP_ADDRESS_LOCAL_PEER = "tcp://*:5557"
ROUTER_BACKUP_ADDRESS_REMOTE_PEER = "tcp://localhost:5556"

class FSMError(Exception):
    """Exception class for invalid state"""
    pass