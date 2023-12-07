import time

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
HEARTBEAT_BSTAR = 2000          # In msecs

ROUTER_ADDRESS_LOCAL_PEER = "tcp://*:5556"
ROUTER_ADDRESS_REMOTE_PEER = "tcp://localhost:5557"
ROUTER_BACKUP_ADDRESS_LOCAL_PEER = "tcp://*:5557"
ROUTER_BACKUP_ADDRESS_REMOTE_PEER = "tcp://localhost:5556"

class BStarState(object):
    def __init__(self, state, event, peer_expiry):
        self.state = state
        self.event = event
        self.peer_expiry = peer_expiry

class BStarException(Exception):
    pass

fsm_states = {
        STATE_PRIMARY: {
            PEER_BACKUP: ("I: connected to backup (slave), ready as master",
                        STATE_ACTIVE),
            PEER_ACTIVE: ("I: connected to backup (master), ready as slave",
                        STATE_PASSIVE)
            },
        STATE_BACKUP: {
            PEER_ACTIVE: ("I: connected to primary (master), ready as slave",
                        STATE_PASSIVE),
            CLIENT_REQUEST: ("", False)
            },
        STATE_ACTIVE: {
            PEER_ACTIVE: ("E: fatal error - dual masters, aborting", False)
            },
        STATE_PASSIVE: {
            PEER_PRIMARY: ("I: primary (slave) is restarting, ready as master",
                        STATE_ACTIVE),
            PEER_BACKUP: ("I: backup (slave) is restarting, ready as master",
                        STATE_ACTIVE),
            PEER_PASSIVE: ("E: fatal error - dual slaves, aborting", False),
            CLIENT_REQUEST: (CLIENT_REQUEST, True)  # Say true, check peer later
            }
        }

def run_fsm(fsm):
    # There are some transitional states we do not want to handle
    state_dict = fsm_states.get(fsm.state, {})
    res = state_dict.get(fsm.event)
    if res:
        msg, state = res
    else:
        return
    if state is False:
        raise BStarException(msg)
    elif msg == CLIENT_REQUEST:
        assert fsm.peer_expiry > 0
        if int(time.time() * 1000) > fsm.peer_expiry:
            fsm.state = STATE_ACTIVE
        else:
            raise BStarException()
    else:
        print(msg)
        fsm.state = state