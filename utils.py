# Auxiliary functions for the main script
# enum for request types
import time
import uuid
from enum import Enum, IntEnum, StrEnum

ROUTER_ADDRESS = "tcp://localhost:5554"
ROUTER_BIND_ADDRESS = "tcp://*:5554"
ROUTER_BACKUP_ADDRESS = "tcp://localhost:5555"
ROUTER_BACKUP_BIND_ADDRESS = "tcp://*:5555"
NUM_ROUTERS = 2
R_QUORUM = 2
W_QUORUM = 3
N = 4
TIMEOUT_THRESHOLD = 500
MONITOR_INTERVAL = 30
MIN_TIME_BETWEEN_RETRIES = 1
HEALTH_CHECK_TIMEOUT = 0.15
COORDINATOR_HEALTH_CHECK_TIMEOUT = 0.3
REPLICA_COUNT = 24

class MessageType(StrEnum):
    GET = 'GET'
    PUT = 'PUT'
    DELETE = 'DELETE'
    GET_RESPONSE = 'GET_RESPONSE'
    PUT_RESPONSE = 'PUT_RESPONSE'
    DELETE_RESPONSE = 'DELETE_RESPONSE'
    REGISTER = 'REGISTER'
    REGISTER_RESPONSE = 'REGISTER_RESPONSE'
    HEARTBEAT = 'HEARTBEAT'
    HEARTBEAT_RESPONSE = 'HEARTBEAT_RESPONSE'
    COORDINATE_PUT = 'COORDINATE_PUT'
    COORDINATE_PUT_RESPONSE = 'COORDINATE_PUT_RESPONSE'
    COORDINATE_GET = 'COORDINATE_GET'
    COORDINATE_GET_RESPONSE = 'COORDINATE_GET_RESPONSE'
    COORDINATE_DELETE = 'COORDINATE_DELETE'
    COORDINATE_DELETE_RESPONSE = 'COORDINATE_DELETE_RESPONSE'
    HINTED_HANDOFF = 'HINTED_HANDOFF'
    HINTED_HANDOFF_RESPONSE = 'HINTED_HANDOFF_RESPONSE'
    HEALTH_CHECK = 'HEALTH_CHECK'
    HEALTH_CHECK_RESPONSE = 'HEALTH_CHECK_RESPONSE'
    ADD_NODE = 'ADD_NODE'
    REMOVE_NODE = 'REMOVE_NODE'
    WRITE_HINT = 'WRITE_HINT'
    WRITE_HINT_RESPONSE = 'WRITE_HINT_RESPONSE'
    DELETE_HINT = 'DELETE_HINT'
    DELETE_HINT_RESPONSE = 'DELETE_HINT_RESPONSE'
    PUT_HANDED_OFF = 'PUT_HANDED_OFF'
    PUT_HANDED_OFF_RESPONSE = 'PUT_HANDED_OFF_RESPONSE'
    DELETE_HANDED_OFF = 'DELETE_HANDED_OFF'
    DELETE_HANDED_OFF_RESPONSE = 'DELETE_HANDED_OFF_RESPONSE'


def build_get_request(key, quorum_id=''):
    """given a key, return a json for a get request of that key"""
    return {
        "type": MessageType.GET,
        "key": key,
        "quorum_id": quorum_id
    }


def build_quorum_get_request(key, quorum_id):
    """given a key, return a json for a get request of that key"""
    return {
        "type": MessageType.COORDINATE_GET,
        "key": key,
        "quorum_id": quorum_id,
    }


def build_put_request(key, value, quorum_id=''):
    """given a key and a value, return a json for a put request of that key and value"""
    return {
        "type": MessageType.PUT,
        "key": key,
        "value": value,
        "quorum_id": quorum_id
    }


def build_quorum_put_request(key, value, quorum_id):
    """given a key and a value, return a json for a put request of that key and value"""
    return {
        "type": MessageType.COORDINATE_PUT,
        "key": key,
        "value": value,
        "quorum_id": quorum_id,
    }


def build_quorum_put_response(quorum_id, result):
    """given the quorum id and the results, return a json for a put request response"""
    return {
        "type": MessageType.COORDINATE_PUT_RESPONSE,
        "result": result,
        "quorum_id": quorum_id
    }


def build_quorum_get_response(quorum_id, result):
    """given the quorum id and the results, return a json for a put request response"""
    return {
        "type": MessageType.COORDINATE_GET_RESPONSE,
        "result": result,
        "quorum_id": quorum_id
    }


def build_quorum_delete_response(quorum_id, result):
    """given the quorum id and the results, return a json for a put request response"""
    return {
        "type": MessageType.COORDINATE_DELETE_RESPONSE,
        "result": result,
        "quorum_id": quorum_id
    }


def build_delete_request(key, quorum_id=''):
    return {
        "type": MessageType.DELETE,
        "key": key,
        "quorum_id": quorum_id

    }


def build_quorum_delete_request(key, quorum_id):
    return {
        "type": MessageType.COORDINATE_DELETE,
        "key": key,
        "quorum_id": quorum_id,
    }


def get_request_type(request):
    """given a request, return the type of the request"""
    return request["type"]


def build_quorum_request_state(nodes, timeout, max_retries, quorum_size):
    """given a list of nodes, return a dictionary with the state of the request"""
    return {
        # "id": f"{uuid.uuid4()}",
        "nodes_with_reply": set(),
        "retry_info": {node: 0 for node in nodes},
        "responses": [],
        "timeout": timeout,
        "max_retries": max_retries,
        "quorum_size": quorum_size,
        "last_retry_time": {node: 0 for node in nodes}
    }


def build_register_request(address):
    """given an address, return a json for a register request of that address"""
    return {
        "type": MessageType.REGISTER,
        "address": address
    }

def build_register_response(nodes):
    """given an address, return a json for a register request of that address"""
    return {
        "type": MessageType.REGISTER_RESPONSE,
        "nodes": nodes
    }

def build_add_node_request(node):
    """given an node, return a json for a register request of that node"""
    return {
        "type": MessageType.ADD_NODE,
        "node": node
    }

def build_remove_node_request(node):
    """given an address, return a json for a register request of that address"""
    return {
        "type": MessageType.REMOVE_NODE,
        "node": node
    }



def build_heartbeat_request():
    """return a json for a heartbeat request"""
    return {
        "type": MessageType.HEARTBEAT,
    }

def build_heartbeat_response():
    """return a json for a heartbeat response"""
    return {
        "type": MessageType.HEARTBEAT_RESPONSE,
    }


def build_write_hint_request(key, failed_node):
    """return a json for a write hint request"""
    return {
        "type": MessageType.WRITE_HINT,
        "key": key,
        "node": failed_node
    }

def build_delete_hint_request(key, failed_node):
    """return a json for a write hint request"""
    return {
        "type": MessageType.DELETE_HINT,
        "key": key,
        "node": failed_node
    }

def build_put_handed_off_request(key, value):
    """return a json for a write hint request"""
    return {
        "type": MessageType.PUT_HANDED_OFF,
        "key": key,
        "value": value
    }

def build_delete_handed_off_request(key):
    """return a json for a write hint request"""
    return {
        "type": MessageType.DELETE_HANDED_OFF,
        "key": key
    }


def get_quorum_value(values, quorum_size):
    """given a list values and a quorum size, return the value that appears in at least quorum_size values"""
    for value in values:
        if values.count(value) >= quorum_size:
            return value
    return None


def get_most_common_value(values):
    """given a list of values, return the value that appears the most times"""
    return max(set(values), key=values.count)


def valid_heartbeat(node_activity):
    return time.time() - node_activity['last_time_active'] < TIMEOUT_THRESHOLD


def valid_health_check(node_activity):
    return node_activity['immediately_available']

