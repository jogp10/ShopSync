# Auxiliary functions for the main script
# enum for request types
import time
import uuid
from enum import Enum, IntEnum, StrEnum

ROUTER_ADDRESS = "tcp://localhost:5554"
ROUTER_BIND_ADDRESS = "tcp://*:5554"
R_QUORUM = 2
W_QUORUM = 3
N = 4
TIMEOUT_THRESHOLD = 500
MONITOR_INTERVAL = 30


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


def build_get_request(key, quorum_id=''):
    """given a key, return a json for a get request of that key"""
    return {
        "type": MessageType.GET,
        "key": key,
        "quorum_id": quorum_id
    }


def build_quorum_get_request(key, quorum_id, replicas=[]):
    """given a key, return a json for a get request of that key"""
    return {
        "type": MessageType.COORDINATE_GET,
        "key": key,
        "quorum_id": quorum_id,
        "replicas": replicas  # todo remove once nodes have their own hashring
    }


def build_put_request(key, value, quorum_id=''):
    """given a key and a value, return a json for a put request of that key and value"""
    return {
        "type": MessageType.PUT,
        "key": key,
        "value": value,
        "quorum_id": quorum_id
    }


def build_quorum_put_request(key, value, quorum_id, replicas=[]):
    """given a key and a value, return a json for a put request of that key and value"""
    return {
        "type": MessageType.COORDINATE_PUT,
        "key": key,
        "value": value,
        "quorum_id": quorum_id,
        "replicas": replicas  # todo remove once nodes have their own hashring
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


def build_quorum_delete_request(key, quorum_id, replicas=[]):
    return {
        "type": MessageType.COORDINATE_DELETE,
        "key": key,
        "quorum_id": quorum_id,
        "replicas": replicas  # todo remove once nodes have their own hashring
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
        "quorum_size": quorum_size
    }


def build_register_request(address):
    """given an address, return a json for a register request of that address"""
    return {
        "type": MessageType.REGISTER,
        "address": address
    }

def build_register_response(message):
    """given an address, return a json for a register request of that address"""
    return {
        "type": MessageType.REGISTER_RESPONSE,
        "message": message
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


