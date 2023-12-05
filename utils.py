# Auxiliary functions for the main script
# enum for request types
import time
import uuid
from enum import Enum, IntEnum


ROUTER_ADDRESS = "tcp://localhost:5554"
ROUTER_BIND_ADDRESS = "tcp://*:5554"
TIMEOUT_THRESHOLD = 500

class MessageType(IntEnum):
    GET = 1
    PUT = 2
    DELETE = 3
    GET_RESPONSE = 4
    PUT_RESPONSE = 5
    DELETE_RESPONSE = 6
    REGISTER = 7
    REGISTER_RESPONSE = 8
    HEARTBEAT = 9
    HEARTBEAT_RESPONSE = 10


# class QuorumState:
#     def __init__(self):
#         self.retries = 0




def build_get_request(key):
    """given a key, return a json for a get request of that key"""
    return {
        "type": MessageType.GET,
        "key": key
    }


def build_quorum_get_request(key, quorum_id):
    """given a key, return a json for a get request of that key"""
    return {
        "type": MessageType.GET,
        "key": key,
        "quorum_id": quorum_id
    }


def build_put_request(key, value):
    """given a key and a value, return a json for a put request of that key and value"""
    return {
        "type": MessageType.PUT,
        "key": key,
        "value": value,
    }


def build_quorum_put_request(key, value, quorum_id, replicas):
    """given a key and a value, return a json for a put request of that key and value"""
    return {
        "type": MessageType.PUT,
        "key": key,
        "value": value,
        "quorum_id": quorum_id,
        "replicas": replicas
    }

def build_delete_request(key):
    return {
        "type": MessageType.DELETE,
        "key": key
    }

def build_quorum_delete_request(key, quorum_id):
    return {
        "type": MessageType.DELETE,
        "key": key,
        "quorum_id": quorum_id
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
