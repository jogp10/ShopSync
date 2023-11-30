# Auxiliary functions for the main script

def build_get_request(key):
    """given a key, return a json for a get request of that key"""
    return {
        "type": "get",
        "key": key
    }


def build_put_request(key, value):
    """given a key and a value, return a json for a put request of that key and value"""
    return {
        "type": "put",
        "key": key,
        "value": value
    }


def get_request_type(request):
    """given a request, return the type of the request"""
    return request["type"].lower()

