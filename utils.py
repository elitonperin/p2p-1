import json


def serialize(input):
    """JSON-serialize and encode as binary"""
    return json.dumps(input).encode()


def deserialize(input):
    """Decode from binary and JSON-deserialize"""
    return json.loads(input.decode())
