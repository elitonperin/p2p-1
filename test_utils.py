import utils


def test_serialization():
    input = {}
    serialized = utils.serialize(input)
    deserialized = utils.deserialize(serialized)
    assert deserialized == input
