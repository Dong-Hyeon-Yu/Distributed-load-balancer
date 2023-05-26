import secrets
import hashlib

default_hash_size = 32


def _get_random_hash():
    return secrets.token_hex(default_hash_size)


def _get_random_data(bytes_size: int):
    return secrets.token_bytes(bytes_size)


class Transaction:

    def __init__(self, data=None, _hash=None, **kwargs):
        if data is None and _hash is None:
            self.data = kwargs.get('data')
            self.hash = kwargs.get('hash')
        else:
            self.data = data
            self.hash = hashlib.sha3_256(str(data).encode('utf-8')).digest() if _hash is None else _hash

    def __hash__(self):
        return int.from_bytes(self.hash, "big")

    @classmethod
    def dummy(cls, length: int):
        return Transaction(
            _get_random_data(length - default_hash_size)
        )

    @classmethod
    def from_json(cls, data):
        return Transaction(data["data"], data["hash"])
