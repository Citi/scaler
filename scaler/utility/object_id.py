import dataclasses
import hashlib
import uuid

SERIALIZER_TAG = hashlib.md5(b"serializer").digest()


@dataclasses.dataclass
class ObjectID:
    """
    Scaler 32-bytes object IDs.

    Object ID are built from 2x16-bytes parts:

    - the first 16-bytes uniquely identify the owner of the object (i.e. the Scaler client's hash);
    - the second 16-bytes uniquely identify the object's content.
    """

    _bytes: bytes

    def __init__(self, _bytes: bytes):
        if len(_bytes) != 32:
            raise ValueError("Scaler object ID must be 32 bytes.")

        self._bytes = _bytes

    @staticmethod
    def generate_unique_object_id(owner: bytes):
        owner_hash = hashlib.md5(owner).digest()
        unique_object_tag = uuid.uuid4().bytes
        return ObjectID(owner_hash + unique_object_tag)

    @staticmethod
    def generate_serializer_object_id(owner: bytes):
        owner_hash = hashlib.md5(owner).digest()
        return ObjectID(owner_hash + SERIALIZER_TAG)

    def owner_hash(self) -> bytes:
        return self._bytes[:16]

    def object_tag(self) -> bytes:
        return self._bytes[16:]

    def bytes(self) -> bytes:
        return self._bytes

    def is_serializer(self) -> bool:
        return self.object_tag() == SERIALIZER_TAG

    def __repr__(self) -> str:
        return f"ObjectID(owner_hash={self.owner_hash().hex()}, object_tag={self.object_tag().hex()}, is_serializer={self.is_serializer()})"

    def __hash__(self) -> int:
        return self._bytes.__hash__()

    def __eq__(self, other):
        return self._bytes.__eq__(other._bytes)
