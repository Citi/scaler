import dataclasses


@dataclasses.dataclass
class ObjectStorageConfig:
    host: str
    port: int

    def to_string(self) -> str:
        return f"{self.host}:{self.port}"

    @staticmethod
    def from_string(address: str) -> "ObjectStorageConfig":
        if ":" not in address:
            raise ValueError("object storage address has to be <host>:<port>")

        host, port = address.split(":", 1)

        return ObjectStorageConfig(host=host, port=int(port))
