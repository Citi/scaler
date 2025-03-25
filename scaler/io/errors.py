from scaler.io.cpp.ffi import ffi, lib, FFITypes

from enum import IntEnum, unique
import os

def check_status(value: "FFITypes.CData") -> None:
    exception = CppException.from_value(value)
    if exception is not None:
        raise exception
    
class Code(IntEnum):
    AlreadyBound = lib.AlreadyBound
    InvalidAddress = lib.InvalidAddress

class CppException(Exception):
    message: str | None

    def __init__(self, message: str | None):
        self.message = message

    @staticmethod
    def from_value(value: "FFITypes.CData") -> "CppException | None":
        if value.message == ffi.NULL:
            message = None
        else:
            message = ffi.string(value.message).decode("utf-8")

        match value.type:
            case lib.Ok:
                return None
            case lib.Errno:
                return ErrnoException(value.no, message)
            case lib.Logical:
                return LogicalException(Code(value.code), message)

class LogicalException(CppException):
    code: Code

    def __init__(self, code: Code, message: str | None):
        super().__init__(message)
        self.code = code

    def __str__(self):
        if self.message is None:
            return f"Logical error: {ffi.string(self.code).decode('utf-8')}"
        return f"{self.message}"

class ErrnoException(CppException):
    errno: int

    def __init__(self, errno: int, message: str | None):
        super().__init__(message)
        self.errno = errno


    def __str__(self):
        if self.message is None:
            return os.strerror(self.errno)
        return f"{self.message}: {os.strerror(self.errno)}"
