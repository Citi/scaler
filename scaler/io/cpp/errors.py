from scaler.io.cpp.ffi import ffi, lib, FFITypes

from enum import IntEnum, unique
import os
import signal

def check_status(value: "FFITypes.CData") -> None:
    exception = CppException.from_status(value)
    if exception is not None:
        raise exception

@unique
class Code(IntEnum):
    AlreadyBound = lib.AlreadyBound
    InvalidAddress = lib.InvalidAddress
    UnsupportedOperation = lib.UnsupportedOperation
    NoThreads = lib.NoThreads

class CppException(Exception):
    message: str | None

    def __init__(self, message: str | None):
        self.message = message

    @staticmethod
    def from_status(status: "FFITypes.CData") -> "CppException | None":
        if status.message == ffi.NULL:
            message = None
        else:
            message = ffi.string(status.message).decode("utf-8")

        match status.type:
            case lib.Ok:
                return None
            case lib.Errno:
                return ErrnoException(status.no, message)
            case lib.Logical:
                return LogicalException(Code(status.code), message)
            case lib.Signal:
                return SignalException(status.signal, message)

class LogicalException(CppException):
    code: Code

    def __init__(self, code: Code, message: str | None):
        super().__init__(message)
        self.code = code

    def __str__(self):
        if self.message is None:
            return f"Logical error: Code: {self.code.name} ({self.code.value}"
        return f"Logical error: Code: {self.code.name} ({self.code.value}): {self.message}"

class ErrnoException(CppException):
    errno: int

    def __init__(self, errno: int, message: str | None):
        super().__init__(message)
        self.errno = errno


    def __str__(self):
        if self.message is None:
            return os.strerror(self.errno)
        return f"{self.message}: {os.strerror(self.errno)}"

class SignalException(CppException):
    signal: int

    def __init__(self, signal: int, message: str | None):
        super().__init__(message)
        self.signal = signal

    def __str__(self):
        if self.message is None:
            return f"Signal error: Signal: {self.strsignal} ({self.signame})"
        return f"Signal error: Signal: {self.strsignal} ({self.signame}): {self.message}"

    @property
    def signame(self) -> str:
        """e.g. SIGINT"""
        return signal.Signals(self.signal).name

    @property
    def strsignal(self) -> str:
        """e.g. Interrupted"""
        return signal.strsignal(self.signal)
