from abc import ABC, abstractmethod


class DBError(Exception, ABC):
    def __init__(self, message: str = None):
        super().__init__(message or self.message())

    @classmethod
    @abstractmethod
    def message(cls) -> str:
        ...


class DBDuplicateWriteError(DBError):
    @classmethod
    def message(cls) -> str:
        return "Attempt write an object multiple times in the same transaction"


class DBFailedUpdateError(DBError):
    @classmethod
    def message(cls) -> str:
        return r"""
            An attempt was made to re-write an object with an existing version number.
            This can occur if:
                1. You attempted to save an existing object without loading it first
                2. Someone else has saved this object since you loaded it
                3. You are trying to update an immutable object
        """


class DBNotFoundError(DBError):
    @classmethod
    def message(cls) -> str:
        return "No object found"


class DBUnknownError(DBError):
    def __init__(self, native_exception: Exception):
        super().__init__(f"{self.message()} - native exception was {native_exception}")

    @classmethod
    def message(cls) -> str:
        return f"Unknown error"

