from abc import abstractmethod


class ObjectStoreError(Exception):
    def __init__(self, message: str = None):
        super().__init__(message or self.message())

    @classmethod
    @abstractmethod
    def message(cls) -> str:
        ...


class FailedUpdateError(ObjectStoreError):
    @classmethod
    def message(cls) -> str:
        return r"""
            An attempt was made to re-write an object with an existing version number.
            This can occur if:
                1. You attempted to save an existing object without loading it first
                2. Someone else has saved this object since you loaded it
                3. You are trying to update an immutable object
        """


class NotFoundError(ObjectStoreError):
    @classmethod
    def message(cls) -> str:
        return "No object found"


class WrongStoreError(ObjectStoreError):
    def __init__(self, object_type: str, object_id: bytes):
        message = f"""Attempting to save object {object_type}:{object_id.decode()}
                      in a different store to the one from which it was loaded"""
        super().__init__(message)

    @classmethod
    def message(cls) -> str:
        return ""


