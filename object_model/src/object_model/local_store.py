
from pathlib import Path


class LocalStore:
    def __init__(self, root: Path):
        self.__root = root
