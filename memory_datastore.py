from datastore import Datastore


class MemoryDatastore(Datastore):
    _dict: dict

    def __init__(self):
        self._dict = {}

    def set(self, key, value):
        self._dict[key] = value

    def get(self, key):
        return self._dict[key]

    def delete(self, key):
        return self._dict.pop(key)

    def get_all_keys(self):
        return self._dict.keys()
