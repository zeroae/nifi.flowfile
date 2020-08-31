from copy import deepcopy
from typing import Dict

from nifi.flowfile.attributes import CoreAttributes


class FlowFile(object):
    _attributes = None
    _content = b""

    def __init__(self, attributes, content):
        self._attributes = attributes
        self._content = content

    def __getitem__(self, item):
        return self.get_attribute(item)

    def get_attribute(self, item):
        return self._attributes.get(item)

    def get_attributes(self) -> Dict[str, str]:
        return self._attributes

    def put_attribute(self, key: str, value: str):
        return self.put_all_attributes(**{key: value})

    def put_all_attributes(self, **kwargs):
        new_attributes = deepcopy(self._attributes)
        new_attributes.update(kwargs)
        new_attributes.update(CoreAttributes.create_default_attributes())
        return FlowFile(new_attributes, self._content)

    def del_attribute(self, key):
        return self.del_all_attributes(keys={key})

    def del_all_attributes(self, keys: set):
        new_attributes: dict = deepcopy(self._attributes)
        for key in keys & new_attributes.keys():
            del new_attributes[key]
        new_attributes.update(CoreAttributes.create_default_attributes())
        return FlowFile(new_attributes, self._content)
