"""Contains helpers and types for sharing and storing object state."""

import json
from dataclasses import asdict, dataclass


def dataclass_from_json(cls, j: str | bytes):
    """
    dataclass_from_json is a deserialization helper utility.

    JSON data can be given via j, which will be sent through
    json.loads. The resulting dict will be passed as kwargs to
    the given dataclass class.

    If the given JSON data does not represent a dict, a TypeError
    is raised.

    :param cls: The dataclass class to load the json data into.
    :param j: The JSON input to deserialize.
    :return: An instance of cls.
    """

    d = json.loads(j)
    if not isinstance(d, dict):
        raise TypeError(
            "Loaded JSON data is not a dict, unable to convert to "
            f"dataclass {cls.__class__.__name__}: {j!r}",
        )

    return cls(**d)


def dataclass_instance_to_json(s) -> str:
    """
    dataclass_to_json is a serialization helper utility.

    This function serializes the given dataclass instance into a
    JSON object. This is performed by converting the dataclass into
    a dict and then passing it through json.dumps.

    :param s: Dataclass instance to serialize.
    :return: str
    """

    return json.dumps(asdict(s))


@dataclass
class ObjectUpdate:
    """
    ObjectUpdate is a serializable container for update messages.

    Value must be a dict of strings mapped to JSON encodeable values.
    """

    obj_uid: str
    owner_uid: str
    value: dict[str, list | tuple | str | int | float | bool | None]

    def __setattr__(self, key, value):
        """__setattr__ performs input validation on the field 'value'."""

        if key == "value":
            if not isinstance(value, dict):
                raise TypeError("value must be a dict")

            for k, v in value.items():
                if not isinstance(k, str):
                    raise TypeError(f"Keys in value must be strings: {k}")

                if v is None:
                    continue

                valid = False
                for t in (list, tuple, str, int, float, bool):
                    if isinstance(v, t):
                        valid = True
                        break

                if not valid:
                    raise TypeError(
                        f"Value in dict is of unsupported type: {v}"
                    )

        self.__dict__[key] = value

    @classmethod
    def from_json(cls, j: str | bytes) -> "ObjectUpdate":
        """
        Create a new ObjectUpdate from the given JSON string.

        May raise a TypeError if deserialization is unable to be
        completed.
        """
        return dataclass_from_json(cls, j)

    def to_json(self) -> str:
        """Serialize the ObjectUpdate instance to a JSON string."""
        return dataclass_instance_to_json(self)
