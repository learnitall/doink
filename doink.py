"""Utilities for producing and consuming object state changes from Kafka."""

import json
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from threading import Thread
from typing import Any, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.protocol.message import Message


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


def _get_class_name(obj: Any) -> str:
    """
    get_class_name returns the class of the given object.

    This is an alias for obj.__class__.__name__. It can be expanded
    in the future as needed.
    """
    return obj.__class__.__name__


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


class KafkaHandler:
    """
    KafkaHandler is the interface objects consume to work with Kafka.

    The handler manages consuming and producing ObjectUpdate messages
    from and to Kafka.
    """

    def __init__(self, topic: str, **config):
        """
        Create the KafkaHandler.

        All kwargs are passed to the Kafka consumer and producer classes.

        :param str topic: Topic to listen to updates on.
        """

        self._topic = topic
        self._config = config
        self._consumer: Optional[KafkaConsumer] = None
        self._producer: Optional[KafkaProducer] = None
        self._background_updater: Optional[Thread] = None
        self._callbacks: dict[str, List[Callable[[ObjectUpdate], None]]] = (
            dict()
        )

    def _consume_messages(self):
        """
        Consume messages in Kafka, from the configured consumer.

        If the consumer is None, this method fails. This is purposeful, as
        this method should only be called when the consumer is available.
        """
        for msg in self._consumer:
            if msg is not None:
                self.handle_new_message(msg)

    def handle_new_message(self, message: Message):
        """Deserialize the message's value and pass it to callbacks."""

        update = ObjectUpdate.from_json(message.value.decode("utf-8"))

        callbacks = self._callbacks.get(update.obj_uid, None)
        if callbacks is not None:
            for callback in callbacks:
                callback(update)

    def start(self):
        """
        Start kicks off the KafkaHandler.

        It creates the Consumer, Producer and a thread for calling
        callbacks.
        """

        self.stop()

        self._consumer = KafkaConsumer(**self._config)
        self._producer = KafkaProducer(**self._config)

        self._consumer.subscribe([self._topic])

        self._background_updater = Thread(target=self._consume_messages)
        self._background_updater.start()

    def stop(self):
        """Stop the KafkaHandler and turn it idle."""
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()

        if self._consumer is not None:
            self._consumer.close()

        if self._background_updater is not None:
            self._background_updater.join()

    def register(self, obj_uid: str, callback: Callable[[ObjectUpdate], None]):
        """
        Register the given callback for the given object UID.

        When a message is received which is relevant to the object UID,
        it will be passed to the callback.
        """

        if self._callbacks.get(obj_uid) is None:
            self._callbacks[obj_uid] = []

        self._callbacks[obj_uid].append(callback)

    def update(self, owner_uid: str, obj_uid: str, obj: Any):
        """
        Broadcast an object update to Kafka for the given object.

        It is up to consumers to interpret how to merge the passed
        in object with an existing counter-part.
        """

        msg = ObjectUpdate(
            owner_uid=owner_uid, obj_uid=obj_uid, value=obj
        ).to_json()
        self._producer.send(topic=self._topic, value=msg)


class SharedObject:
    """
    SharedObject is a base class for creating a Kafka-synced object.

    Updates to attributes not starting with an underscore are
    broadcast using the given KafkaHandler. A callback is registered
    with the KafkaHandler to listen for updates to attributes and apply
    them transparently.

    Each object is identified using a unique identifier and each
    individual instance of an object is identified with a unique
    identifier generated during instantiation.
    """

    def __init__(self, handler: KafkaHandler, topic: str, obj_uid: str):
        """Create the shared object and register the callback."""

        self._kafka_handler = handler
        self._kafka_obj_uid = obj_uid
        self._kafka_topic = topic

        # TODO: Although uuid4 is a very large space, a conflict is
        #  going to happen.
        self._kafka_my_uid = str(uuid.uuid4())

        self._kafka_handler.register(
            self._kafka_obj_uid,
            self._kafka_callback,
        )

    def _kafka_grab_all_shared_attrs(self) -> dict:
        """Return dict with attributes not starting with an underscore."""
        result = dict()

        for key, value in self.__dict__.items():
            if not key.startswith("_"):
                result[key] = value

        return result

    def _kafka_callback(self, update: ObjectUpdate):
        """
        _kafka_callback is the registered kafka callback.

        Updates received which match the instance's owner ID are
        ignored.
        """
        if not isinstance(update.value, dict):
            raise ValueError(
                f"Received bad update value for shared object: {update}"
            )

        if update.owner_uid == self._kafka_my_uid:
            return

        for key, value in update.value.items():
            super().__setattr__(key, value)

    def _kafka_push_obj(self):
        """
        Push an update containing all shared attributes.

        Used for initializing the state of a new object.
        """

        self._kafka_handler.update(
            self._kafka_my_uid,
            self._kafka_obj_uid,
            self._kafka_grab_all_shared_attrs(),
        )

    def __setattr__(self, key, value):
        """Pushes updates to shared attributes to KafkaHandler."""
        if not key.startswith("_"):
            self._kafka_handler.update(
                self._kafka_my_uid, self._kafka_obj_uid, {key: value}
            )

        self.__dict__[key] = value
