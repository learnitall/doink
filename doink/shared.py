"""Utilities for producing and consuming object state changes from Kafka."""

import uuid

from .kafka import KafkaHandler
from .state import ObjectUpdate


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

    def __init__(self, kafka_handler: KafkaHandler, topic: str, obj_uid: str):
        """Create the shared object and register the callback."""

        self._shared_kafka_handler = kafka_handler
        self._shared_obj_uid = obj_uid
        self._shared_kafka_topic = topic
        # TODO: Although uuid4 is a very large space, a conflict is
        #  going to happen.
        self._shared_my_uid = str(uuid.uuid4())

        self._shared_kafka_handler.register(
            self._shared_obj_uid,
            self._shared_kafka_callback,
        )

        existing = kafka_handler.get(obj_uid)
        if existing is not None:
            for key, value in existing.items():
                self.__dict__[key] = value

    def _shared_kafka_callback(self, update: ObjectUpdate):
        """
        _shared_kafka_callback is the registered kafka callback.

        Updates received which match the instance's owner ID are
        ignored.
        """

        if not isinstance(update.value, dict):
            raise ValueError(
                f"Received bad update value for shared object: {update}"
            )

        for key, value in update.value.items():
            super().__setattr__(key, value)

    def __setattr__(self, key, value):
        """Pushes updates to shared attributes to KafkaHandler."""
        if not key.startswith("_"):
            self._shared_kafka_handler.update(
                self._shared_my_uid, self._shared_obj_uid, {key: value}
            )

            return

        self.__dict__[key] = value
