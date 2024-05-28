"""Utilities for producing and consuming object state changes from Kafka."""

import uuid

from .kafka import KafkaHandler, ObjectUpdate


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
