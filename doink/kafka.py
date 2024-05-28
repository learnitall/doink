"""Utilities for publishing object updates to Kafka."""

from collections.abc import Callable
from threading import Thread
from typing import Any, List, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.protocol.message import Message

from .state import ObjectUpdate


def _get_class_name(obj: Any) -> str:
    """
    get_class_name returns the class of the given object.

    This is an alias for obj.__class__.__name__. It can be expanded
    in the future as needed.
    """
    return obj.__class__.__name__


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
