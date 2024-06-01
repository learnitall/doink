"""pytest configuration."""

import time
from typing import Any, List

import pytest
from kafka.protocol.message import Message


class MockKafkaProducer:
    """
    MockKafkaProducer mocks KafkaProducers for unit testing.

    The implementation is sparse, meaning the functionality of
    the KafkaProducer is not fully implemented.

    The mock_messages attribute should be set to a dictionary
    that is alive in an outer scope. This dictionary can then
    be shared with other producers and consumers to simulate
    message passing.
    """

    def __init__(self, *_, **__):
        """
        Create the MockKafkaProducer.

        Initializes an empty dictionary of messages.
        """

        self.mock_messages = {}

    def send(self, *_, **kwargs):
        """
        Mock the send method.

        This isn't super accurate, but will be good enough until the
        usage of the producer in doink becomes more advanced.
        """

        topic = kwargs["topic"]
        if self.mock_messages.get(topic) is None:
            self.mock_messages[topic] = []

        self.mock_messages[topic].append(
            Message(value=kwargs.get("value").encode("utf-8"))
        )

    def flush(self):
        """Mock flush by performing a no-op."""

    def close(self):
        """Mock close by performing a no-op."""


class MockKafkaConsumer:
    """
    MockKafkaConsumer mocks a KafkaConsumer for unit testing.

    The implementation is sparse, meaning the functionality of
    the KafkaProducer is not fully implemented.

    The mock_messages attribute should be set to a dictionary
    that is alive in an outer scope. This dictionary can then
    be shared with other producers and consumers to simulate
    message passing.

    The class implements the interator interface. All remaining
    messages in the mock_messages attribute will be yielded when
    the stop method is called.
    """

    def __init__(self, *_, **__):
        """
        Create the MockKafkaConsumer.

        Initializes with an empty dictionary of messages.
        """

        self.mock_messages = {}
        self.mock_topics = []
        self.mock_alive = True
        self.mock_messages_left = True

    def subscribe(self, *args, **kwargs):
        """Mock the subscribe method."""

        topics = kwargs.get("topics", args[0])
        for topic in topics:
            self.mock_topics.append(topic)

    def close(self):
        """
        Mock the close method.

        For iterators, this causes all remaining messages to
        be consumed from subscribed topics and a StopIteration
        exception to be raised.
        """

        self.mock_alive = False

    def __iter__(self):
        """Mark instances as iterable."""

        return self

    def __next__(self):
        """Return first available message from subscribed topics."""

        while True:
            for topic in self.mock_topics:
                messages: List[Message] = self.mock_messages.get(topic)
                if messages is not None and len(messages) > 0:
                    return messages.pop(0)

            # When we get to this point, we have no more
            # messages to send, because we returned them
            # all. We can either wait for more messages
            # or raise a StopIteration to signal we're out.
            if not self.mock_alive:
                raise StopIteration

            time.sleep(0.1)


def get_class_recorder(registry: dict[str, Any]):
    """
    get_class_recorder returns a class for inspecting other classes.

    The returned class is a base class that performs the following:

    1. Records whenever a method is called. When a method is called,
       the dictionary attribute "calls" is updated with the arguments
       passed.
    2. Saves all instantiations of the class in the given registry
       dictionary.

    This tool can be used in unit tests to handle cases where inspection
    into a class instance may be needed, but the instance is not exposed
    publicly.
    """

    def record_decorator(f):
        def record_call(self, *args, **kwargs):
            self.calls[f.__name__] = {"args": args, "kwargs": kwargs}

            f(self, *args, **kwargs)

        return record_call

    def get_init_recorder(f):
        def init(self, *args, **kwargs):
            f(self, *args, **kwargs)
            registry[self.__class__.__name__] = self

        return init

    class Recorder(type):
        """Record method calls and instantiations (metaclass)."""

        def __new__(mcs, *args, **kwargs):  # noqa: N804
            c = super().__new__(mcs, *args, **kwargs)
            c.__init__ = get_init_recorder(c.__init__)
            c.calls = {}

            for key, value in c.__dict__.items():
                if callable(value):
                    setattr(c, key, record_decorator(value))

            return c

    return Recorder


@pytest.fixture
def mock_kafka(monkeypatch):
    """
    mock_kafka is a pytest fixture for mocking kafka objects.

    Mocked KafkaProducer and KafkaConsumer classes are created
    using the MockKafkaProducer, MockKafkaConsumer and
    get_recorder_class utilities. The fixture returns the internal
    messages
    :param monkeypatch:
    :return:
    """
    registry = dict()
    recorder = get_class_recorder(registry)

    messages = dict()

    class Consumer(MockKafkaConsumer, metaclass=recorder):
        """Recorded consumer with shared messages."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            self.mock_messages = messages

    class Producer(MockKafkaProducer, metaclass=recorder):
        """Recorded producer with shared messages."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            self.mock_messages = messages

    monkeypatch.setattr("doink.kafka.KafkaConsumer", Consumer)
    monkeypatch.setattr("doink.kafka.KafkaProducer", Producer)

    yield messages, registry

    for _, value in registry.items():
        value.close()
