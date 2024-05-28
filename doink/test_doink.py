"""Test functionality of stuff in doink.py."""

import json
import time
import uuid
from typing import Any, List

import pytest
from kafka.protocol.message import Message

from . import doink


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
                    return messages.pop()

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

    monkeypatch.setattr(doink, "KafkaConsumer", Consumer)
    monkeypatch.setattr(doink, "KafkaProducer", Producer)

    yield messages, registry

    for _, value in registry.items():
        value.close()


class TestObjectUpdate:
    """Test functionality of the ObjectUpdate class."""

    @staticmethod
    @pytest.fixture
    def obj_test_values():
        """
        Return dict of values to test serialization.

        The returned values are meant to be attached to the
        ObjectUpdate's value field. Items under the 'good' key will
        have no problem being serialized and deserialized. Items
        under the 'bad' key should raise an error.
        """

        primitives = ["str", 0, 1.0, [1], (3,), True, False, None]
        good_values = [{"str": v for v in primitives}]

        bad_values = (
            primitives
            + [{v: "str" for v in primitives if getattr(v, "__hash__", False)}]
            + [{"str": v for v in good_values}]
            + [
                {
                    "str": doink.ObjectUpdate(
                        owner_uid="bad", obj_uid="bad", value=dict()
                    )
                }
            ]
        )

        return {
            "good": good_values,
            "bad": bad_values,
        }

    def test_object_update_can_be_serialized_to_json(self, obj_test_values):
        """Test ObjectUpdate's validation for the 'value' field."""

        for i, value in enumerate(obj_test_values["good"]):
            update = doink.ObjectUpdate(
                obj_uid=str(i), owner_uid=str(i), value=value
            )
            expected = json.dumps(
                {"obj_uid": str(i), "owner_uid": str(i), "value": value}
            )
            result = update.to_json()

            assert result == expected

        for i, value in enumerate(obj_test_values["bad"]):
            with pytest.raises(TypeError):
                doink.ObjectUpdate(
                    obj_uid=str(i), owner_uid=str(i), value=value
                )

    def test_object_update_can_be_deserialized_from_json(
        self, obj_test_values
    ):
        """Test ObjectUpdate's validation for the 'value' field."""

        for i, value in enumerate(obj_test_values["good"]):
            expected = doink.ObjectUpdate(
                obj_uid=str(i), owner_uid=str(i), value=value
            )
            result = doink.ObjectUpdate.from_json(
                json.dumps(
                    {"obj_uid": str(i), "owner_uid": str(i), "value": value}
                )
            )

            assert result == expected

        for i, value in enumerate(obj_test_values["bad"]):
            with pytest.raises(TypeError):
                doink.ObjectUpdate.from_json(
                    json.dumps({"uid": str(i), "value": value})
                )


class TestKafkaHandler:
    """Test the functionality of the KafkaHandler class."""

    def test_kafka_handler_can_be_instantiated_without_error(self):
        """Sanity check a KafkaHandler can be constructed."""

        _ = doink.KafkaHandler("topic")

    def test_kafka_handler_can_call_stop_after_instantiation(self):
        """Sanity check the stop method can be called before start."""

        handler = doink.KafkaHandler("topic")
        handler.stop()

    # pylint: disable=redefined-outer-name,unused-argument
    def test_mock_kafka_handler_can_stop_after_starting(self, mock_kafka):
        """Sanity check the KafkaHandler can cleanly start and stop."""

        handler = doink.KafkaHandler("topic")
        handler.start()
        handler.stop()

    # pylint: disable=redefined-outer-name,unused-argument
    def test_kafka_handler_can_trigger_callbacks(self, mock_kafka):
        """Test the KafkaHandler triggers callbacks when appropriate."""

        obj_uid = str(uuid.uuid4())
        owner_uid = "0"
        # Use a list rather than an integer, so we can mutate the
        # variable inside the scope of the callback function
        success_counter = []
        error_counter = []

        def wait_on_counter(counter, n):
            start = time.time()

            while len(counter) != n:
                time.sleep(0.1)
                if time.time() - start > 1:
                    raise TimeoutError(
                        "Counter did not increment enough within the timeout"
                    )

        def callback(object_update: doink.ObjectUpdate):
            if object_update.obj_uid != obj_uid:
                # This condition should never be hit.
                error_counter.append(ValueError("incorrect uid"))

            if len(object_update.value) != 1:
                error_counter.append(ValueError("incorrect length"))

            v = object_update.value.get("key", None)
            if v != "value":
                error_counter.append(ValueError("incorrect value"))

            success_counter.append(0)

        handler = doink.KafkaHandler(str(obj_uid))
        handler.start()
        handler.register(obj_uid, callback)

        for _ in range(10):
            handler.update(owner_uid, obj_uid, {"key": "value"})

        wait_on_counter(success_counter, 10)
        assert len(error_counter) == 0

        handler.update(owner_uid, obj_uid + "_extra", {"key": "value"})
        with pytest.raises(TimeoutError):
            wait_on_counter(error_counter, 1)

        handler.update(owner_uid, obj_uid, {"key": "value", "key2": "value2"})
        wait_on_counter(error_counter, 1)
        assert str(error_counter.pop()) == str(ValueError("incorrect length"))

        handler.update(owner_uid, obj_uid, {"key": "not value"})
        wait_on_counter(error_counter, 1)
        assert str(error_counter.pop()) == str(ValueError("incorrect value"))

        handler.update(owner_uid, obj_uid, {"not key": "value"})
        wait_on_counter(error_counter, 1)
        assert str(error_counter.pop()) == str(ValueError("incorrect value"))


class TestSharedObject:
    """Test functionality of the SharedObject."""

    # pylint: disable=redefined-outer-name,unused-argument
    def test_shared_object_can_share_updates_no_existing_state(
        self, mock_kafka
    ):
        """Test updates can be shared between two new objects."""

        handler = doink.KafkaHandler("topic")
        handler.start()

        uid = str(uuid.uuid4())

        class MySharedObject(doink.SharedObject):
            """Test child of SharedObject."""

            def __init__(self):
                super().__init__(handler, "topic", uid)
                self.my_cool_value = "my_cool_value"
                self.my_other_cool_value = "my_other_cool_value"
                self._my_unshared_cool_value = "my_unshared_cool_value"

        obj1 = MySharedObject()
        obj2 = MySharedObject()

        obj1.my_cool_value = 123
        obj1.my_other_cool_value = True
        obj1._my_unshared_cool_value = None  # pylint: disable=protected-access

        handler.stop()

        assert obj2.my_cool_value == obj1.my_cool_value
        assert obj2.my_other_cool_value == obj1.my_other_cool_value
        # pylint: disable=protected-access
        assert obj2._my_unshared_cool_value != obj1._my_unshared_cool_value
