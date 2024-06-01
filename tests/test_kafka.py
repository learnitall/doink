"""Test functionality of stuff in kafka.py."""

import threading
import uuid

from doink.kafka import KafkaHandler, ObjectUpdate


class TestKafkaHandler:
    """Test the functionality of the KafkaHandler class."""

    def test_kafka_handler_can_be_instantiated_without_error(self):
        """Sanity check a KafkaHandler can be constructed."""

        _ = KafkaHandler("topic")

    def test_kafka_handler_can_call_stop_after_instantiation(self):
        """Sanity check the stop method can be called before start."""

        handler = KafkaHandler("topic")
        handler.stop()

    # pylint: disable=redefined-outer-name,unused-argument
    def test_mock_kafka_handler_can_stop_after_starting(self, mock_kafka):
        """Sanity check the KafkaHandler can cleanly start and stop."""

        handler = KafkaHandler("topic")
        handler.start()
        handler.stop()

    # pylint: disable=redefined-outer-name,unused-argument
    def test_kafka_handler_can_materialize_state_from_updates(
        self, mock_kafka
    ):
        """Test the KafkaHandler can construct object state."""

        handler = KafkaHandler("topic")
        handler.start()

        obj_uid = str(uuid.uuid4())
        owner_uid = "0"

        obj = {"key": "value"}

        event = threading.Event()

        def callback(_):
            event.set()

        handler.register(obj_uid, callback)

        handler.update(owner_uid, obj_uid, obj)
        event.wait()
        assert handler.get(obj_uid) == {"key": "value"}
        event.clear()

        obj["key"] = "a different value"
        handler.update(owner_uid, obj_uid, obj)
        event.wait()
        assert handler.get(obj_uid) == {"key": "a different value"}
        event.clear()

        obj = {"key": "value1", "key2": "second value"}
        handler.update(owner_uid, obj_uid, obj)
        event.wait()
        for key, value in handler.get(obj_uid).items():
            assert obj.get(key, None) is not None
            assert value == obj.get(key)
        event.clear()

    # pylint: disable=redefined-outer-name,unused-argument
    def test_kafka_handler_can_trigger_callbacks(self, mock_kafka):
        """Test the KafkaHandler triggers callbacks when appropriate."""

        obj_uid = str(uuid.uuid4())
        owner_uid = "0"
        # Use a list rather than an integer, so we can mutate the
        # variable inside the scope of the callback function
        successes = []
        errors = []
        event = threading.Event()

        def callback(object_update: ObjectUpdate):
            if object_update.obj_uid != obj_uid:
                # This condition should never be hit.
                errors.append(ValueError("incorrect uid"))
                event.set()

            if len(object_update.value) != 1:
                errors.append(ValueError("incorrect length"))
                event.set()

            v = object_update.value.get("key", None)
            if v != "value":
                errors.append(ValueError("incorrect value"))
                event.set()

            successes.append(0)
            if len(successes) == 10:
                event.set()

        handler = KafkaHandler(str(obj_uid))
        handler.start()
        handler.register(obj_uid, callback)

        for _ in range(10):
            handler.update(owner_uid, obj_uid, {"key": "value"})
        assert event.wait(1) is True
        assert len(errors) == 0

        event.clear()
        handler.update(owner_uid, obj_uid + "_extra", {"key": "value"})
        assert event.wait(1) is False

        errors = []
        event.clear()
        handler.update(owner_uid, obj_uid, {"key": "value", "key2": "value2"})
        event.wait(1)
        assert str(errors.pop()) == str(ValueError("incorrect length"))

        event.clear()
        handler.update(owner_uid, obj_uid, {"key": "not value"})
        event.wait(1)
        assert str(errors.pop()) == str(ValueError("incorrect value"))

        event.clear()
        handler.update(owner_uid, obj_uid, {"not key": "value"})
        event.wait(1)
        assert str(errors.pop()) == str(ValueError("incorrect value"))
