"""Test functionality of stuff in kafka.py."""

import json
import time
import uuid

import pytest

from doink.kafka import KafkaHandler, ObjectUpdate


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
                    "str": ObjectUpdate(
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
            update = ObjectUpdate(
                obj_uid=str(i), owner_uid=str(i), value=value
            )
            expected = json.dumps(
                {"obj_uid": str(i), "owner_uid": str(i), "value": value}
            )
            result = update.to_json()

            assert result == expected

        for i, value in enumerate(obj_test_values["bad"]):
            with pytest.raises(TypeError):
                ObjectUpdate(obj_uid=str(i), owner_uid=str(i), value=value)

    def test_object_update_can_be_deserialized_from_json(
        self, obj_test_values
    ):
        """Test ObjectUpdate's validation for the 'value' field."""

        for i, value in enumerate(obj_test_values["good"]):
            expected = ObjectUpdate(
                obj_uid=str(i), owner_uid=str(i), value=value
            )
            result = ObjectUpdate.from_json(
                json.dumps(
                    {"obj_uid": str(i), "owner_uid": str(i), "value": value}
                )
            )

            assert result == expected

        for i, value in enumerate(obj_test_values["bad"]):
            with pytest.raises(TypeError):
                ObjectUpdate.from_json(
                    json.dumps({"uid": str(i), "value": value})
                )


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

        def callback(object_update: ObjectUpdate):
            if object_update.obj_uid != obj_uid:
                # This condition should never be hit.
                error_counter.append(ValueError("incorrect uid"))

            if len(object_update.value) != 1:
                error_counter.append(ValueError("incorrect length"))

            v = object_update.value.get("key", None)
            if v != "value":
                error_counter.append(ValueError("incorrect value"))

            success_counter.append(0)

        handler = KafkaHandler(str(obj_uid))
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
